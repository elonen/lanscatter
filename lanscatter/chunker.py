from typing import List, Set, Iterable, Dict, Tuple, Optional, Callable, Generator
from types import SimpleNamespace
import os, json, hashlib, asyncio, time
import aiofiles, aiofiles.os, collections, threading
import mmap
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, CancelledError
from pathlib import Path, PurePosixPath
import lz4.frame

# Tools for scanning files in a directory and splitting them into hashed chunks.
# MasterNode and PeerNode both use this for maintaining and syncing their state.

# TODO: convert to use fileio.py instead of directly os.*

HashType = str

class HashableBase:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def __repr__(self):
        return self.__class__.__name__ + str(self.__dict__)
    def __hash__(self):
        return hash(self.__repr__())
    def __eq__(self, other):
        return self.__repr__() == other.__repr__()


class FileChunk(HashableBase):
    path: str           # path + filename
    pos: int            # chunk start position in bytes
    size: int           # chunk size in bytes
    cmpratio: float     # Compression ratio (compressed size / original size)
    hash: HashType      # Hex checksum of data contents (blake2, digest_size=12)


class FileAttribs(HashableBase):
    path: str
    size: int                      # size of complete file in bytes
    mtime: int                     # last modified (unix timestamp)
    treehash: Optional[HashType]   # combine hash for whole file (hash of concatenated chunk hashes)

    @property
    def is_dir(self):
        return self.size < 0


class HashFunc:
    """Replaceable hash function, currently implemented as blake2b"""
    def __init__(self):
        self.h = hashlib.blake2b(digest_size=12)

    def update(self, data):
        self.h.update(data)
        return self

    async def update_async(self, data):
        """Like update() but runs in a separate thread."""
        await asyncio.get_running_loop().run_in_executor(None, lambda: self.h.update(data))
        return self

    def result(self) -> HashType:
        return self.h.hexdigest()


def calc_tree_hash(chunks: Iterable[FileChunk]) -> HashType:
    """
    Sort given chunks by position in file and calculate a combined (tree) hash for them
    """
    path = None
    csum = HashFunc().result()
    for c in sorted(chunks, key=lambda c: c.pos):
        csum = HashFunc().update((str(csum) + str(c.hash)).encode('utf-8')).result()
        assert(c.path == path or path is None)
        path = c.path
    return csum


class SyncBatch:
    """
    Represents sync folder contents and provides tools for comparing them
    """
    chunk_size: int
    chunks: Set[FileChunk]
    files: Dict[str, FileAttribs]

    def __init__(self, chunk_size: int = 0, chunks=(), files=()):
        self.chunk_size = chunk_size
        self.chunks = set()
        self.files = {}
        self.add(files, chunks)

    def __bool__(self):
        """True if batch is not empty"""
        return not not (self.files or self.chunks)

    def __repr__(self):
        """Unambigious, comparable, human readable string representation of batch contents"""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    def __hash__(self):
        return hash(self.__repr__())

    def __eq__(self, other):
        """Compare batch contents. True if folders they represent have identical contents."""
        return self.__repr__() == other.__repr__()

    def sanity_checks(self) -> None:
        """Assert that contents are valid"""
        dupe_count = collections.Counter([(c.path, c.pos) for c in self.chunks])
        illegal_dupes = [chunk for (chunk, cnt) in dupe_count.items() if cnt != 1]
        assert not illegal_dupes
        for f in self.files.values():
            assert isinstance(f.path, str)
            assert '\\' not in f.path, f"Non-posix path: {f.path}"  # Windows version must also use '/' as a separator
        for f in self.files.values():
            assert f.is_dir == f.path.endswith('/')
        for c in self.chunks:
            assert c.path in self.files

    def add(self, files: Iterable[FileAttribs] = (), chunks: Iterable[FileChunk] = ()):
        """
        Add given file attributes and chunks to batch, if not there already.
        Recalculates file tree hashes for files that got new chunks.
        """
        chunks = tuple(chunks)
        self.files.update({a.path: a for a in files})
        self.chunks.update(chunks)
        for path in set((c.path for c in chunks)):
            if path not in self.files:
                self.files[path] = FileAttribs(path=path, size=0, mtime=int(time.time()), treehash=None)
            self.files[path].treehash = calc_tree_hash((c for c in self.chunks if c.path == path))
            self.files[path].size = sum((c.size for c in self.chunks if c.path == path))

    def discard(self, paths: Iterable[str] = (), chunks: Iterable[FileChunk] = ()):
        """
        Remove given paths and chunks from current batch.
        Deletes hashes for deleted paths, and invalidates (doesn't recalc) treehashes for paths with removed chunks.
        """
        paths = set(paths)
        for path in paths:
            self.files.pop(path, None)
        self.chunks = set((c for c in self.chunks if c.path not in paths))
        for c in tuple(chunks):
            self.chunks.discard(c)
            if c.path in self.files:
                self.files[c.path].treehash = None

    def first_chunk_with(self, chunk_hash: HashType) -> Optional[FileChunk]:
        """Return first chunk with given content (hash)"""
        return next(iter((c for c in self.chunks if c.hash == chunk_hash)), None)

    def file_tree_diff(self, there: 'SyncBatch'):
        """Compare this and given batches for file attribute changes"""
        there_by_path = {a.path: a for a in there.files.values()}
        names_here, names_there = set(self.files.keys()), set(there_by_path.keys())
        differing_attribs = [a for a in self.files.values() if (a.path in there_by_path and there_by_path[a.path] != a)]
        return SimpleNamespace(
            there_only=names_there - names_here,
            here_only=names_here - names_there,
            with_different_attribs=differing_attribs)

    def chunk_diff(self, there: 'SyncBatch'):
        """Compare this and given batches for content (chunk list) changes"""
        there_chunks = there.chunks
        return SimpleNamespace(
            there_only=there_chunks - self.chunks,
            here_only=self.chunks - there_chunks)

    def copy_chunk_compress_ratios_from(self, other):
        """Copy (replace) chunk compression ratio values from given other FileBatch."""
        cmp_ratios = {c.hash: c.cmpratio for c in other.chunks if c.cmpratio is not None}
        self.chunks = set((FileChunk(path=c.path, pos=c.pos, size=c.size, hash=c.hash, cmpratio=cmp_ratios.get(c.hash)) for c in self.chunks))

    def all_hashes(self) -> Set[HashType]:
        """Return set of unique hashes in the batch"""
        return set((c.hash for c in self.chunks))

    def have_all_hashes(self, other: Iterable[HashType]) -> bool:
        """True if the batch contains all hashes that the 'other' does."""
        return not (set(other) - self.all_hashes())

    def to_dict(self) -> Dict:
        """Turn batch object into a serializable dictionary"""
        return {
            'chunk_size': self.chunk_size,
            'files': [f.__dict__ for f in sorted(list(self.files.values()), key=lambda f: f.path)],
            'chunks': [c.__dict__ for c in sorted(list(self.chunks), key=lambda c: c.path + f'{c.pos:016}')]}

    @staticmethod
    def from_dict(data: Dict) -> 'SyncBatch':
        res = SyncBatch(chunk_size=data['chunk_size'])
        res.add(files=(FileAttribs(**d) for d in data['files']),
                chunks=(FileChunk(**d) for d in data['chunks']))
        return res


def _hash_file(basedir: str, relpath: str, chunk_size: int, executor, progress_func: Callable, test_compress: bool) -> \
        Generator[Future, None, None]:
    """
    Split given file into chunks and return multithreading futures that hash them.

    Hashlib releases GIL so this is fast even with a ThreadPoolExecutor. Uses memory mapping
    instead of buffers for maximum efficiency.

    :param basedir: Base directory to search for files
    :param relpath: Pathname to file
    :param executor: Executor to run hash functions in.
    :param file_progress_func: Progress reporting callback
    :return: Generator that yields multithreading Futures that return FileChunks
    """
    path = Path(basedir) / relpath
    files_size = path.stat().st_size
    if files_size == 0:
        yield executor.submit(lambda: FileChunk(path=relpath, pos=0, size=0, hash=HashFunc().result(), cmpratio=1.0))
    else:
        with open(path, 'r+b') as f:
            mm = mmap.mmap(f.fileno(), 0)
            for pos in range(0, files_size, chunk_size):
                def do_hash(p, s):
                    progress_func(relpath, s, p, files_size)
                    h = HashFunc().update(mm[p:(p+s)]).result()
                    compress_ratio = 1.0 if not test_compress else \
                        min(1.0, float("%.2g" % (len(lz4.frame.compress(mm[p:(p+s)])) / s)))
                    return FileChunk(path=relpath, pos=p, size=s, hash=h, cmpratio=compress_ratio)
                yield executor.submit(do_hash, pos, min(chunk_size, files_size - pos))


async def scan_dir(basedir: str, chunk_size: int, old_batch: Optional[SyncBatch], progress_func: Callable, test_compress: bool) ->\
        Tuple[SyncBatch, Iterable[str]]:
    """
    Scan given directory and generate a list of FileChunks of its contents. If old_chunks is provided,
    assumes contents haven't changed if mtime and size are identical. Optionally tests how compressible chunks are.

    :param basedir: Folders to scan
    :param progress_func: Progress report callback - func(cur_filename, file_progress, total_progress)
    :param chunk_size: Length of chunk to split files into
    :param old_batch: If given, compares dir it and skips hashing files with identical size & mtime
    :param test_compress: Test for compressibility while hashing
    :return: Tuple(New list of FileChunks or old_chunks if no changes are detected, List[errors],
                   Dict[<hash>: compress_ratio, ...])
    """
    errors = []
    fnames = []
    dirs = []
    base = Path(basedir)
    for root, d_names, f_names in os.walk(basedir, topdown=False, onerror=None, followlinks=False):
        for path in d_names:
            dirs.append(str(PurePosixPath((Path(root)/path).relative_to(base))))
        for f in f_names:
            fnames.append(str(PurePosixPath((Path(root)/f).relative_to(base))))

    async def file_needs_rehash(path: str):
        try:
            f = old_batch.files.get(path) if old_batch else None
            s = await aiofiles.os.stat(base/path)
            return f is None or f.size != s.st_size or f.mtime != int(s.st_mtime)
        except (FileNotFoundError, KeyError):
            return True

    # Return immediately if we are completely up to date:
    files_needing_rehash = set([fn for fn in fnames if await file_needs_rehash(fn)])
    if old_batch and len(fnames) == len(old_batch.files) and not files_needing_rehash:
        return old_batch, errors

    # Prepare progress reporting
    total_size = int(sum([(await aiofiles.os.stat(base/f)).st_size for f in fnames]))
    total_remaining = total_size

    progr_lock = threading.RLock()  # Mutex to maintain total_remaining inside file_progress()
    def file_progress(path, just_read, pos, file_size):
        nonlocal total_remaining
        with progr_lock:
            total_remaining -= just_read
            perc = float(pos) / file_size if file_size > 0 else 1.0
            progress_func(path, total_progress=1-float(total_remaining)/(total_size or 1), file_progress=perc)

    # Hash files as needed
    res_files, res_chunks = [], []

    # Copy hashes for apparently non-modified files from old_chunks:
    for fn in (set(fnames) - files_needing_rehash):
        res_chunks.extend([c for c in old_batch.chunks if c.path == fn])
        res_files.append(old_batch.files[fn])
        total_remaining -= res_files[-1].size

    # Hash file contents in multiple threads
    with ThreadPoolExecutor() as pool:
        futures = []
        for fn in files_needing_rehash:
            try:
                futures.extend(_hash_file(basedir, fn, chunk_size, pool, file_progress, test_compress=test_compress))
            except (OSError, IOError) as e:
                errors.append(f'[{fn}]: ' + str(e))
        for f in as_completed(futures):
            try:
                res_chunks.append(f.result())
            except (OSError, IOError) as e:
                errors.append(str(e))

    # Read file attributes and calculate tree hashes
    for fn in files_needing_rehash:
        try:
            st = (Path(basedir) / fn).stat()
            chunks = [c for c in res_chunks if c.path == fn]
            res_files.append(FileAttribs(path=fn, size=st.st_size, mtime=int(st.st_mtime), treehash=calc_tree_hash(chunks)))
        except (OSError, IOError) as e:
            errors.append(f'[{fn}]: ' + str(e))

    # Include all directories and read their file attribs
    dirs = set(dirs) | set((str(PurePosixPath(Path(p).parent)) for p in fnames)) - set('.')
    for d in dirs:
        try:
            res_files.append(FileAttribs(path=d+'/', size=-1, treehash=None,
                                         mtime=int((await aiofiles.os.stat(base/d)).st_mtime)))
        except (OSError, IOError) as e:
            errors.append(f'[{d}/]: ' + str(e))

    res = SyncBatch(chunk_size)
    res.add(files=res_files, chunks=res_chunks)

    for x in (*res.chunks, *res.files.values()):
        assert isinstance(x.path, str)
        assert '\\' not in x.path, f"Non-posix path: '{x.path}'"

    return res, errors
