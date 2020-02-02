from typing import List, Set, Iterable, Dict, Tuple, Optional, Callable, Generator
from types import SimpleNamespace
import os, json, hashlib, asyncio, time
import aiofiles, aiofiles.os, collections, threading
import mmap
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, CancelledError
from pathlib import Path, PurePosixPath
import lz4.frame
from .common import Defaults, process_multibuffer_io, file_read_producer
from .ratelimiter import RateLimiter

# Tools for scanning files in a directory and splitting them into hashed chunks.
# MasterNode and PeerNode both use this for maintaining and syncing their state.

HashType = str

class HashableBase:
    def __init__(self, **kwargs):
        self.__dict__.update({k: kwargs[k] for k in sorted(kwargs)})
    def __repr__(self):
        return self.__class__.__name__ + str({k: self.__dict__[k] for k in sorted(self.__dict__)})
    def __hash__(self):
        return hash(self.__repr__())
    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class FileChunk(HashableBase):
    path: str           # path + filename
    pos: int            # chunk start position in bytes
    size: int           # chunk size in bytes
    cmpratio: float     # Compression ratio (compressed size / original size)
    hash: HashType      # Chain hash of subchunks

class FileAttribs(HashableBase):
    path: str
    size: int                      # size of complete file in bytes
    mtime: int                     # last modified (unix timestamp)
    chain_hash: Optional[HashType]  # Combine hash for whole file (hash of concatenated chunk hashes)

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


def calc_chain_hash(chunks: Iterable[FileChunk]) -> HashType:
    """
    Sort given chunks by position in file and calculate a combined (chain) hash for them
    """
    path = None
    csum = HashFunc().result()
    for c in sorted(chunks, key=lambda c: c.pos):
        csum = HashFunc().update((csum + c.hash).encode('utf-8')).result()
        assert(c.path == path or path is None)
        path = c.path
    return csum


class SyncBatch:
    """
    Represents sync folder contents and provides tools for comparing them
    """
    chunk_size: int
    sub_chunk_size: int
    chunks: Set[FileChunk]
    files: Dict[str, FileAttribs]

    def __init__(self, chunk_size: int = 0, sub_chunk_size: int = 0):
        assert chunk_size >= sub_chunk_size
        self.chunk_size = chunk_size
        self.sub_chunk_size = sub_chunk_size
        self.chunks = set()
        self.files = {}

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
        Recalculates file chain hashes for files that got new chunks.
        """
        chunks = tuple(chunks)
        self.files.update({a.path: a for a in files})
        self.chunks.update(chunks)
        for path in set((c.path for c in chunks)):
            if path not in self.files:
                self.files[path] = FileAttribs(path=path, size=0, mtime=int(time.time()), chain_hash=None)
            self.files[path].chain_hash = calc_chain_hash((c for c in self.chunks if c.path == path))
            self.files[path].size = sum((c.size for c in self.chunks if c.path == path))

    def discard(self, paths: Iterable[str] = (), chunks: Iterable[FileChunk] = ()):
        """
        Remove given paths and chunks from current batch.
        Deletes hashes for deleted paths, and invalidates (doesn't recalc) chain hashes for paths with removed chunks.
        """
        paths = set(paths)
        for path in paths:
            self.files.pop(path, None)
        self.chunks = set((c for c in self.chunks if c.path not in paths))
        for c in tuple(chunks):
            self.chunks.discard(c)
            if c.path in self.files:
                self.files[c.path].chain_hash = None

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
        return SimpleNamespace(
            there_only=there.chunks - self.chunks,
            here_only=self.chunks - there.chunks)

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
            'sub_chunk_size': self.sub_chunk_size,
            'files': [f.__dict__ for f in sorted(list(self.files.values()), key=lambda f: f.path)],
            'chunks': [c.__dict__ for c in sorted(list(self.chunks), key=lambda c: c.path + f'{c.pos:016}')]}

    @staticmethod
    def from_dict(data: Dict) -> 'SyncBatch':
        res = SyncBatch(chunk_size=data['chunk_size'], sub_chunk_size=data['sub_chunk_size'])
        res.add(files=(FileAttribs(**d) for d in data['files']),
                chunks=(FileChunk(**d) for d in data['chunks']))
        return res


class SubChunkHashTask(HashableBase):
    chunk: FileChunk    # Chunk this sub-part belongs to
    pos: int            # start position in bytes
    size: int           # chunk size in bytes
    pos_perc: float     # pos / total_file_size (for progress reporting)
    cmpr_size: int      # Compressed size
    result: HashType    # Hex checksum of data contents (blake2, digest_size=12)


async def file_to_hash_tasks(fio, relpath: str, max_chunk_size: int, max_sub_chunk_size: int) -> \
        Tuple[List[FileChunk], List[SubChunkHashTask]]:
    """
    Split file into chunks and chunks further into hash tasks.
    Chunk hash will be a chain hash of sub chunk hashes (and later, file hash a chain hash of chunk hashes).

    :param fio: FileIO object to read from
    :param relpath: Pathname to file
    :param max_chunk_size: Maximum chunk size in bytes
    :param max_sub_chunk_size: Maximum hash task size in bytes
    """
    res_chunks, res_sub_chunks = [], []
    assert max_chunk_size >= max_sub_chunk_size
    file_size = (await fio.stat(relpath)).st_size
    if file_size == 0:
        res_chunks = [FileChunk(path=relpath, pos=0, cmpratio=1, hash='', size=0)]
        res_sub_chunks = [SubChunkHashTask(chunk=res_chunks[0], pos=0, cmpr_size=0, result=None, pos_perc=0, size=0)]
    else:
        for chunk_pos in range(0, file_size, max_chunk_size):
            chunk = FileChunk(path=relpath, pos=chunk_pos, cmpratio=0, hash='',
                              size=min(max_chunk_size, file_size - chunk_pos))
            res_chunks.append(chunk)
            for ht_pos in range(chunk.pos, chunk.pos+chunk.size, max_sub_chunk_size):
                res_sub_chunks.append(SubChunkHashTask(
                    chunk=chunk, pos=ht_pos, cmpr_size=None, result=None, pos_perc=float(ht_pos)/file_size,
                    size=min(max_sub_chunk_size, (chunk.pos + chunk.size) - ht_pos)))
    return res_chunks, res_sub_chunks


async def scan_dir(fio, max_chunk_size: int, max_sub_chunk_size: int, old_batch: Optional[SyncBatch],
                   progress_func: Callable, test_compress: bool) ->\
        Tuple[SyncBatch, Iterable[str]]:
    """
    Scan given directory and generate a list of FileChunks of its contents. If old_chunks is provided,
    assumes contents haven't changed if mtime and size are identical. Optionally tests how compressible chunks are.

    :param fio: FileIO with basedir on folder to scan
    :param progress_func: Progress report callback - func(cur_filename, file_progress, total_progress)
    :param max_chunk_size: Length of chunk to split files into
    :param max_sub_chunk_size: Block size to hash at a time (chunk has is a chain hash of sub chunks)
    :param old_batch: If given, compares dir it and skips hashing files with identical size & mtime
    :param test_compress: Test for compressibility while hashing
    :return: Tuple(New list of FileChunks or old_chunks if no changes are detected, List[errors],
                   Dict[<hash>: compress_ratio, ...])
    """
    errors = []
    fnames = []
    dirs = []
    for root, d_names, f_names in os.walk(str(fio.basedir), topdown=False, onerror=None, followlinks=False):
        for path in d_names:
            dirs.append(str(PurePosixPath((Path(root)/path).relative_to(fio.basedir))))
        for f in f_names:
            fnames.append(str(PurePosixPath((Path(root)/f).relative_to(PurePosixPath(fio.basedir)))))

    async def file_needs_rehash(p: str):
        try:
            f = old_batch.files.get(p) if old_batch else None
            s = await fio.stat(p)
            return f is None or f.size != s.st_size or f.mtime != int(s.st_mtime)
        except (FileNotFoundError, KeyError):
            return True

    # Return immediately if we are completely up to date:
    files_needing_rehash = set([fn for fn in fnames if await file_needs_rehash(fn)])
    if old_batch and not files_needing_rehash and len(fnames) == len(old_batch.files):
        return old_batch, errors

    # Prepare progress reporting
    total_size = int(sum([(await fio.stat(f)).st_size for f in fnames]))
    total_remaining = total_size
    progr_lock = threading.RLock()  # Mutex to maintain integrity of total_remaining inside file_progress()

    def file_progress(path, just_read, file_perc):
        nonlocal total_remaining
        with progr_lock:
            total_remaining -= just_read
            progress_func(path, total_progress=1-float(total_remaining)/(total_size or 1), file_progress=file_perc)

    # Hash files as needed
    res_files, res_chunks = [], []

    # Copy hashes for apparently non-modified files from old_chunks:
    for fn in (set(fnames) - files_needing_rehash):
        res_chunks.extend([c for c in old_batch.chunks if c.path == fn])
        res_files.append(old_batch.files[fn])
        total_remaining -= res_files[-1].size

    # Split files into chunks and sub chunks (hash tasks)
    new_chunks = []
    hash_tasks_pending = asyncio.Queue()
    hash_tasks_done = []
    for fn in files_needing_rehash:
        chs, sub_chs = await file_to_hash_tasks(fio, fn, max_chunk_size, max_sub_chunk_size)
        new_chunks.extend(chs)
        for ht in sub_chs:
            hash_tasks_pending.put_nowait(ht)

    # Hash files (process SubChunkHashTask) using multiple threads
    if True:
        async def hash_and_compress(buff):
            hash_task, data = buff
            def do_it():
                file_progress(hash_task.chunk.path, hash_task.size, hash_task.pos_perc)
                with lz4.frame.LZ4FrameCompressor() as lz:
                    return HashFunc().update(data).result(),\
                           (len(lz.begin()) + len(lz.compress(data)) + len(lz.flush())) if test_compress else len(data)
            loop = asyncio.get_running_loop()
            hash_task.result, hash_task.cmpr_size = await loop.run_in_executor(None, do_it)
            hash_tasks_done.append(hash_task)

        fh = None
        async def read_file_sub_chunks(__):
            """Producer for producer/consumer loop."""
            nonlocal fh
            if hash_tasks_pending.empty():
                return None
            else:
                ht = hash_tasks_pending.get_nowait()
                # Reuse current file handle if possible
                if not fh or fh.name != ht.chunk.path or await fh.tell() != ht.pos:
                    if fh: await fh.close()
                    fh = await fio.open_and_seek(ht.chunk.path, ht.pos, for_write=False).__aenter__()
                return ht, await fh.read(ht.size)

        try:
            await process_multibuffer_io(
                producer=read_file_sub_chunks, consumer=hash_and_compress, parallel_consumers=True,
                initial_buffers=[(True, True) for __ in range(Defaults.MAX_WORKERS)])
        except (OSError, IOError) as e:
            errors.append(f'Hashing failed: ' + str(e))
        finally:
            if fh: await fh.close()

    # Combine SubChunkHashTasks results in Chunks
    # (multithread processed SubChunkHashTasks may be out of order; sort them by pos to get correct chain hashes)
    hash_tasks_done = sorted(hash_tasks_done, key=lambda ht: ht.chunk.path + '\0' + '\0' + "%016d" % ht.pos)
    for ht in hash_tasks_done:
        ht.chunk.cmpratio += ht.cmpr_size  # temporarily accumulate size, we'll calculate ratio later
        ht.chunk.hash = HashFunc().update((ht.chunk.hash + ht.result).encode('utf-8')).result()
        assert ht.chunk in new_chunks
    for c in new_chunks:
        c.cmpratio = min(1.0, float("%.2g" % (c.cmpratio / (c.size or 1))))
    res_chunks.extend(new_chunks)

    # Read file attributes and calculate tree hashes
    for fn in files_needing_rehash:
        try:
            s = await fio.stat(fn)
            chunks = [c for c in res_chunks if c.path == fn]
            assert fn not in (fa.path for fa in res_files)
            res_files.append(FileAttribs(path=fn, size=s.st_size, mtime=int(s.st_mtime), chain_hash=calc_chain_hash(chunks)))
        except (OSError, IOError) as e:
            errors.append(f'[{fn}]: ' + str(e))

    # Include all directories and read their file attribs
    dirs = set(dirs) | set((str(PurePosixPath(Path(p).parent)) for p in fnames)) - set('.')
    for d in dirs:
        try:
            res_files.append(FileAttribs(path=d+'/', size=-1, chain_hash=None,
                                         mtime=int((await fio.stat(d)).st_mtime)))
        except (OSError, IOError) as e:
            errors.append(f'[{d}/]: ' + str(e))

    res = SyncBatch(max_chunk_size, max_sub_chunk_size)
    res.add(files=res_files, chunks=res_chunks)

    return res, errors
