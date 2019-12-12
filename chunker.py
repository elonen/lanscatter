from typing import List, Set, Iterable, Dict, Tuple, Optional
from types import SimpleNamespace
import os, json, hashlib, asyncio, time
import aiofiles, aiofiles.os, collections
import mmap

# Tools for scanning files in a directory and splitting them into hashed chunks.
# FileServer and FileClient both use this for maintaining and syncing their state.

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
    hash: HashType       # Hex checksum of data contents (blake2, digest_size=12)


class FileAttribs(HashableBase):
    path: str
    size: int      # size of complete file in bytes
    mtime: int     # last modified (unix timestamp)
    treehash: HashType   # combine hash for whole file (hash of concatenated chunk hashes)


def calc_tree_hash(chunks: Iterable[FileChunk]) -> HashType:
    path = None
    csum = HashFunc().result()
    for c in sorted(chunks, key=lambda c: c.pos):
        csum = HashFunc().update((str(csum) + str(c.hash)).encode('utf-8')).result()
        assert(c.path == path or path is None)
        path = c.path
    return csum


class SyncBatch:
    chunk_size: int
    chunks: Set[FileChunk]
    files: Dict[str, FileAttribs]

    def __init__(self, chunk_size: int = 0, chunks=(), files=()):
        self.chunk_size = chunk_size
        self.chunks = set()
        self.files = {}
        self.add(files, chunks)

    def __bool__(self):
        return not not (self.files or self.chunks)

    def sanity_checks(self) -> None:
        # Check that we don't have multiple chunks for a file at same pos
        dupe_count = collections.Counter([c.path + '\0' + str(c.pos) for c in self.chunks])
        illegal_dupes = [chunk for (chunk, cnt) in dupe_count.items() if cnt != 1]
        assert (not illegal_dupes)

    def add(self, files: Iterable[FileAttribs] = (), chunks: Iterable[FileChunk] = ()):
        chunks = tuple(chunks)
        self.files.update({a.path: a for a in files})
        self.chunks.update(chunks)
        for path in set((c.path for c in chunks)):
            if path not in self.files:
                self.files[path] = FileAttribs(path=path, size=0, mtime=int(time.time()), treehash=None)
            self.files[path].treehash = calc_tree_hash((c for c in self.chunks if c.path == path))

    def discard(self, paths: Iterable[str] = (), chunks: Iterable[FileChunk] = ()):
        '''
        Remove given paths and chunks from current batch.
        Deletes hashes for deleted paths, and invalidates (doesn't recalc) treehashes for paths with removed chunks.
        '''
        paths = set(paths)
        for path in paths:
            self.files.pop(path, None)
        self.chunks = set((c for c in self.chunks if c.path not in paths))
        chunks = tuple(chunks)
        for c in chunks:
            self.chunks.discard(c)
            if c.path in self.files:
                self.files[c.path].treehash = None

    def chunks_containing(self, hash: HashType) -> Iterable[FileChunk]:
        return tuple((c for c in self.chunks if c.hash == hash))

    def first_chunk_with(self, hash: HashType) -> Optional[FileChunk]:
        return next(iter(self.chunks_containing(hash)), None)

    def file_tree_diff(self, there: 'SyncBatch'):
        there_by_path = {a.path: a for a in there.files.values()}
        names_here, names_there = set(self.files.keys()), set(there_by_path.keys())
        differing_attribs = [a for a in self.files.values() if (a.path in there_by_path and there_by_path[a.path] != a)]
        return SimpleNamespace(
            there_only=names_there - names_here,
            here_only=names_here - names_there,
            with_different_attribs=differing_attribs)

    def chunk_diff(self, there: 'SyncBatch'):
        there = there.chunks
        return SimpleNamespace(
            there_only=there - self.chunks,
            here_only=self.chunks - there)

    def all_hashes(self) -> Set[HashType]:
        return set((c.hash for c in self.chunks))

    def have_all_hashes(self, other: Iterable[HashType]) -> bool:
        return not (set(other) - self.all_hashes())

    def to_dict(self) -> Dict:
        return {
            'chunk_size': self.chunk_size,
            'files': [f.__dict__ for f in sorted(list(self.files.values()), key=lambda f: f.path)],
            'chunks': [c.__dict__ for c in sorted(list(self.chunks), key=lambda c: c.path + f'{c.pos:016}')]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @staticmethod
    def from_dict(data: Dict) -> 'SyncBatch':
        res = SyncBatch(chunk_size = data['chunk_size'])
        res.add(files=(FileAttribs(**d) for d in data['files']),
                chunks=(FileChunk(**d) for d in data['chunks']))
        return res

    @staticmethod
    def from_json(json_txt: str) -> 'SyncBatch':
        return from_dict(json.loads(json_txt))


# Replaceable hash function, currently implemented as blake2b
class HashFunc:
    def __init__(self):
        self.h = hashlib.blake2b(digest_size=12)

    def update(self, data):
        self.h.update(data)
        return self

    async def update_async(self, data):
        # TODO: make this run in a separate thread or process pool for better concurrency with file/net IO
        await asyncio.sleep(0)  # Minimal yield -- give simultaneously started IO tasks a chance to go first
        self.h.update(data)
        return self

    def result(self) -> HashType:
        return self.h.hexdigest()


async def read_attribs(basedir: str, relpath: str):
    path = os.path.join(basedir, relpath)
    st = await aiofiles.os.stat(path)
    return FileAttribs(path=relpath, size=st.st_size, mtime=int(st.st_mtime), treehash=None)


async def hash_file(basedir: str, relpath: str, chunk_size: int,
                    file_progress_func=None) -> Tuple[FileAttribs, List[FileChunk]]:
    '''
    Split given file into chunks and hash them
    :param: basedir: Base directory to search for files
    :param relpath: Pathname to file
    :param file_progress_func: Progress reporting callback
    :return: List of FileChunk
    '''
    assert(chunk_size > 0)
    path = os.path.join(basedir, relpath)
    st = await aiofiles.os.stat(path)

    pos, chunks = 0, []

    if st.st_size == 0:
        chunks.append(FileChunk(
            path=relpath,
            pos=0, size=0,
            hash=HashFunc().result()))
    else:
        async with aiofiles.open(path, 'r+b') as f:
            map = mmap.mmap(f.fileno(), 0)
            while pos < st.st_size:
                sz = min(chunk_size, st.st_size - pos)
                csum = await HashFunc().update_async(map[pos:(pos+sz)])
                hash = csum.result()
                chunks.append(FileChunk(
                        path=relpath,
                        pos=pos, size=sz,
                        hash=hash))
                pos += sz
                if file_progress_func:
                    file_progress_func(relpath, sz, pos, st.st_size)

    attribs = FileAttribs(path=relpath, size=st.st_size, mtime=int(st.st_mtime), treehash=calc_tree_hash(chunks))
    return attribs, chunks


async def scan_dir(basedir: str, chunk_size: int, old_batch=None, progress_func=None) -> SyncBatch:
    '''
    Scan given directory and generate a list of FileChunks of its contents. If old_chunks is provided,
    assumes contents haven't changed if mtime and size are identical.

    :param basedir: Folders to scan
    :param progress_func: Progress report callback - func(cur_filename, file_progress, total_progress)
    :return: New list of FileChunks, or old_chunks if no changes are detected
    '''
    fnames = []
    for root, d_names, f_names in os.walk(basedir, topdown=False, onerror=None, followlinks=False):
        for f in f_names:
            fnames.append(os.path.relpath(os.path.join(root, f), basedir))

    async def file_needs_rehash(path: str):
        try:
            f = old_batch.files.get(path) if old_batch else None
            s = await aiofiles.os.stat(os.path.join(basedir, path))
            return f is None or f.size != s.st_size or f.mtime != int(s.st_mtime)
        except (FileNotFoundError, KeyError):
            return True

    # Return immediately if we are completely up to date:
    files_needing_rehash = set([fn for fn in fnames if await file_needs_rehash(fn)])
    if old_batch and len(fnames) == len(old_batch.files) and not files_needing_rehash:
        return old_batch

    # Prepare progress reporting
    total_size = int(sum([(await aiofiles.os.stat(os.path.join(basedir, f))).st_size for f in fnames]))
    total_remaining = total_size

    def file_progress(path, just_read, pos, file_size):
        nonlocal total_remaining
        total_remaining -= just_read
        if progress_func:
            perc = float(pos) / file_size if file_size > 0 else 1.0
            progress_func(path, total_progress=1-float(total_remaining)/(total_size or 1), file_progress=perc)

    # Hash files as needed
    res_files, res_chunks = [], []
    for fn in fnames:
        if fn in files_needing_rehash:
            f, c = await hash_file(basedir, fn, chunk_size, file_progress_func=file_progress)
            res_files.append(f)
            res_chunks.extend(c)
        else:
            res_chunks.extend([c for c in old_batch.chunks if c.path == fn])
            res_files.append(old_batch.files[fn])
            total_remaining -= res_files[-1].size

    res = SyncBatch(chunk_size)
    res.add(files=res_files, chunks=res_chunks)
    return res


async def monitor_folder_forever(basedir: str, update_interval: float, progress_func=None,
                                 chunk_size: int = 64*1024*1024):
    '''
    Test 'basedir' for changes every 'update_interval_secs' and yield a new list of FileChunks when it happens.
    (This is an async generator, intended to be used with "async for" syntax.)

    :param basedir: Folder to watch
    :param update_interval: Update interval in seconds
    :param progress_func: Callback for progress reports when hashing - func(cur_filename, file_progress, total_progress)
    :param chunk_size: Chunk size in bytes (for splitting files)
    '''
    batch = None
    while True:
        new_batch = await scan_dir(basedir, chunk_size=chunk_size, old_batch=batch, progress_func=progress_func)
        if not (new_batch is batch):
            batch = new_batch
            yield batch
        else:
            await asyncio.sleep(update_interval)


# ----------------------------

async def main():

    def progress(cur_filename, file_progress, total_progress):
        print(cur_filename, file_progress, total_progress)

    src = await scan_dir('sync-source/', 64*1024*1024, progress_func=progress)
    trg = await scan_dir('sync-target/', 64*1024*1024, progress_func=progress)
    fdiff = trg.file_tree_diff(src.files.values())
    print(fdiff)
    cdiff = trg.chunk_diff(src.chunks)
    print(cdiff)

if __name__ == "__main__":
    asyncio.run(main())
