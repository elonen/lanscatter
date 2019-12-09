from typing import List
import os, json, hashlib, asyncio
import aiofiles, aiofiles.os

# Tools for scanning files in a directory and splitting them into hashed chunks.
# FileServer and FileClient both use this for maintaining and syncing their state.

ChunkId = str


class FileChunk:
    filename: str       # path + filename
    pos: int            # chunk start position in bytes
    size: int           # chunk size in bytes
    file_size: int      # size of complete file in bytes
    file_mtime: int     # last modified (unix timestamp)
    hash: ChunkId       # Hex checksum of data contents (blake2, digest_size=12)

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def __repr__(self):
        return 'FileChunk('+str(self.__dict__)+')'
    def __hash__(self):
        return hash(self.__repr__())
    def __eq__(self, other):
        return self.__repr__() == other.__repr__()


# Replaceable hash function, currently implemented as blake2b
class HashFunc:
    def __init__(self):
        self.h = hashlib.blake2b(digest_size=12)

    def update(self, data):
        self.h.update(data)

    async def update_async(self, data):
        # TODO: make this run in a separate thread or process pool for better concurrency with file/net IO
        await asyncio.sleep(0)  # Minimal yield -- give simultaneously started IO tasks a chance to go first
        self.h.update(data)

    def result(self) -> ChunkId:
        return self.h.hexdigest()


async def hash_file(basedir: str, relpath: str, chunk_size: int,
                    file_progress_func=None) -> List[FileChunk]:
    '''
    Split given file into chunks and hash them
    :param: basedir: Base directory to search for files
    :param relpath: Pathname to file
    :param file_progress_func: Progress reporting callback
    :return: List of FileChunk
    '''
    res = []
    path = os.path.join(basedir, relpath)

    st = await aiofiles.os.stat(path)
    pos = 0
    if file_progress_func:
        file_progress_func(relpath, 0, 0, st.st_size)

    FILE_BUFFER_SIZE = 256 * 1024

    async with aiofiles.open(path, 'rb') as f:
        while True:  # Need to run loop at least once, break is at the end
            assert (pos <= st.st_size)
            csum = HashFunc()
            buff_in, buff_out = bytearray(FILE_BUFFER_SIZE), None
            sz = min((st.st_size-pos), chunk_size)
            remaining = sz

            async def read_file():
                nonlocal buff_in, remaining
                if remaining > 0:
                    if remaining < len(buff_in):
                        buff_in = bytearray(remaining)
                    cnt = await f.readinto(buff_in)
                    remaining -= cnt
                else:
                    buff_in = None

            async def update_hash():
                nonlocal buff_out
                if not buff_out:
                    buff_out = bytearray(FILE_BUFFER_SIZE)
                else:
                    await csum.update_async(buff_out)

            # Read and hash concurrently
            while remaining > 0:
                await asyncio.gather(read_file(), update_hash())
                buff_in, buff_out = buff_out, buff_in  # Swap buffers
            await update_hash()  # Write once more to flush buff_out

            res.append(FileChunk(
                    filename=relpath,
                    pos=pos,
                    size=sz,
                    file_size=st.st_size,
                    file_mtime=int(st.st_mtime+0.5),
                    hash=csum.result()))

            pos += sz
            if file_progress_func:
                file_progress_func(relpath, sz, pos, st.st_size)

            if pos >= st.st_size:
                break

    return res


async def scan_dir(basedir: str, chunk_size: int, old_chunks=(), progress_func=None) -> List[FileChunk]:
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

    # Map filename -> chunk for faster lookup of current list
    fn_to_old_chunk = {c.filename: c for c in old_chunks} if old_chunks else {}

    async def file_needs_rehash(filename: str):
        try:
            c = fn_to_old_chunk[filename]
            s = await aiofiles.os.stat(os.path.join(basedir, filename))
            return c.file_size != s.st_size or c.file_mtime != int(s.st_mtime + 0.5)
        except (FileNotFoundError, KeyError):
            return True

    # Return immediately if we are completely up to date:
    files_needing_rehash = set([fn for fn in fnames if await file_needs_rehash(fn)])
    if len(fnames) == len(fn_to_old_chunk.keys()) and len(files_needing_rehash) == 0:
        return old_chunks

    # Prepare progress reporting
    total_size = int(sum([(await aiofiles.os.stat(os.path.join(basedir, f))).st_size for f in fnames]))
    total_remaining = total_size

    def file_progress(path, just_read, pos, file_size):
        nonlocal total_remaining
        total_remaining -= just_read
        if progress_func:
            perc = float(pos)/file_size if file_size>0 else 1.0
            progress_func(path, total_progress=1-float(total_remaining)/(total_size or 1), file_progress=perc)

    # Hash files as needed
    res = []
    for fn in fnames:
        if fn not in files_needing_rehash:
            res.extend([c for c in old_chunks if c.filename == fn])
            total_remaining -= res[-1].file_size
        else:
            res.extend(await hash_file(basedir, fn, chunk_size, file_progress_func=file_progress))

    # Sort results by filename and then by chunk position
    return sorted(res, key=lambda c: c.filename+f'{c.pos:016}')



async def monitor_folder_forever(basedir: str, update_interval: float, progress_func=None,
                                 chunk_size: int = 64*1024*1024):
    '''
    Test 'basedir' for changes every 'update_interval_secs' and yield a new list of FileChunks when it happens.
    (This is an async generator, intended to be used with "async for" syntax.)

    :param basedir: Folder to watch
    :param update_interval: Update interval in seconds
    :param progress_func: Callback for progress reports when hashing - func(cur_filename, file_progress, total_progress)
    '''
    chunks = []
    while True:
        new_chunks = await scan_dir(basedir, chunk_size=chunk_size, old_chunks=chunks, progress_func=progress_func)
        if not (new_chunks is chunks):
            chunks = new_chunks
            yield chunks
        else:
            await asyncio.sleep(update_interval)


def chunks_to_dict(chunks, chunk_size):
    return {'chunks': [c.__dict__ for c in sorted(list(chunks), key=lambda c: c.filename + f'{c.pos:016}')],
            'chunk_size': chunk_size}

def chunks_to_json(chunks, chunk_size):
    return json.dumps(chunks_to_dict(chunks, chunk_size), indent=2)

def dict_to_chunks(data):
    chunk_size = data['chunk_size']
    chunks = [FileChunk(**d) for d in data['chunks']]
    return chunks, chunk_size

def json_to_chunks(json_str):
    return dict_to_chunks(json.loads(json_str))
