from typing import List
import os, json, hashlib, asyncio
import aiofiles, aiofiles.os

PART_SIZE = 16*1024*1024

class FileChunk(object):
    filename: str       # path + filename
    pos: int            # chunk start position in bytes
    size: int           # part size in bytes
    file_size: int      # size of complete file in bytes
    file_mtime: int     # last modified (unix timestamp)
    sha1: str           # SHA1 hex checksum of part
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


async def _hash_file(basedir: str, relpath: str, file_progress_func=None) -> List[FileChunk]:
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

    async with aiofiles.open(path, 'rb') as f:
        while pos < st.st_size:
            assert (pos <= st.st_size)
            sz = min((st.st_size-pos), PART_SIZE)
            res.append(FileChunk(
                    filename=relpath,
                    pos=pos,
                    size=sz,
                    file_size=st.st_size,
                    file_mtime=int(st.st_mtime+0.5),
                    sha1=hashlib.sha1(await f.read(sz)).hexdigest()))
            pos += sz
            if file_progress_func:
                file_progress_func(relpath, sz, pos, st.st_size)

    return res


async def hash_dir(basedir: str, old_chunks=None, progress_func=None) -> List[FileChunk]:
    '''
    Scan given directory and generate a list of FileChunks of its contents.
    :param basedir: Folders to scan
    :param progress_func: Progress report callback - func(cur_filename, file_progress, total_progress)
    :return:
    '''
    fnames = []
    for root, d_names, f_names in os.walk(basedir, topdown=False, onerror=None, followlinks=False):
        for f in f_names:
            fnames.append(os.path.relpath(os.path.join(root, f), basedir))

    # Map filename -> chunk for faster lookup of current list
    fn_to_old_chunk = {c.filename:c for c in old_chunks} if old_chunks else {}

    async def file_needs_rehash(filename: str):
        try:
            c = fn_to_old_chunk[filename]
            s = await aiofiles.os.stat(os.path.join(basedir, filename))
            return c.file_size != s.st_size or c.file_mtime != int(s.st_mtime + 0.5)
        except FileNotFoundError:
            return True
        except KeyError:
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
            progress_func(path, total_progress=1-float(total_remaining)/total_size, file_progress=perc)

    # Hash files as needed
    res = []
    for fn in fnames:
        if fn not in files_needing_rehash:
            res.extend([c for c in old_chunks if c.filename == fn])
            total_remaining -= res[-1].file_size
        else:
            res.extend(await _hash_file(basedir, fn, file_progress_func=file_progress))

    # Sort results by filename and then by chunk position
    return sorted(res, key=lambda c: c.filename+f'{c.pos:016}')



async def monitor_folder_forever(basedir: str, update_interval: float, progress_func=None):
    '''
    Test 'basedir' for changes every 'update_interval_secs' and yield a new list of FileChunks when it happens.
    (This is an async generator, intended to be used with "async for" syntax.)

    :param basedir: Folder to watch
    :param update_interval: Update interval in seconds
    :param progress_func: Callback for progress reports when hashing - func(cur_filename, file_progress, total_progress)
    '''
    chunks = []
    while True:
        new_chunks = await hash_dir(basedir, chunks, progress_func=progress_func)
        if not (new_chunks is chunks):
            chunks = new_chunks
            yield chunks
        else:
            await asyncio.sleep(update_interval)


def chunks_to_json(chunks):
    return json.dumps([c.__dict__ for c in chunks], indent=4)


def json_to_chunks(json_str):
    return [FileChunk(**d) for d in json.loads(json_str)]
