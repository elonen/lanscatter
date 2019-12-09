from util.ratelimiter import RateLimiter
from pathlib import Path
from chunker import FileChunk, HashFunc
from aiohttp import web, ClientSession
from typing import Tuple, Optional
from contextlib import suppress
import aiofiles, os, time, asyncio, aiohttp

class FileIO:
    '''
    Helper for reading and writing chunks from/to files + downloading / uploading them over network.
    Supports rate limiting and checks that file operations stay inside given base directory.
    '''
    basedir: Path
    dl_limiter: RateLimiter
    ul_limiter: RateLimiter

    FILE_BUFFER_SIZE = 256 * 1024
    DOWNLOAD_BUFFER_MAX = 64 * 1024
    NETWORK_BUFFER_MIN = 8 * 1024

    def __init__(self, basedir: Path, dl_rate_limit: float, ul_rate_limit: float):
        '''
        :param basedir: Folder to limit read/write into.
        :param dl_rate_limit: Maximum download rate in Mbit/s
        :param ul_rate_limit: Maximum upload rate in Mbit/s
        '''
        assert(isinstance(basedir, Path))
        basedir = basedir.resolve()
        if not basedir.is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot serve from it.')
        self.basedir = basedir
        self.dl_limiter = RateLimiter(dl_rate_limit * 1024 * 1024 / 8, period=1.0, burst_factor=2.0)
        self.ul_limiter = RateLimiter(ul_rate_limit * 1024 * 1024 / 8, period=1.0, burst_factor=2.0)

    def resolve_and_sanitize(self, relative_path, must_exist=False):
        '''
        Check that given relative path stays inside basedir and return an absolute path string.
        :return: Absolute Path() object
        '''
        p = (self.basedir / Path(relative_path)).resolve()
        if self.basedir not in p.parents:
            raise PermissionError(f'Filename points outside basedir: {str(relative_path)}')
        return p

    def open_and_seek(self, path: str, pos: int, for_write: bool = False):
        '''
        Helper. Open file for read or write and seek/grow to given size.
        :param path: File to open
        :param pos: File position to seek/grow into
        :param for_write: If true, open for write, otherwise for read
        :return: Aiofiles file handle
        '''
        path = self.resolve_and_sanitize(path)
        if for_write:
            mode = ('r+b' if path.is_file() else 'wb')
            os.makedirs(os.path.dirname(path), exist_ok=True)
        else:
            if not path.is_file():
                raise FileNotFoundError(f'Cannot read, no such file: "{str(path)}"')
            mode = 'rb'

        # Return an "async with" compatible object that opens file and seeks
        class _TempAsyncMgr:
            async def __aenter__(self):
                self.f = await aiofiles.open(path, mode=mode)
                await self.f.seek(pos)
                return self.f
            async def __aexit__(self, exc_type, exc, tb):
                self.f.close()
        return _TempAsyncMgr()


    async def upload_chunk(self, chunk: FileChunk, request: web.Request)\
            -> Tuple[web.StreamResponse, Optional[float]]:
        '''
        Read given chunk from disk and stream out as a HTTP response
        :param chunk: Chunk to read
        :param request: HTTP request to answer
        :return: Tuple(Aiohttp.response, float(seconds the upload took) or None if it no progress was made)
        '''
        response = None
        remaining = chunk.size
        start_t = time.time()
        try:
            async with self.open_and_seek(chunk.filename, chunk.pos, for_write=False) as f:
                # Ok, read chunk from file and stream it out
                response = web.StreamResponse(
                    status=200,
                    reason='OK',
                    headers={'Content-Type': 'application/octet-stream', 'Content-Disposition': 'inline'})
                await response.prepare(request)

                buff_in, buff_out = bytearray(self.FILE_BUFFER_SIZE), None

                async def read_file():
                    nonlocal buff_in, remaining, chunk
                    if remaining > 0:
                        if remaining < len(buff_in):
                            buff_in = bytearray(remaining)
                        cnt = await f.readinto(buff_in)
                        if cnt != len(buff_in):
                            raise web.HTTPNotFound(reson=f'Filesize mismatch / "{str(chunk.filename)}" changed? Read {cnt} but expected {len(buff_in)}.')
                        remaining -= cnt
                    else:
                        buff_in = None

                async def write_http():
                    nonlocal buff_out
                    if not buff_out:
                        buff_out = bytearray(self.FILE_BUFFER_SIZE)
                    else:
                        i, cnt = 0, len(buff_out)
                        while cnt > 0:
                            limited_n = int(await self.ul_limiter.acquire(cnt, self.NETWORK_BUFFER_MIN))
                            await response.write(buff_out[i:(i + limited_n)])
                            i += limited_n
                            cnt -= limited_n

                while remaining > 0:
                    await asyncio.gather(read_file(), write_http())  # Read and write concurrently
                    buff_in, buff_out = buff_out, buff_in  # Swap buffers
                await write_http()  # Write once more to flush buff_out

                await response.write_eof()
                return response, (time.time() - start_t)

        except asyncio.exceptions.CancelledError as e:
            # If client disconnected, predict how long upload would have taken
            try:
                predicted_time = (time.time() - start_t) / (1-remaining/chunk.size)
                return response, predicted_time
            except ZeroDivisionError:
                return response, None

        except PermissionError as e:
            raise web.HTTPForbidden(reason=str(e))
        except FileNotFoundError as e:
            raise web.HTTPNotFound(reason=str(e))
        except Exception as e:
            raise web.HTTPInternalServerError(reason=str(e))


    async def copy_chunk_locally(self, copy_from: FileChunk, copy_to: FileChunk) -> None:
        '''
        Locally copy chunk contents from one file (+position) to another.
        :param copy_from: Where to copy from
        :param copy_to: Where to copy into
        :return: None
        '''
        if copy_from.hash != copy_to.hash:
            raise ValueError(f"From and To chunks must have same hash (was: '{copy_from.hash}' vs '{copy_to.hash}').")

        if copy_from.filename == copy_to.filename and copy_from.pos == copy_to.pos:
            return  # Nothing to do

        async with self.open_and_seek(copy_from.filename, copy_from.pos, for_write=False) as inf:
            if not inf:
                raise FileNotFoundError(f'Local copy failed, no such file: {str(copy_from.filename)}')
            async with self.open_and_seek(copy_to.filename, copy_to.pos, for_write=True) as outf:
                remaining = copy_from.size
                buff = bytearray(self.FILE_BUFFER_SIZE)
                while remaining > 0:
                    if remaining < len(buff):
                        buff = bytearray(remaining)
                    read = await inf.readinto(buff)
                    if read != len(buff):
                        raise IOError(
                            f"'File {str(copy_from.filename)}' changed? Expected {len(buff)} bytes but got {read}.")
                    await outf.write(buff)
                    remaining -= read


    async def download_chunk(self, chunk: FileChunk, url: str, http_session: ClientSession) -> None:
        '''
        Download chunk from given URL and write directly into (the middle of a) file as specified by FileChunk.
        :param chunk: Specs for chunk to get
        :param url: URL to download from
        :param http_session: AioHTTP session to use for GET.
        '''
        async with http_session.get(url, raise_for_status=True) as resp:
            async with self.open_and_seek(chunk.filename, chunk.pos, for_write=True) as outf:
                if resp.status != 200:  # some error
                    raise IOError(f'Unknown/unsupported HTTP status: {resp.status}')
                else:
                    read_bytes = b'-'
                    csum = HashFunc()
                    while read_bytes:
                        limited_n = int(await self.dl_limiter.acquire(self.DOWNLOAD_BUFFER_MAX, self.NETWORK_BUFFER_MIN))
                        read_bytes = await resp.content.read(limited_n)
                        self.dl_limiter.unspend(limited_n - len(read_bytes))
                        await asyncio.gather(
                            outf.write(read_bytes),
                            csum.update_async(read_bytes))
                    if csum.result() != chunk.hash:
                        raise IOError(f'Checksum error verifying {chunk.hash} from {url}')
            os.truncate(self.resolve_and_sanitize(chunk.filename), chunk.file_size)



    async def change_mtime(self, path, mtime):
        path = self.resolve_and_sanitize(path)
        os.utime(str(path), (mtime, mtime))

    async def create_folders(self, path):
        path = self.resolve_and_sanitize(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)

    async def size_and_mtime(self, path):
        path = self.resolve_and_sanitize(path)
        s = await aiofiles.os.stat(str(path))
        return s.st_size, s.st_mtime

    async def remove_file_and_paths(self, path):
        path = self.resolve_and_sanitize(path)
        path.unlink()
        for d in path.parents:
            if d == self.basedir:
                break
            with suppress(OSError):
                d.rmdir()
