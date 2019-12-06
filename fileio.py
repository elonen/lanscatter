from util.ratelimiter import RateLimiter
from pathlib import Path
from chunker import FileChunk
from aiohttp import web, ClientSession
import aiofiles, io, hashlib

class FileIO(object):
    '''
    Helper for reading and writing chunks from/to files + downloading / uploading them over network.
    Supports rate limiting and checks that file operations stay inside given base directory.
    '''

    _basedir: Path
    _dl_limiter: RateLimiter
    _ul_limiter: RateLimiter

    _FILE_BUFFER_SIZE = 256 * 1024
    _DOWNLOAD_BUFFER_MAX = 64*1024
    _NETWORK_BUFFER_MIN = 8*1024

    def __init__(self, basedir: Path, dl_rate_limit: float, ul_rate_limit: float):
        '''
        :param basedir: Folder to limit read/write into.
        :param dl_rate_limit: Maximum download rate in Mbit/s
        :param ul_rate_limit: Maximum upload rate in Mbit/s
        '''
        if not basedir.is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot serve from it.')
        self._basedir = basedir
        self._dl_limiter = RateLimiter(dl_rate_limit*1024*1024/8, period=1.0, burst_factor=2.0)
        self._ul_limiter = RateLimiter(ul_rate_limit*1024*1024/8, period=1.0, burst_factor=2.0)


    def _open_and_seek(self, path_str: str, pos: int, for_write: bool):
        '''
        Helper. Open file for read or write and seek/grow to given size.
        :param path_str: File to open
        :param pos: File position to seek/grow into
        :param for_write: If true, open for write, otherwise for read
        :return: Aiofiles file handle
        '''
        path = self._basedir / Path(path_str)
        if self._basedir not in path.parents:
            raise PermissionError(f'Filename points outside basedir: {str(path)}')
        if for_write:
            mode = ('r+b' if path.is_file() else 'wb')
        else:
            if not path.is_file():
                raise FileNotFoundError(f'Cannot read, no such file: "{str(path)}"')
            mode = 'rb'

        class _TempAsyncMgr:
            async def __aenter__(self):
                self.f = await aiofiles.open(path, mode=mode)
                await self.f.seek(pos)
                return self.f
            async def __aexit__(self, exc_type, exc, tb):
                self.f.close()
        return _TempAsyncMgr()


    async def upload_chunk(self, chunk: FileChunk, request: web.Request) -> web.StreamResponse:
        '''
        Read given chunk from disk and stream out as a HTTP response
        :param chunk: Chunk to read
        :param request: HTTP request to answer
        :return: Aiohttp response object
        '''
        try:
            async with self._open_and_seek(chunk.filename, chunk.pos, for_write=False) as f:
                # Ok, read chunk from file and stream it out
                response = web.StreamResponse(
                    status=200,
                    reason='OK',
                    headers={'Content-Type': 'application/octet-stream', 'Content-Disposition': 'inline'})
                await response.prepare(request)
                remaining = chunk.size
                buff = bytearray(self._FILE_BUFFER_SIZE)
                while remaining > 0:

                    # Read file
                    if remaining < len(buff):
                        buff = bytearray(remaining)
                    cnt = await f.readinto(buff)
                    if cnt != len(buff):
                        raise web.HTTPNotFound(reson=f'Filesize mismatch / "{str(path)}" changed? Read {cnt} but expected {len(buff)}.')
                    remaining -= cnt

                    # Throttle response bandwidth
                    i = 0
                    while cnt > 0:
                        limited_n = int(await self._ul_limiter.acquire(cnt, self._NETWORK_BUFFER_MIN))
                        await response.write(buff[i:(i + limited_n)])
                        i += limited_n
                        cnt -= limited_n

                await response.write_eof()
                return response

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

        async with self._open_and_seek(copy_from.filename, copy_from.pos, for_write=False) as inf:
            if not inf:
                raise FileNotFoundError(f'Local copy failed, no such file: {str(copy_from.filename)}')
            async with self._open_and_seek(copy_to.filename, copy_to.pos, for_write=True) as outf:
                remaining = copy_from.size
                buff = bytearray(self._FILE_BUFFER_SIZE)
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
        Download chunk from given URL and write directly into file as specified by FileChunk.
        :param chunk: Specs for chunk to download (& write to disk)
        :param url: URL to download from
        :param http_session: AioHTTP session to use for GET.
        '''
        async with self._open_and_seek(chunk.filename, chunk.pos, for_write=True) as outf:
            async with http_session.get(url, raise_for_status=True) as resp:
                if resp.status != 200:  # some error
                    raise Exception(f'Unknown/unsupported HTTP status: {resp.status}')
                else:
                    read_bytes = b'-'
                    csum = hashlib.blake2b(digest_size=12)
                    while read_bytes:
                        limited_n = int(await self._dl_limiter.acquire(self._DOWNLOAD_BUFFER_MAX, self._NETWORK_BUFFER_MIN))
                        read_bytes = await resp.content.read(limited_n)
                        self._dl_limiter.return_unused(limited_n - len(read_bytes))
                        csum.update(read_bytes)
                        await outf.write(read_bytes)
                    if csum.hexdigest() != chunk.hash:
                        raise Exception(f'Checksum error verifying {chunk.hash} from {url}')
