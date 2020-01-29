from pathlib import Path, PurePosixPath
from aiohttp import web, ClientSession
from typing import Tuple, Optional, Callable, Iterable
from contextlib import suppress
import aiofiles, os, time, asyncio, aiohttp, mmap
import lz4.frame

from .common import Defaults
from .chunker import FileChunk, HashFunc
from .ratelimiter import RateLimiter


async def _buffered_async_in_out(producer: Callable, consumer: Callable, initial_buffers: Iterable, timeout=999999):
    """
    Buffered async producer/consumer loop framework, with optional timeout.
    Timeout guard raises a TimeoutError if neither producer nor consumer do any work in 'timeout' seconds.

    :param producer: Async function that takes a buffer, writes stuff to it and returns the buffer or None if all done.
    :param consumer: Async function that takes a buffer and does something with the data (or not)
    :param initial_buffers: Arbitrary number of pre-create buffers to use for IO.
    :param timeout: Timeout after this many seconds if no progress is made.
    """
    end_trigger = asyncio.Event()
    last_progress_t = time.time()

    free_buffers, full_buffers = asyncio.Queue(), asyncio.Queue()
    for b in initial_buffers:
        await free_buffers.put(b)

    async def producer_loop():
        nonlocal last_progress_t
        while True:
            b = await free_buffers.get()
            b = await producer(b)
            await full_buffers.put(b)  # Signal end for consumer with None
            last_progress_t = time.time()
            if b is None:
                break

    async def consumer_loop():
        nonlocal last_progress_t
        while True:
            b = await full_buffers.get()
            if b is None:
                end_trigger.set()  # let no_progress_timeout_guard() know we're done
                break
            else:
                await consumer(b)
                await free_buffers.put(b)
                last_progress_t = time.time()

    async def no_progress_timeout_guard():
        nonlocal end_trigger, last_progress_t
        while not end_trigger.is_set():
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(end_trigger.wait(), timeout=timeout/4)
            if time.time() - last_progress_t > timeout:
                raise TimeoutError(f"Timeout. No progress in {Defaults.TIMEOUT_WHEN_NO_PROGRESS} seconds.")

    await asyncio.gather(producer_loop(), consumer_loop(), no_progress_timeout_guard())


class FileIO:
    """
    Helper for reading and writing chunks from/to files + downloading / uploading them over network.
    Supports rate limiting and checks that file operations stay inside given base directory.
    """
    basedir: Path
    dl_limiter: RateLimiter
    ul_limiter: RateLimiter

    def __init__(self, basedir: Path, dl_rate_limit: float, ul_rate_limit: float):
        """
        :param basedir: Folder to limit read/write into.
        :param dl_rate_limit: Maximum download rate in Mbit/s
        :param ul_rate_limit: Maximum upload rate in Mbit/s
        """
        assert(isinstance(basedir, Path))
        basedir = basedir.resolve()
        if not basedir.is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot serve from it.')
        self.basedir = basedir
        self.dl_limiter = RateLimiter(dl_rate_limit * 1024 * 1024 / 8, period=1.0, burst_factor=2.0)
        self.ul_limiter = RateLimiter(ul_rate_limit * 1024 * 1024 / 8, period=1.0, burst_factor=2.0)

    def resolve_and_sanitize(self, relative_path, must_exist=False):
        """
        Check that given relative path stays inside basedir and return an absolute path string.
        :return: Absolute Path() object
        """
        assert '\\' not in str(relative_path), f"Non-posix path found: {str(relative_path)}"
        p = (self.basedir / relative_path).resolve()
        if self.basedir not in p.parents:
            raise PermissionError(f'Filename points outside basedir: {str(relative_path)}')
        return p

    def open_and_seek(self, path: str, pos: int, for_write: bool = False):
        """
        Helper. Open file for read or write and seek/grow to given size.
        :param path: File to open
        :param pos: File position to seek/grow into
        :param for_write: If true, open for write, otherwise for read
        :return: Aiofiles file handle
        """
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
            -> Tuple[web.StreamResponse, Optional[float], Optional[float]]:
        """
        Read given chunk from disk and stream out as a HTTP response.

        :param chunk: Chunk to read
        :param request: HTTP request to answer
        :param use_lz4: Compress with LZ4 if client accepts it
        :return: Tuple(Aiohttp.response, float(seconds the upload took) or None if it no progress was made, compr_ratio)
        """
        use_lz4 = (chunk.cmpratio < 0.95) and ('lz4' in str(request.headers.get('Accept-Encoding')))
        response = None
        remaining = chunk.size
        upload_size = 0
        start_t = time.time()
        try:
            async with self.open_and_seek(chunk.path, chunk.pos, for_write=False) as f:
                with lz4.frame.LZ4FrameCompressor() as lz:
                    # Ok, read chunk from file and stream it out
                    response = web.StreamResponse(
                        status=200,
                        reason='OK',
                        headers={'Content-Type': 'application/octet-stream', 'Content-Disposition': 'inline',
                                 'Content-Encoding': 'lz4' if use_lz4 else 'None'})
                    await response.prepare(request)
                    if use_lz4:
                        await response.write(lz.begin())

                    async def read_file(buff: bytearray):
                        nonlocal remaining, chunk
                        if remaining > 0:
                            if remaining < len(buff):
                                buff = bytearray(remaining)
                            cnt = await f.readinto(buff)
                            if cnt != len(buff):
                                raise web.HTTPNotFound(reason=f'Filesize mismatch / "{str(chunk.path)}" changed? Read {cnt} but expected {len(buff)}.')
                            remaining -= cnt
                            return buff

                    async def write_http(buff: bytearray):
                        nonlocal upload_size
                        i, cnt = 0, len(buff)
                        while cnt > 0:
                            limited_n = int(await self.ul_limiter.acquire(cnt, Defaults.NETWORK_BUFFER_MIN))
                            raw = memoryview(buff)[i:(i + limited_n)]
                            out = lz.compress(raw) if use_lz4 else raw
                            upload_size += len(out)
                            await response.write(out)
                            self.dl_limiter.unspend(limited_n - len(out))
                            i += limited_n
                            cnt -= limited_n

                    await _buffered_async_in_out(
                        producer=read_file, consumer=write_http, timeout=Defaults.TIMEOUT_WHEN_NO_PROGRESS,
                        initial_buffers=[bytearray(Defaults.FILE_BUFFER_SIZE) for i in range(5)])

                    if use_lz4:
                        await response.write(lz.flush())
                    return response, (time.time() - start_t), (upload_size / (chunk.size or 1))

        except asyncio.CancelledError as e:
            # If client disconnected, predict how long upload would have taken
            try:
                predicted_time = (time.time() - start_t) / (1-remaining/chunk.size)
                return response, predicted_time, None
            except ZeroDivisionError:
                return response, None, None

        except PermissionError as e:
            raise web.HTTPForbidden(reason=str(e))
        except FileNotFoundError as e:
            raise web.HTTPNotFound(reason=str(e))
        except Exception as e:
            # TODO: Write some magic string into chunked response to indicate error and then append message?
            #   (problem: there's no way of signaling the error message to client after Status 200 has been sent.)
            raise web.HTTPInternalServerError(reason=str(e))
        finally:
            if response is not None:
                with suppress(ConnectionResetError):
                    await response.write_eof()  # Close chunked response despite possible errors


    async def copy_chunk_locally(self, copy_from: FileChunk, copy_to: FileChunk) -> bool:
        """
        Locally copy chunk contents from one file (+position) to another.
        :param copy_from: Where to copy from
        :param copy_to: Where to copy into
        :return: True if chunk was copied ok, False if content hash didn't match anymore
        """
        if copy_from.path == copy_to.path and copy_from.pos == copy_to.pos:
            return True
        if copy_from.hash != copy_to.hash:
            raise ValueError(f"From and To chunks must have same hash (was: '{copy_from.hash}' vs '{copy_to.hash}').")

        async with self.open_and_seek(copy_from.path, copy_from.pos, for_write=False) as inf:
            if not inf:
                raise FileNotFoundError(f'Local copy failed, no such file: {str(copy_from.path)}')
            async with self.open_and_seek(copy_to.path, copy_to.pos, for_write=True) as outf:
                remaining = copy_from.size
                csum = HashFunc()

                async def read_file(buff: bytearray):
                    nonlocal remaining
                    if remaining > 0:
                        if remaining < len(buff):
                            buff = bytearray(remaining)
                        cnt = await inf.readinto(buff)
                        if cnt != len(buff):
                            raise IOError(f'Filesize mismatch / "{str(copy_from.path)}" changed? Read {cnt} but expected {len(buff)}.')
                        remaining -= cnt
                        return buff

                async def write_and_csum(buff):
                    await asyncio.gather(outf.write(buff), csum.update_async(buff))

            await _buffered_async_in_out(
                producer=read_file, consumer=write_and_csum,
                initial_buffers=[bytearray(Defaults.FILE_BUFFER_SIZE) for i in range(5)])

            return csum.result() == copy_from.hash


    async def download_chunk(self, chunk: FileChunk, url: str, http_session: ClientSession, file_size: int= -1) -> None:
        """
        Download chunk from given URL and write directly into (the middle of a) file as specified by FileChunk.
        :param chunk: Specs for chunk to get
        :param url: URL to download from
        :param http_session: AioHTTP session to use for GET.
        :param file_size: Size of complete file (optional). File will be truncated to this size.
        """
        with suppress(RuntimeError):  # Avoid dirty exit in aiofiles when Ctrl^C (RuntimeError('Event loop is closed')
            aio_timeout = aiohttp.ClientTimeout(connect=Defaults.TIMEOUT_WHEN_NO_PROGRESS, sock_connect=Defaults.TIMEOUT_WHEN_NO_PROGRESS)
            async with http_session.get(url, headers={'Accept-Encoding': 'lz4'}, timeout=aio_timeout) as resp:
                if resp.status != 200:  # some error
                    raise IOError(f'HTTP status {resp.status}')
                else:
                    use_lz4 = 'lz4' in str(resp.headers.get('Content-Encoding'))
                    with lz4.frame.LZ4FrameDecompressor() as lz:
                        async with self.open_and_seek(chunk.path, chunk.pos, for_write=True) as outf:
                            #csum = HashFunc()

                            async def read_http(buff_dummy):
                                limited_n = int(await self.dl_limiter.acquire(
                                    Defaults.DOWNLOAD_BUFFER_MAX, Defaults.NETWORK_BUFFER_MIN))
                                new_buff = await resp.content.read(limited_n)
                                self.dl_limiter.unspend(limited_n - len(new_buff))
                                return new_buff or None

                            async def write_and_csum(buff):
                                #await asyncio.gather(outf.write(buff), csum.update_async(buff))
                                await outf.write(lz.decompress(buff) if use_lz4 else buff)

                            await _buffered_async_in_out(
                                producer=read_http, consumer=write_and_csum, timeout=Defaults.TIMEOUT_WHEN_NO_PROGRESS,
                                initial_buffers=[True for i in range(5)])

                            # Checksuming here is actually waste of CPU since we'll rehash sync dir anyway when finished
                            #if csum.result() != chunk.hash:
                            #    raise IOError(f'Checksum error verifying {chunk.hash} from {url}')

                            if file_size >= 0:
                                await outf.truncate(file_size)


    async def change_mtime(self, path, mtime):
        path = self.resolve_and_sanitize(path)
        os.utime(str(path), (mtime, mtime))

    async def create_folders(self, path):
        """Create given directory and all parents"""
        path = self.resolve_and_sanitize(path)
        os.makedirs(path, exist_ok=True)

    async def size_and_mtime(self, path):
        path = self.resolve_and_sanitize(path)
        s = await aiofiles.os.stat(str(path))
        return s.st_size, s.st_mtime

    async def remove_file_and_paths(self, path):
        path = self.resolve_and_sanitize(path)
        if path.exists():
            if path.is_dir():
                with suppress(OSError):
                    path.rmdir()
            else:
                path.unlink()
        for d in path.parents:
            if d == self.basedir and d != '.':
                break
            with suppress(OSError):
                d.rmdir()


# TODO: Write unit test to confirm sanitizing works and prevents messing up files outside the sync dir
