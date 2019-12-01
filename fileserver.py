from aiohttp import web
from pathlib import Path
from typing import List, Dict, Callable
import ssl, asyncio, aiofiles, socket, hashlib, argparse
from filechunks import FileChunk, monitor_folder_forever, chunks_to_json
from contextlib import suppress
from util.ratelimiter import RateLimiter
from common import *

# HTTP file server that continuously auto-scans given directory,
# serves file chunks to clients, and maintains a list of P2P URLs so network load get distributed
# like a semi-centralized bittorrent.

class FileServer(object):
    _chunks: List[FileChunk]
    _hash_to_chunk: Dict[str, FileChunk]
    _basedir: str
    _filelist: str

    def __init__(self, basedir: str, status_func: Callable, chunk_size: int = -1, http_cache_time: int = 60*5):
        self._chunks = []           # List fo all FileChunks
        self._hash_to_chunk = {}    # Map hash -> FileChunk
        self._basedir = basedir     # Directory to serve/monitor files from
        self._base_url = ''         # URL to access this server
        self._filelist_etag = ''    # MD5 sum of file/chunk list
        self._http_cache_time = http_cache_time
        self._peers_map = {}
        self._peers_etag = ''
        self._chunk_size = chunk_size   # Only required on master server
        self._status_func = status_func

        if not Path(basedir).is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot serve from it.')

        # If no status function is given, make a dummy one
        if not self._status_func:
            def dummy_status_func(progress: float=None, cur_status: str=None, log_error: str=None, log_info: str=None, popup: bool = False):
                pass
            self._status_func = dummy_status_func


    def base_url(self):
        return self._base_url

    def set_chunks(self, chunks):
        '''
        Receive new chunks from directory scanner and start serving them.
        '''
        self._status_func(log_info='Chunks updated. Now serving new ones.')
        self._chunks = chunks
        self._hash_to_chunk = {c.hash: c for c in chunks}

        filelist = chunks_to_json(self._chunks, self._chunk_size)
        self._filelist_etag = hashlib.md5(filelist.encode('utf-8')).hexdigest()

        # Make sure server's URL is in peer list for all local hashes
        if self._base_url:
            for c in self._chunks:
                if c.hash not in self._peers_map:
                    self._peers_map[c.hash] = []
                url = f'{self._base_url}/chunk/{c.hash}'
                if url not in self._peers_map[c.hash]:
                    self._peers_map[c.hash].append(url)


    def run_server(self, serve_filelist=True, port=14433, https_cert=None, https_key=None, ul_limit: float = 10000):
        '''
        Create HTTP(S) server loop.

        :param serve_filelist: Should this server serve /filelist and /peers, or only chunk data?
            True for master server, False for peer2peer servers.
        :param port: TCP port to listen at.
        :param https_cert: PEM filename or None
        :param https_key: PEM filename or None

        :return: Asyncio task
        '''
        base_path = Path(self._basedir)

        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        self._base_url = ('https://' if (https_cert and https_key) else 'http://') + ip_addr + ':' + str(port)

        self._status_func(log_info=f'Starting {"master" if serve_filelist else "p2p"} file server on {self._base_url}.')

        ul_limiter = RateLimiter(ul_limit*1024*1024/8, 1.0, burst_factor=2.0)

        # HTTP HANDLER - Serve requested file chunk to a client
        async def handle_get_chunk(request):
            '''
            HTTP GET handler that serves out a file chunk with given hash.
            '''
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
            h = request.match_info.get('hash', 'noname')

            if h not in self._hash_to_chunk:
                return web.Response(status=404, text=f'404 NOT FOUND. Hash not found on this server: {h}')
            else:
                # Check if we have the chunk
                c = self._hash_to_chunk[h]

                path = base_path / c.filename
                if base_path not in path.parents:
                    return web.Response(status=403, text=f'403 FORBIDDEN. Filename points outside basedir?? {str(path)}')
                elif not path.is_file():
                    return web.Response(status=404, text=f'404 NOT FOUND. Host should have chunk, but file is missing! {str(path)}')
                else:
                    # Ok, read chunk from file and stream it out
                    response = web.StreamResponse(
                        status=200,
                        reason='OK',
                        headers={'Content-Type': 'application/octet-stream', 'Content-Disposition': 'inline'})
                    await response.prepare(request)
                    async with aiofiles.open(path, mode='rb') as f:
                        await f.seek(c.pos)
                        remaining = c.size
                        buff = bytearray(256*1024)
                        while remaining > 0:

                            # Read file
                            if remaining < len(buff):
                                buff = bytearray(remaining)
                            cnt = await f.readinto(buff)
                            if cnt != len(buff):
                                self._status_func(log_error=f'ERROR: File "{str(path)}" changed? Read {cnt} but expected {len(buff)}.')
                                return web.Response(status=500, text=f'500 INTERNAL ERROR. Filesize mismatch.')
                            remaining -= cnt

                            # Throttle response bandwidth
                            i = 0
                            while cnt > 0:
                                limited_n = int(await ul_limiter.acquire(cnt, 4096))
                                await response.write(buff[i:(i+limited_n)])
                                i += limited_n
                                cnt -= limited_n

                    await response.write_eof()
                    return response


        # HTTP HANDLER - Return filelist (hash list) to a client
        async def handle_get_filelist(request):
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
            if self._filelist_etag and request.headers.get('If-None-Match') == self._filelist_etag:
                return web.HTTPNotModified()
            return web.Response(
                status=200, body=chunks_to_json(self._chunks, self._chunk_size),
                content_type='application/json',
                headers={'ETag': self._filelist_etag, 'Cache-Control': f'public, max-age: {self._http_cache_time}'})


        # HTTP HANDLER - Return a list of URLs that hashes can be downloaded from
        async def handle_get_peers(request):
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
            if self._peers_etag and request.headers.get('If-None-Match') == self._peers_etag:
                return web.HTTPNotModified()
            res = json.dumps(self._peers_map, indent=4)
            self._peers_etag = hashlib.md5(res.encode('utf-8')).hexdigest()
            return web.Response(
                status=200, body=res,
                content_type='application/json',
                headers={'ETag': self._peers_etag, 'Cache-Control': f'public, max-age: 30'})


        # HTTP HANDLER - Add/delete hash -> URL mappings (peer list)
        async def update_peer_urls(request):
            self._status_func(log_info=f"[{request.remote}] POST {request.path_qs}")
            try:
                ops = await request.json()
            except json.decoder.JSONDecodeError as e:
                return web.Response(status=400, body='400 BAD REQUEST: JSON / ' + str(e), content_type='text/plain')
            bad_hashes = []
            for line in ops:
                if not isinstance(line, list) or len(line) != 3 or (line[0] not in ('ADD', 'DEL')):
                    return web.Response(
                        status=400, body='400 BAD REQUEST: please send a json list of ["ADD|DEL", "<HASH>", "<URL>"]',
                        content_type='text/plain')
                else:
                    op, h, url = line[0], line[1].strip(), line[2]
                    c = self._hash_to_chunk.get(h)
                    if c is None:
                        bad_hashes.append(h)
                    else:
                        if h not in self._peers_map:
                            self._peers_map[h] = []
                        peers = self._peers_map[h]
                        if op == 'ADD' and (url not in peers):
                            peers.append(url)
                        elif op == 'DEL' and (url in peers):
                            if not url.startswith(self._base_url):  # don't remove server's own url even if requested
                                peers.remove(url)

            # Return updated list
            res = json.dumps(self._peers_map, indent=4)
            self._peers_etag = hashlib.md5(res.encode('utf-8')).hexdigest()
            if bad_hashes:
                res = json.dumps({**self._peers_map, 'ERROR_MISSING_HASHES': bad_hashes}, indent=4)
                self._status_func(log_error=f"WARNING: Client reported URLs for unknown hashes: {str(bad_hashes)}")

            return web.Response(
                status=200, body=json.dumps(self._peers_map, indent=4),
                content_type='application/json',
                headers={'ETag': self._peers_etag, 'Cache-Control': f'public, max-age: 30'})


        # Create aiohttp web app
        app = web.Application()
        app.add_routes([web.get('/chunk/{hash}', handle_get_chunk)])
        if serve_filelist:
            app.add_routes([
                    web.get('/filelist', handle_get_filelist),
                    web.get('/peers', handle_get_peers),
                    web.post('/peers', update_peer_urls),
                ])

        # Setup HTTPS if certificate and key are provided (otherwise use plain HTTP):
        context = None
        if https_cert and https_key:
            self._status_func(log_info=f"SSL: Using {https_cert} and {https_key} for serving HTTPS.")
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_default_certs()
            context.load_cert_chain(certfile=https_cert, keyfile=https_key)
        else:
            self._status_func(log_info=f"SSL cert not provided. Serving plain HTTP.")

        async def wrap_runner():
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, port=port, ssl_context=context)
            await site.start()
        return wrap_runner()


async def run_master_server(base_dir: str, port: int,
                            dir_scan_interval: float = 20, status_func=None, ul_limit: float = 10000,
                            chunk_size=64*1024*1024,
                            https_cert=None, https_key=None):
    async def dir_scanner():
        def progress_func_adapter(cur_filename, file_progress, total_progress):
            if status_func:
                status_func(progress=total_progress, cur_status=f'Hashing "{cur_filename}" ({int(file_progress * 100 + 0.5)}% done)')
        async for new_chunks in monitor_folder_forever(base_dir, dir_scan_interval, progress_func_adapter, chunk_size=chunk_size):
            server.set_chunks(new_chunks)

    server = FileServer(base_dir, chunk_size=chunk_size, status_func=status_func)
    await asyncio.gather(
        dir_scanner(),
        server.run_server(serve_filelist=True, port=port, ul_limit=ul_limit, https_cert=https_cert, https_key=https_key))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', help='Directory to serve files from')
    parser.add_argument('-p', '--port', dest='port', type=int, default=14433, help='HTTP(s) server port')
    parser.add_argument('--ul-rate', dest='ul_limit', type=float, default=10000, help='Rate limit uploads, Mb/s')
    parser.add_argument('--chunksize', dest='chunksize', type=int, default=64*1024*1024, help='Chunk size for splitting files (in bytes)')
    parser.add_argument('--sslcert', type=str, default=None, help='SSL certificate file for HTTPS (optional)')
    parser.add_argument('--sslkey', type=str, default=None, help='SSL key file for HTTPS (optional)')
    parser.add_argument('--json', dest='json', action='store_true', default=False, help='Show status as JSON (for GUI usage)')
    args = parser.parse_args()
    status_func = json_status_func if args.json else human_cli_status_func
    with suppress(KeyboardInterrupt):
        asyncio.run(run_master_server(
            base_dir=args.dir, port=args.port, ul_limit=args.ul_limit,
            https_cert=args.sslcert, https_key=args.sslkey,
            chunk_size=args.chunksize, status_func=status_func))


if __name__ == "__main__":
    main()
