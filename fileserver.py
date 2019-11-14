from aiohttp import web
from pathlib import Path
from typing import List, Dict, Callable
import ssl, asyncio, aiofiles, socket, json, hashlib, argparse

from filechunks import FileChunk, monitor_folder_forever, chunks_to_json, json_to_chunks


class FileServer(object):
    _chunks: List[FileChunk]
    _hash_to_chunk: Dict[str, FileChunk]
    _basedir: str
    _manifest: str

    def __init__(self, basedir: str, http_cache_time: int = 60*5, status_func: Callable=None):
        self._chunks = []           # List fo all FileChunks
        self._hash_to_chunk = {}    # Map sha1sum -> FileChunk
        self._basedir = basedir     # Directory to serve/monitor files from
        self._base_url = ''         # URL to access this server
        self._manifest_etag = ''    # MD5 sum of manifest
        self._http_cache_time = http_cache_time
        self._peers_map = {}
        self._peers_etag = ''
        self._status_func = status_func

        if not Path(basedir).is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot serve from it.')

        # If no status function is given, make a dummy one
        if not self._status_func:
            def dummy_status_func(progress: float=None, cur_status: str=None, log_error: str=None, log_info: str=None):
                pass
            self._status_func = dummy_status_func


    def base_url(self):
        return self._base_url

    def set_chunks(self, chunks):
        self._status_func(log_info='Files changed. Now serving new ones.')
        self._chunks = chunks
        self._hash_to_chunk = {c.sha1: c for c in chunks}

        manifest = chunks_to_json(self._chunks)
        self._manifest_etag = hashlib.md5(manifest.encode('utf-8')).hexdigest()

        # Make sure server's URL is in peer list for all local hashes
        if self._base_url:
            for c in self._chunks:
                url = f'{self._base_url}/chunk/{c.sha1}'
                if c.sha1 not in self._peers_map:
                    self._peers_map[c.sha1] = []
                peers = self._peers_map[c.sha1]
                if url not in peers:
                    peers.append(url)

    def run_server(self, serve_manifest=True, port=14433, https_cert=None, https_key=None):
        base_path = Path(self._basedir)

        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        self._base_url = ('https://' if (https_cert and https_key) else 'http://') + ip_addr + ':' + str(port)

        self._status_func(log_info=f'Starting file server on {self._base_url}.')

        # HTTP HANDLER - Serve requested file chunk to a client
        async def handle_get_chunk(request):
            '''
            HTTP GET handler that serves out a file chunk with given sha1 sum.
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
                        size = c.size
                        while size > 0:
                            buf_size = min(64*1024, size)
                            buf = await f.read(buf_size)
                            if len(buf) != buf_size:
                                self._status_func(log_error=f'ERROR: File "{str(path)}" changed? Read {len(buf)} but expected {buf_size}.')
                                return web.Response(status=500, text=f'500 INTERNAL ERROR. Filesize mismatch.')
                            await response.write(buf)
                            size -= buf_size
                    await response.write_eof()
                    return response


        # HTTP HANDLER - Return manifest (hash list) to a client
        async def handle_get_manifest(request):
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
            if request.headers.get('If-None-Match') == self._manifest_etag:
                return web.HTTPNotModified()
            return web.Response(
                status=200, body=chunks_to_json(self._chunks),
                content_type='application/json',
                headers={'ETag': self._manifest_etag, 'Cache-Control': f'public, max-age: {self._http_cache_time}'})


        # HTTP HANDLER - Return a list of URLs that hashes can be downloaded from
        async def handle_get_peers(request):
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
            if request.headers.get('If-None-Match') == self._peers_etag:
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
        if serve_manifest:
            app.add_routes([
                    web.get('/manifest', handle_get_manifest),
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

        # Return async server task
        task = web._run_app(app, ssl_context=context, port=port)
        return task


async def run_master_server(base_dir: str, port: int,
                            dir_scan_interval: float = 20, status_func=None,
                            https_cert=None, https_key=None ):
    async def dir_scanner():
        def progress_func_adapter(cur_filename, file_progress, total_progress):
            if status_func:
                status_func(progress=total_progress, cur_status=f'Hashing "{cur_filename}" ({int(file_progress * 100 + 0.5)}% done)')
        async for new_chunks in monitor_folder_forever(base_dir, dir_scan_interval, progress_func_adapter):
            server.set_chunks(new_chunks)

    server = FileServer(base_dir, status_func=status_func)
    await asyncio.gather(
        dir_scanner(),
        server.run_server(serve_manifest=True, port=port, https_cert=https_cert, https_key=https_key))


# ------------------------

async def async_main(basedir: str, port: int, cert: str, key: str):

    def test_status_func(progress: float = None, cur_status: str = None, log_error: str = None, log_info: str = None):
        if progress is not None:
            print(f" | Progress: {int(progress*100+0.5)}%")
        if cur_status is not None:
            print(f" | Cur status: {cur_status}")
        if log_error is not None:
            print(f" | ERROR: {log_error}")
        if log_info is not None:
            print(f" | INFO: {log_info}")

    await asyncio.gather(run_master_server(basedir, port, https_cert=cert, https_key=key, status_func=test_status_func))


if __name__== "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', help='Directory to serve files from.')
    parser.add_argument('-p', '--port', dest='port', type=int, default=14433, help='HTTP(s) port to server at.')
    parser.add_argument('--sslcert', type=str, default=None, help='SSL certificate file for HTTPS (optional)')
    parser.add_argument('--sslkey', type=str, default=None, help='SSL key file for HTTPS (optional)')

    args = parser.parse_args()
    asyncio.run(async_main(basedir=args.dir, port=args.port, cert=args.sslcert, key=args.sslkey))
