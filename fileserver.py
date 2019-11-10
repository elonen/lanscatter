from aiohttp import web
from pathlib import Path
from typing import List, Dict
import ssl, asyncio, aiofiles, socket, json, hashlib

from filechunks import FileChunk, monitor_folder_forever, chunks_to_json, json_to_chunks


class FileServer(object):
    _chunks: List[FileChunk]
    _hash_to_chunk: Dict[str, FileChunk]
    _basedir: str
    _manifest: str

    def __init__(self, basedir: str, http_cache_time: int = 60*5):
        self._chunks = []           # List fo all FileChunks
        self._hash_to_chunk = {}    # Map sha1sum -> FileChunk
        self._basedir = basedir     # Directory to serve/monitor files from
        self._base_url = ''         # URL to access this server
        self._manifest = '[]'       # JSON list of FileChunks the server sends to clients
        self._manifest_etag = ''    # MD5 sum of manifest
        self._http_cache_time = http_cache_time

    def _update_manifest(self):
        # Make sure this server's URL is in all chunks
        if self._base_url:
            for c in self._chunks:
                u = self._base_url + "/chunk/" + c.sha1
                if c.has_chunk and u not in c.urls:
                    c.urls.append(u)
        # Make JSON manifest
        self._manifest = chunks_to_json(self._chunks)
        self._manifest_etag = hashlib.md5(self._manifest.encode('utf-8')).hexdigest()

    def set_chunks(self, chunks):
        self._chunks = chunks
        self._hash_to_chunk = {c.sha1: c for c in chunks}
        self._update_manifest()

    def run_server(self, serve_manifest=True, port=14433, https_cert=None, https_key=None):
        base_path = Path(self._basedir)

        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        self._base_url = ('https://' if (https_cert and https_key) else 'http://') + ip_addr + ':' + str(port)

        # HTTP HANDLER - Serve requested file chunk to a client
        async def handle_get_chunk(request):
            '''
            HTTP GET handler that serves out a file chunk with given sha1 sum.
            '''
            h = request.match_info.get('hash', "noname")
            print(f"[{request.remote}] GET hash {h}")
            if h not in self._hash_to_chunk:
                return web.Response(status=404, text=f'404 NOT FOUND. Hash not found on this server: {h}')
            else:
                # Check if we have the chunk
                c = self._hash_to_chunk[h]
                if not c.has_chunk:
                    return web.Response(status=404, text=f'404 NOT FOUND. This host knows about, but does not (yet) have chunk {h}')

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
                                raise Exception('file size mismatch!')
                            await response.write(buf)
                            size -= buf_size
                    await response.write_eof()
                    return response

        # HTTP HANDLER -Return manifest (hash list) to a client
        async def handle_get_manifest(request):
            print(f"[{request.remote}] GET manifest")
            if request.headers.get('If-None-Match') == self._manifest_etag:
                return web.HTTPNotModified()
            return web.Response(
                status=200, body=self._manifest,
                content_type='application/json',
                headers={'ETag': self._manifest_etag, 'Cache-Control': f'public, max-age: {self._http_cache_time}'})

        async def _update_peer_urls(request, op):
            """
            Add or delete hash -> URL mappings (peer URLs)
            """
            print(f"[{request.remote}] POST {request.path_qs}")
            try:
                hashes_to_urls = await request.json()
            except json.decoder.JSONDecodeError as e:
                return web.Response(status=400, body='400 BAD REQUEST: invalid JSON: ' + str(e), content_type='text/plain')
            assert(op in ('add', 'del'))
            bad_hashes = []
            changed = False
            for h, url in hashes_to_urls.items():
                if h in self._hash_to_chunk:
                    c = self._hash_to_chunk[h]
                    if op == 'add':
                        if url not in c.urls:
                            c.urls.append(url)
                            changed = True
                    else:
                        if url in c.urls:
                            c.urls.remove(url)
                            changed = True
                else:
                    bad_hashes.append(h)
            if changed:
                self._update_manifest()
            errors_str = (' - EXCEPT: Unknown hashes: ' + ', '.join(bad_hashes)) if bad_hashes else ''
            return web.Response(status=200, body='OK' + errors_str, content_type='text/plain')

        # HTTP HANDLER - Add list of hash -> peer url mappings
        async def handle_add_peers(request):
            return await _update_peer_urls(request, 'add')

        # HTTP HANDLER - Delete list of hash -> peer url mappings
        async def handle_del_peers(request):
            return await _update_peer_urls(request, 'del')


        # Create aiohttp web app
        app = web.Application()
        app.add_routes([
                web.get('/chunk/{hash}', handle_get_chunk),
                web.post('/add-peers', handle_add_peers),
                web.post('/del-peers', handle_del_peers),
        ])
        if serve_manifest:
            app.add_routes([web.get('/manifest', handle_get_manifest)])

        # Setup HTTPS if certificate and key are provided (otherwise use plain HTTP):
        context = None
        if https_cert and https_key:
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_default_certs()
            context.load_cert_chain(certfile=https_cert, keyfile=https_key)

        # Return async server task
        task = web._run_app(app, ssl_context=context, port=port)
        return task




async def async_main():
    BASE_DIR = "test/"
    UPDATE_INTERVAL = 5

    def print_progress(cur_filename, file_progress, total_progress):
        print(cur_filename, int(file_progress * 100 + 0.5), int(total_progress * 100 + 0.5))

    server = FileServer(BASE_DIR)

    async def dir_scanner():
        nonlocal BASE_DIR, server
        async for new_chunks in monitor_folder_forever(BASE_DIR, UPDATE_INTERVAL, print_progress):
            print("Folder changed, setting chunks.")
            server.set_chunks(new_chunks)

    # server.run_server(https_cert='ssl/localhost.crt', https_key='ssl/localhost.key'))
    await asyncio.gather(server.run_server(), dir_scanner())

asyncio.run(async_main())
