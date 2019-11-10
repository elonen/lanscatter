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
        self._manifest_etag = ''    # MD5 sum of manifest
        self._http_cache_time = http_cache_time
        self._peers_map = {}
        self._peers_etag = ''


    def set_chunks(self, chunks):
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
                status=200, body=chunks_to_json(self._chunks),
                content_type='application/json',
                headers={'ETag': self._manifest_etag, 'Cache-Control': f'public, max-age: {self._http_cache_time}'})


        # HTTP HANDLER - Return a list of URLs that hashes can be downloaded from
        async def handle_get_peers(request):
            print(f"[{request.remote}] GET peers")
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
            print(f"[{request.remote}] POST {request.path_qs}")
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

            return web.Response(
                status=200, body=json.dumps(res, indent=4),
                content_type='application/json',
                headers={'ETag': self._peers_etag, 'Cache-Control': f'public, max-age: 30'})


        # Create aiohttp web app
        app = web.Application()
        app.add_routes([
                web.get('/chunk/{hash}', handle_get_chunk),
                web.get('/peers', handle_get_peers),
                web.post('/peers', update_peer_urls),
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
