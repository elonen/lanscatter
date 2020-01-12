from aiohttp import web
from typing import Callable
import ssl, socket

from .fileio import FileIO
from .chunker import SyncBatch

class FileServer:

    def __init__(self, status_func: Callable, upload_finished_func=None):
        self.batch = SyncBatch(0)
        self.base_url: str = '(server not running)'
        self.hostname = socket.gethostname()
        self.upload_times = []              # List of how long each upload took (for tracking speed)
        self.active_uploads = 0
        self._status_func = status_func
        self._on_upload_finished = upload_finished_func

    def set_batch(self, new_batch: SyncBatch):
        self.batch = new_batch

    def create_http_server(self, port, fileio: FileIO, https_cert=None, https_key=None, extra_routes=()):
        """
        Create HTTP(S) server loop.
        :param port: TCP port to listen at.
        :param fileio: FileIO object for reading chunks from disk
        :param https_cert: PEM filename or None
        :param https_key: PEM filename or None
        :param extra_routes: Additional routes for aiohttp server (see web.Application.add_routes() for details)
        :return: Asyncio task for the server
        """
        ip_addr = socket.gethostbyname(socket.gethostname())
        self.base_url = ('https://' if (https_cert and https_key) else 'http://') + ip_addr + ':' + str(port)

        self._status_func(log_info=f'Starting file server on {self.base_url}.')

        async def hdl__get_chunk(request):
            """
            HTTP GET handler that serves out a file chunk with given hash.
            """
            self.active_uploads += 1
            try:
                self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
                if 'lz4' not in str(request.headers.get('Accept-Encoding')).lower():
                    self._status_func(log_debug=f"[{request.remote}] no 'lz4' in 'Accept-Encoding'")
                h = request.match_info.get('hash')

                chunk = self.batch.first_chunk_with(chunk_hash=h)
                if not chunk:
                    raise web.HTTPNotFound(reason=f'Chunk not on this host: {h}')
                try:
                    res, ul_time, comp_ratio = await fileio.upload_chunk(chunk, request)
                    if comp_ratio and comp_ratio < 1.0:
                        self._status_func(log_debug=f"Compression ratio: {float('%.3g' % comp_ratio)} (for {request.path_qs})")
                    if ul_time:
                        self.upload_times.append(ul_time)
                except Exception as e:
                    raise e
                return res
            finally:
                self.active_uploads -= 1
                await self._on_upload_finished()

        app = web.Application()
        app.add_routes([web.get('/blob/{hash}', hdl__get_chunk)])
        if extra_routes:
            app.add_routes(extra_routes)

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
