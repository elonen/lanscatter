from aiohttp import web, WSMsgType
from pathlib import Path
from typing import List, Dict, Callable, Optional
import ssl, asyncio, aiofiles, socket, hashlib, argparse, traceback
from chunker import FileChunk, monitor_folder_forever, chunks_to_json, chunks_to_dict
from types import SimpleNamespace
from contextlib import suppress
from util.ratelimiter import RateLimiter
from common import *
import planner
from fileio import FileIO

# HTTP file server that continuously auto-scans given directory,
# serves file chunks to clients, and maintains a list of P2P URLs so network load get distributed
# like a semi-centralized bittorrent.


class FileServer(object):
    _filelist: List[FileChunk]
    _hash_to_chunk: Dict[str, FileChunk]
    _basedir: str
    _filelist: str

    class ClientInfo(object):
        def __init__(self, address: str):
            self.address = address
            self.port = None
            self.nick = None

        @property
        def url(self):
            return f'http://{self.address}:{self.port}/'


    def __init__(self, basedir: str, status_func: Callable, chunk_size: int = -1, http_cache_time: int = 60*5):
        self._filelist = []          # List fo all FileChunks
        self._hash_to_chunk = {}    # Map hash -> FileChunk
        self._filelist_etag = ''    # MD5 sum of file/chunk list
        self._basedir = basedir     # Directory to serve/monitor files from
        self._http_cache_time = http_cache_time
        self._chunk_size = chunk_size   # Only required on master server
        self._status_func = status_func

        self._planner = planner.SwarmCoordinator()

        if not Path(basedir).is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot serve from it.')

        # If no status function is given, make a dummy one
        if not self._status_func:
            def dummy_status_func(progress: float=None, cur_status: str=None, log_error: str=None, log_info: str=None, popup: bool = False):
                pass
            self._status_func = dummy_status_func


    def replace_filelist(self, new_filelist):
        '''
        Receive new chunks from directory scanner and start serving them.
        '''
        self._status_func(log_info='Chunks updated. Now serving new ones.')
        self._filelist = new_filelist
        self._hash_to_chunk = {c.hash: c for c in self._filelist}

        fl_json = chunks_to_json(self._filelist, self._chunk_size)
        new_etag = hashlib.md5(fl_json.encode('utf-8')).hexdigest()

        if self._filelist_etag != new_etag:  # filelist changed
            self._filelist_etag = new_etag
            self._planner.reset_chunks((c.hash for c in new_filelist))
            fl_msg = {'action': 'new_filelist', 'data': chunks_to_dict(self._filelist or (), self._chunk_size)}
            for n in self._planner.nodes:
                n.client.send(fl_msg)


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
        fileio = FileIO(Path(self._basedir), 0, ul_limit)

        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        self._base_url = ('https://' if (https_cert and https_key) else 'http://') + ip_addr + ':' + str(port)
        self._status_func(log_info=f'Starting {"master" if serve_filelist else "p2p"} file server on {self._base_url}.')


        async def on_websocket_msg_from_client(address, send_queue, node: Optional[planner.Node], msg) -> Optional[planner.Node]:
            '''
            :param address: IP address of originating client
            :param send_queue: Asyncio Queue for outgoing messages
            :param node: planner.Node object, or None if not yet joined a swarm
            :param msg: Message from client in a dict
            :return: New Node handle if messages caused a swarm join, otherwise None
            '''
            client_name = (node.client.nick if node else 'new_node') + "@" + str(address)
            self._status_func(log_info=f'Msg from {client_name}: {str(msg)}')

            async def error(txt):
                self._status_func(log_error=f'Sending error to client {client_name}: {txt}')
                await send_queue.put({'action': 'error', 'orig_msg': msg, 'message': txt})
            async def ok(txt):
                await send_queue.put({'action': 'ok', 'message': txt})

            try:
                action = msg.get('action')

                # ---------------------------------------------------
                # Client wants to join swarm
                # ---------------------------------------------------
                if action == 'join_swarm':
                    dl_url = msg.get('dl_url')
                    if not dl_url or 'http' not in dl_url:
                        return await error("'dl_url' argument missing or invalid.")
                    if node:  # rejoin = destroy old and create new
                        node.destroy()
                        self._status_func(log_info=f'Rejoining "{client_name}".')
                    slots = msg.get('concurrent_transfers') or 2
                    node = self._planner.node_join((), slots, slots)
                    node.client = SimpleNamespace(
                        dl_url=dl_url,
                        nick=msg.get('nick') or 'anonymous',
                        send=lambda m: send_queue.put_nowait(m))
                    self._status_func(log_info=f'Client "{client_name}" joined swarm as "{node.client.nick}".'
                                               f' URL: {node.client.dl_url}')
                    await ok('Join ok. Sending filelist.')
                    await send_queue.put({'action': 'new_filelist',
                                          'data': chunks_to_dict(self._filelist or (), self._chunk_size)})
                    return node
                else:
                    if not node:
                        return await error("Join the swarm first.")

                # ---------------------------------------------------
                # Client's got (new) chunks
                # ---------------------------------------------------
                if action == 'set_chunks' or action == 'add_chunks':
                    if not isinstance(msg.get('chunks'), list):
                        return await error("Must have list in arg 'chunks'")
                    if not node:
                        return await error("Join the swarm first.")
                    unknown_chunks = node.add_chunks(msg.get('chunks'), clear_first=(action == 'set_chunks'))
                    await ok('Chunks updated')
                    if unknown_chunks:
                        self._status_func(log_info=f'Client "{client_name}" had unknown chunks: {str(unknown_chunks)}')
                        await send_queue.put({
                            'action': 'rehash',
                            'message': 'You reported chunks not belonging to the swarm. You need to rehash files.',
                            'unknown_chunks': tuple(unknown_chunks)})

                # ---------------------------------------------------
                # Client reports current downloads, uploads and speed
                # ---------------------------------------------------
                elif action == 'report_transfers':
                    if not node:
                        return await error("Join the swarm first.")
                    dl_count, ul_count = msg.get('downloads'), msg.get('uploads')
                    incoming_chunks = msg.get('incoming_chunks')
                    last_upload_secs = msg.get('last_upload_secs')
                    if None in (dl_count, ul_count, incoming_chunks):
                        return await error(f"Missing args.")
                    node.set_active_transfers(incoming_chunks, dl_count, ul_count)
                    node.update_transfer_speed(last_upload_secs)
                    await ok('Transfer status updated')


                elif action is None:
                    return await error("Missing parameter 'action")
                else:
                    return await error(f"Unknown 'action': {str(action)}")

            except Exception as e:
                await error('Exception raised: '+str(e))
                self._status_func(log_error=f'Error while handling client ({client_name}) message "{str(msg)}": \n'+
                                            traceback.format_exc(), popup=True)


        async def http_handler__start_websocket(request):
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}. Converting to websocket.")

            # Turn request to websocket and start listening
            ws = web.WebSocketResponse(heartbeat=30)
            await ws.prepare(request)

            address = str(request.remote)
            send_queue = asyncio.Queue()
            node = None

            async def send_loop():
                while not ws.closed:
                    with suppress(asyncio.TimeoutError):
                        msg = await asyncio.wait_for(send_queue.get(), timeout=1.0)
                        if msg and not ws.closed:
                            await ws.send_json(msg)
            send_task = asyncio.create_task(send_loop())

            try:
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            new_node = await on_websocket_msg_from_client(address, send_queue, node, msg.json())
                            if new_node:
                                node = new_node
                        except Exception as e:
                            self._status_func(log_error=f'Error ("{str(e)}") handing client msg: {msg.data}')
                            send_queue.put_nowait({'command': 'error', 'orig_msg': msg.data,
                                                   'message': 'Exception: ' + str(e)})
                    elif msg.type == WSMsgType.ERROR:
                        self._status_func(log_error=f'Connection for client "{node.client.nick if node else address}" '
                                                    'closed with err: %s' % ws.exception())

                self._status_func(log_info=f'Connection closed from "{node.client.nick if node else address}"')

            finally:
                if node:
                    node.destroy()

            send_task.cancel()
            return ws


        # HTTP HANDLER - Serve requested file chunk to a client
        async def http_handler__get_chunk(request):
            '''
            HTTP GET handler that serves out a file chunk with given hash.
            '''
            self._status_func(log_info=f"[{request.remote}] GET {request.path_qs}")
            h = request.match_info.get('hash')
            chunk = self._hash_to_chunk.get(h or '-')
            if chunk is None:
                raise web.HTTPNotFound(reson=f'Chunk not on this host: {h}')
            return await fileio.upload_chunk(chunk, request)


        # Create HTTP server
        app = web.Application()
        app.add_routes([web.get('/chunk/{hash}', http_handler__get_chunk)])
        if serve_filelist:
            app.add_routes([web.get('/ws', http_handler__start_websocket)])

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

# ---------------------------------------------------------------------------------------------------

async def run_master_server(base_dir: str, port: int,
                            dir_scan_interval: float = 20, status_func=None, ul_limit: float = 10000,
                            chunk_size=64*1024*1024,
                            https_cert=None, https_key=None):
    async def dir_scanner():
        def progress_func_adapter(cur_filename, file_progress, total_progress):
            if status_func:
                status_func(progress=total_progress, cur_status=f'Hashing "{cur_filename}" ({int(file_progress * 100 + 0.5)}% done)')
        async for new_chunks in monitor_folder_forever(base_dir, dir_scan_interval, progress_func_adapter, chunk_size=chunk_size):
            server.replace_filelist(new_chunks)

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
