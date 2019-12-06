from aiohttp import web, WSMsgType
from pathlib import Path
from typing import Callable, Optional
import asyncio, aiofiles, argparse, traceback
from chunker import monitor_folder_forever, chunks_to_dict, chunks_to_json
from types import SimpleNamespace
from contextlib import suppress
from common import *

import planner
from fileio import FileIO
from fileserver import FileServer

# HTTP file server that continuously auto-scans given directory,
# serves file chunks to clients, and maintains a list of P2P URLs so network load get distributed
# like a semi-centralized bittorrent.


class MasterNode:

    def __init__(self, status_func: Callable, chunk_size: int):
        self._chunk_size = chunk_size
        self._planner = planner.SwarmCoordinator()
        self._fileserver = FileServer(status_func)
        self._status_func = status_func

    def __make_filelist_msg(self):
        return {'action': 'new_filelist', 'data': chunks_to_dict(self._fileserver.filelist or (), self._chunk_size)}

    async def replace_filelist(self, new_filelist):
        old = chunks_to_json(self._fileserver.filelist, self._chunk_size)
        new = chunks_to_json(new_filelist, self._chunk_size)
        if old != new:
            self._status_func(log_info='Filelist changed. Updating planner and notifying clients.')
            self._fileserver.clear_chunks()
            self._fileserver.add_chunks(new_filelist)

            # Update planner and send new list to clients
            self._planner.reset_chunks((c.hash for c in self._fileserver.filelist))
            fl_msg = self.__make_filelist_msg()
            for n in self._planner.nodes:
                await n.client.send_queue.put(fl_msg)
        else:
            self._status_func(log_info='New filelist identical to old one. No action.')


    def run_master_server(self, base_dir: str, port=14433, https_cert=None, https_key=None, ul_limit: float = 10000):

        if not Path(base_dir).is_dir():
            raise NotADirectoryError(f'Path "{base_dir}" is not a directory. Cannot serve from it.')

        async def __on_websocket_msg_from_client(
                address: str, send_queue: asyncio.Queue,
                node: Optional[planner.Node], msg) -> Optional[planner.Node]:
            '''
            Handle received messages from clients.

            :param address: IP address of originating client
            :param send_queue: Asyncio Queue for outgoing messages
            :param node: planner.Node object, or None if not yet joined a swarm
            :param msg: Message from client in a dict
            :return: New Node handle if messages caused a swarm join, otherwise None
            '''
            client_name = (node.client.nick if node else self._fileserver.hostname) + "@" + str(address)
            self._status_func(log_info=f'Msg from {client_name}: {str(msg)}')

            async def error(txt):
                self._status_func(log_error=f'Sending error to client {client_name}: {txt}')
                await send_queue.put({'action': 'error', 'orig_msg': msg, 'message': txt})

            async def ok(txt):
                await send_queue.put({'action': 'ok', 'message': txt})

            try:
                action = msg.get('action')

                # ---------------------------------------------------
                # Client is ready to sync
                # ---------------------------------------------------
                if action == 'join_swarm':
                    dl_slots = msg.get('concurrent_transfers') or 2

                    initial_chunks = msg.get('chunks')
                    if not isinstance(initial_chunks, list):
                        return await error("'chunks' argument missing or invalid.")

                    dl_url = msg.get('dl_url')
                    if not dl_url or 'http' not in dl_url:
                        return await error("'dl_url' argument missing or invalid.")

                    if node:  # rejoin = destroy old and create new
                        node.destroy()
                        self._status_func(log_info=f'Rejoining "{client_name}".')

                    node = self._planner.node_join(initial_chunks, dl_slots, dl_slots)
                    node.client = SimpleNamespace(
                        dl_url=dl_url,
                        nick=msg.get('nick') or self._fileserver.hostname,
                        send_queue=send_queue)

                    self._status_func(log_info=f'Client "{client_name}" joined swarm as "{node.client.nick}".'
                                               f' URL: {node.client.dl_url}')
                    await ok('Joined swarm.')
                    await send_queue.put(self.__make_filelist_msg())
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
            ws = web.WebSocketResponse(heartbeat=30)
            await ws.prepare(request)

            address = str(request.remote)
            send_queue = asyncio.Queue()
            node = None

            # Infinite async loop reading from send_queue and writing to websocket
            async def send_loop():
                while not ws.closed:
                    with suppress(asyncio.TimeoutError):
                        msg = await asyncio.wait_for(send_queue.get(), timeout=1.0)
                        if msg and not ws.closed:
                            await ws.send_json(msg)
            send_task = asyncio.create_task(send_loop())

            welcome = self.__make_filelist_msg()
            welcome['action'] = 'initial_filelist'
            welcome['message'] = 'Welcome. Hash your files against this and join_swarm when ready to sync.'
            await send_queue.put(welcome)

            try:
                # Infinite loop reading messages from client and handling them
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            new_node = await __on_websocket_msg_from_client(address, send_queue, node, msg.json())
                            if new_node:
                                node = new_node
                        except Exception as e:
                            self._status_func(log_error=f'Error ("{str(e)}") handling client msg: {msg.data}'
                                              'traceback: ' + traceback.format_exc())
                            await send_queue.put({'command': 'error', 'orig_msg': msg.data,
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

        # Start serving chunks over HTTP and accepting client connections on websocket endpoint
        file_io = FileIO(Path(base_dir), 0, ul_limit)
        return self._fileserver.create_http_server(
            port, file_io, https_cert, https_key,
            extra_routes=[web.get('/ws', http_handler__start_websocket)])


# ---------------------------------------------------------------------------------------------------

async def run_master_server(base_dir: str, port: int,
                            dir_scan_interval: float = 20, status_func=None, ul_limit: float = 10000,
                            chunk_size=64*1024*1024,
                            https_cert=None, https_key=None):

    server = MasterNode(status_func=status_func, chunk_size=chunk_size)

    async def dir_scanner():
        def progress_func_adapter(cur_filename, file_progress, total_progress):
            status_func(progress=total_progress,
                        cur_status=f'Hashing "{cur_filename}" ({int(file_progress * 100 + 0.5)}% done)')
        async for new_chunks in monitor_folder_forever(
                base_dir, dir_scan_interval,
                progress_func_adapter, chunk_size=chunk_size):
            await server.replace_filelist(new_chunks)

    await asyncio.gather(
        dir_scanner(),
        server.run_master_server(base_dir=base_dir, port=port, ul_limit=ul_limit, https_cert=https_cert, https_key=https_key))


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
