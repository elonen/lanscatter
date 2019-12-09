from typing import Callable, Iterable, Optional
from chunker import FileChunk, ChunkId, json_to_chunks, chunks_to_json, dict_to_chunks, scan_dir
import asyncio, aiohttp
from aiohttp import web, WSMsgType
import traceback, time, argparse, os
from pathlib import Path
from fileserver import FileServer
from contextlib import suppress
from common import *
from fileio import FileIO

# Client that keeps given directory synced with a master server,
# and then serves downloaded chunks to peers for network load distribution.

class PeerNode:
    '''
    Process that scans sync directory, hashes it, and keeps it synced with a file list from master server.
    '''
    def __init__(self,
                 basedir: str,  # Sync directory path
                 server_url: str,  # URL of master server
                 status_func: Callable,  # Callback for status reporting
                 port: int,  # TCP port for listening to peers
                 file_rescan_interval: float,  # How often to rescan sync directory (seconds)
                 dl_limit: float,  # Download limit, Mbits/s
                 ul_limit: float):  # Upload limit, Mbits/s

        self.local_rescan_interval = file_rescan_interval
        self.earliest_periodical_rescan = time.time()

        if not Path(basedir).is_dir():
            raise NotADirectoryError(f'Path "{basedir}" is not a directory. Cannot read/write files in it.')
        self.file_io = FileIO(Path(basedir), dl_limit, ul_limit)

        self.port = port
        self.server_url: str = server_url
        self.status_func = status_func

        self.remote_filelist: Set[ChunkId] = set()                    # Hashed file parts that exist on server (copy of filelist)

        self.chunk_size = None                      # Read from server

        self.busy_chunks = set()

        self.server_send_queue = asyncio.Queue()
        self.full_rescan_trigger = asyncio.Event()

        async def __on_upload(finished: bool):
            if finished:
                await self.send_transfer_report()
        self.fileserver = FileServer(status_func=self.status_func, upload_callback=__on_upload)


    async def send_transfer_report(self):
        await self.server_send_queue.put({
            'action': 'report_transfers',
            'dls': len(self.busy_chunks),
            'uls': self.fileserver.active_uploads,
            'busy': list(self.busy_chunks),
            'ul_times': list(self.fileserver.upload_times)
        })
        self.fileserver.upload_times.clear()


    async def send_chunk_report_all(self):
        await self.server_send_queue.put({
            'action': 'set_chunks',
            'chunks': list(self.fileserver.hash_to_chunk.keys())
        })

    async def send_chunk_report_new(self, new_chunks: Iterable[ChunkId]):
        await self.server_send_queue.put({
            'action': 'add_chunks',
            'chunks': list(new_chunks)
        })


    async def do_local_file_fixups(self):
        '''
        Walk through local file list and:
         - copy chunks from already downloaded files to missing ones if possible
         - delete dangling extra files
         - set modification time to the target time when file matches remote specs (to speed up rescans)
         - set modification time to current time when file contents differ (=local file is incomplete)
        '''
        if not self.remote_filelist:
            self.status_func(log_info=f"LOCAL: Remote filelist is empty or missing; will not delete local files.")
            return

        all_local_files = set([c.filename for c in self.fileserver.filelist])
        all_remote_files = set([c.filename for c in self.remote_filelist])

        # Copy already downloaded chunks to missing files if possible
        for missing_file in (all_remote_files - all_local_files):
            for c in [c for c in self.remote_filelist if c.filename == missing_file]:
                copy_from = self.fileserver.hash_to_chunk.get(c.hash)
                if copy_from:
                    self.status_func(log_info=f'LOCAL: Reusing {c.hash} from "{copy_from.filename}" to "{c.filename}".')
                    await self.file_io.copy_chunk_locally(copy_from, c)
                    self.fileserver.add_chunks([c])


        # Delete dangling files and fix timestamps on complete / incomplete files
        for fname in all_local_files:
            if fname not in all_remote_files:
                self.status_func(log_info=f'LOCAL: Deleting dangling file: "{fname}"')
                await self.file_io.remove_file_and_paths(fname)
                self.full_rescan_trigger.set()
            else:
                local_chunks = set([c.hash for c in self.fileserver.filelist if c.filename == fname])
                remote_chunks = set([c.hash for c in self.remote_filelist if c.filename == fname])
                missing_chunks = remote_chunks - local_chunks

                local_size, local_mtime = await self.file_io.size_and_mtime(fname)
                remote_mtime = next(iter((c.file_mtime for c in self.remote_filelist if c.filename == fname)))

                if missing_chunks and local_mtime == remote_mtime:
                    self.status_func(log_info=f'LOCAL: File "{fname}" is missing chunks but mtime was'
                                              f' set to target time. Resetting it to "now".')
                    await self.file_io.change_mtime(fname, time.time())
                    self.fileserver.change_mtime(fname, remote_mtime)
                elif local_mtime != remote_mtime:
                    self.status_func(log_info=f'LOCAL: File complete, setting mtime: "{fname}"')
                    await self.file_io.change_mtime(fname, remote_mtime)
                    self.fileserver.change_mtime(fname, remote_mtime)

        if chunks_to_json(self.remote_filelist, self.chunk_size) == \
           chunks_to_json(self.fileserver.filelist, self.chunk_size):
            self.status_func(log_info='Up to date.', cur_status='Up to date.', progress=-1)

    async def download_task(self, chunk_hash, url, http_session, timeout):
        '''
        Async task to download chunk with given hash from given URL and writing it to relevant files.
        '''
        relevant_chunks = set([x for x in self.remote_filelist if x.hash == chunk_hash])
        if not relevant_chunks:
            raise IOError(f'Bad download command from master, or old filelist? Chunk {chunk_hash} is unknown.')

        already_got = relevant_chunks.intersection(self.fileserver.filelist)
        missing = relevant_chunks - already_got
        if not missing or chunk_hash in self.busy_chunks:
            return

        try:
            # No local chunks with given hash -> download it to one of them, ...
            if not already_got:
                assert(url)
                self.busy_chunks.add(chunk_hash)
                await self.send_transfer_report()
                target = next(iter(missing))
                self.status_func(log_info=f'Downloading from: {url}')
                progr = len(self.fileserver.filelist) / (len(self.remote_filelist) or 1)
                self.status_func(cur_status=f'Downloading chunks...', progress=progr)
                await asyncio.wait_for(
                    self.file_io.download_chunk(target, url, http_session),
                    timeout=timeout)
                already_got.add(target)
                missing.discard(target)
                self.fileserver.add_chunks([target])
                await self.send_chunk_report_new([chunk_hash])

            # ...and then copy existing chunk with given hash to all other files
            copy_from = next(iter(already_got))
            for copy_to in missing:
                self.status_func(log_info=f'Reusing {chunk_hash} to local {copy_to.filename}.')
                await self.file_io.copy_chunk_locally(copy_from, copy_to)
                self.fileserver.add_chunks([copy_to])

            # Rescan when it looks like we've got everything
            if len(self.fileserver.filelist) == len(self.remote_filelist):
                self.status_func(log_info=f'Probably got everything. Rescanning to make sure...')
                self.full_rescan_trigger.set()

        except asyncio.TimeoutError:
            self.status_func(log_info=f'Download from {url} took over timeout ({timeout}). Aborted.')

        except IOError as e:
            self.status_func(log_error=f'Download from {url} failed: {str(e)}')

        except Exception as e:
            self.status_func(log_error=f'Exception in download_task: \n' +
                                       traceback.format_exc(), popup=True)
            raise e

        finally:
            self.busy_chunks.discard(chunk_hash)
            await self.send_transfer_report()


    async def handle_server_msg(self, msg, http_session):
        # self.status_func(log_info=f'Message from server: {str(msg)}')

        async def error(txt):
            self.status_func(log_error=f'Error handling server message: {txt}, orig msg="{str(msg)}"')
            await self.server_send_queue.put({'action': 'error', 'orig_msg': msg, 'message': txt})

        try:
            action = msg.get('action')

            if action == 'download':
                chunk, url, timeout = msg.get('chunk'), msg.get('url'), msg.get('timeout')
                if None in (chunk, url, timeout):
                    return error('Bad download command from server')
                asyncio.create_task(self.download_task(chunk, url, http_session, timeout))

            elif action == 'rehash':
                self.status_func(log_info=f'Server requested rescan: "{msg.get("message")}"')
                self.full_rescan_trigger.set()

            elif action in ('new_filelist', 'initial_filelist'):
                data = msg.get('data')
                if not data or None in (data.get('chunks'), data.get('chunk_size')):
                    return error(f'Bad args for "{action}"')

                new_filelist, new_chunk_size = dict_to_chunks(data)
                if not self.chunk_size or chunks_to_json(self.remote_filelist, self.chunk_size) != \
                        chunks_to_json(new_filelist, new_chunk_size):
                    self.chunk_size = new_chunk_size
                    self.full_rescan_trigger.set()
                    self.remote_filelist = set(new_filelist)

            elif action == 'error':
                self.status_func(log_error='Error from server:' + str(json.dumps(msg, indent=2)))
            elif action == 'ok':
                pass
            elif action is None:
                return await error("Missing parameter 'action")
            else:
                return await error(f"Unknown action '{str(action)}'")

        except Exception as e:
            #await error('Exception raised: ' + str(e))
            self.status_func(log_error=f'Error while handling server message "{str(msg)}": \n' +
                                       traceback.format_exc(), popup=True)


    async def server_connection_loop(self):

        # Connect server
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.server_url) as ws:

                # Read send_queue and pass them to websocket
                async def send_loop():
                    while not ws.closed:
                        with suppress(asyncio.TimeoutError):
                            msg = await asyncio.wait_for(self.server_send_queue.get(), timeout=1.0)
                            if msg and not ws.closed:
                                await ws.send_json(msg)
                send_task = asyncio.create_task(send_loop())

                # Read messages from websocket and handle them
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        try:
                            await self.handle_server_msg(msg.json(), session)
                        except Exception as e:
                            self.status_func(log_error=f'Error ("{str(e)}") handling server msg: {msg.data}'
                                                        'traceback: ' + traceback.format_exc())
                            await self.server_send_queue.put({'command': 'error', 'orig_msg': msg.data,
                                                               'message': 'Exception: ' + str(e)})
                    elif msg.type == WSMsgType.ERROR:
                        self.status_func(
                            log_error=f'Connection to server closed with error: %s' % ws.exception())

                self.status_func(log_info=f'Connection to server closed.')
                send_task.cancel()


    async def file_rescan_loop(self):
        self.status_func(log_info=f'File scanner loop starting.')

        def __hash_dir_progress_func(cur_filename, file_progress, total_progress):
            self.status_func(progress=total_progress, cur_status=f'Hashing ({cur_filename} / {int(file_progress*100+0.5)}%)')

        local_chunks = None

        # Wait until server has told us chunk size (cannot chunk files without it)
        while not self.chunk_size:
            await asyncio.sleep(1)

        while True:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.full_rescan_trigger.wait(), timeout=2)

            # TODO: process a per-file queue to scan files when fully downloaded

            # Time for a periodical rescan after sync is complete?
            full_periodical_now = len(self.fileserver.filelist) == len(self.remote_filelist) and \
                                  time.time() >= self.earliest_periodical_rescan

            if self.full_rescan_trigger.is_set() or full_periodical_now:
                self.earliest_periodical_rescan = time.time() + self.local_rescan_interval
                self.full_rescan_trigger.clear()

                self.status_func(log_info='Rescanning local files.')
                new_local_chunks = await scan_dir(
                    str(self.file_io.basedir), chunk_size=self.chunk_size,
                    old_chunks=local_chunks or (), progress_func=__hash_dir_progress_func)
                self.status_func(log_info='Rescan finished.')

                different_from_remote = chunks_to_json(self.remote_filelist, self.chunk_size) != \
                                        chunks_to_json(new_local_chunks, self.chunk_size)

                if new_local_chunks is not local_chunks or different_from_remote:
                    self.fileserver.clear_chunks()
                    self.fileserver.add_chunks(new_local_chunks)
                    await self.do_local_file_fixups()
                    if local_chunks is None:
                        await self.server_send_queue.put({
                            'action': 'join_swarm',
                            'chunks': [c.hash for c in new_local_chunks],
                            'dl_url': self.fileserver.base_url + '/chunk/{chunk}',
                            'nick': self.fileserver.hostname,
                            'concurrent_transfers': 2  # TODO: make this a command line arg
                        })
                    else:
                        await self.send_chunk_report_all()
                    local_chunks = new_local_chunks

    async def run(self):
        '''
        Run all async loop until one of them exits.
        '''
        try:
            with suppress(asyncio.CancelledError, GeneratorExit):
                await asyncio.gather(
                    self.fileserver.create_http_server(port=self.port, fileio=self.file_io),
                    self.file_rescan_loop(),
                    self.server_connection_loop())
        except Exception as e:
            self.status_func(log_error='PeerNode error:\n' + traceback.format_exc(), popup=True)
        self.status_func(log_info='PeerNode run_forever() exiting.')

# --------------------------------------------------------------------------------------------------------

async def run_file_client(base_dir: str, server_url: str, port: int, status_func=None,
                          rescan_interval: float = 60, dl_limit: float = 10000, ul_limit: float = 10000):
    await PeerNode(basedir=base_dir, server_url=server_url, status_func=status_func, port=port,
             file_rescan_interval=rescan_interval, dl_limit=dl_limit, ul_limit=ul_limit).run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', help='Sync directory')
    parser.add_argument('--url', default='ws://localhost:14433/ws', help='Master server URL')
    parser.add_argument('-p', '--port', dest='port', type=int, default=14435, help='HTTP(s) port for P2P transfers')
    parser.add_argument('--rescan-interval', dest='rescan_interval', type=float, default=60, help='How often to rescan files')
    parser.add_argument('--dl-rate', dest='dl_limit', type=float, default=10000, help='Rate limit downloads, Mb/s')
    parser.add_argument('--ul-rate', dest='ul_limit', type=float, default=10000, help='Rate limit uploads, Mb/s')
    parser.add_argument('--cache-time', dest='cache_time', type=float, default=45, help='HTTP cache expiration time')
    parser.add_argument('--json', dest='json', action='store_true', default=False, help='Show status as JSON (for GUI usage)')
    args = parser.parse_args()
    status_func = json_status_func if args.json else human_cli_status_func
    with suppress(KeyboardInterrupt):
        asyncio.run(run_file_client(base_dir=args.dir, server_url=args.url, port=args.port,
                                    rescan_interval=args.rescan_interval,
                                    dl_limit=args.dl_limit, ul_limit=args.ul_limit,
                                    status_func=status_func))


if __name__ == "__main__":
    main()
