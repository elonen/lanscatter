from typing import Callable, Dict, Tuple, Set, Optional
import asyncio, aiohttp
from aiohttp import WSMsgType
from pathlib import Path
from contextlib import suppress
import async_timeout
import traceback, time, json, platform, os
from concurrent.futures import ThreadPoolExecutor

import signal
import concurrent.futures

from .chunker import SyncBatch, scan_dir
from .common import make_human_cli_status_func, json_status_func, Defaults, parse_cli_args
from .fileserver import FileServer
from .fileio import FileIO


# Client that keeps given directory synced with a master server,
# and then serves downloaded chunks to peers for network load distribution.

class PeerNode:

    def __init__(self,
                 basedir: str,                  # Sync directory path
                 status_func: Callable,         # Callback for status reporting
                 file_rescan_interval: float,   # How often to rescan sync directory (seconds)
                 dl_limit: float,               # Download limit, Mbits/s
                 ul_limit: float):              # Upload limit, Mbits/s

        self.local_rescan_interval = file_rescan_interval
        self.next_periodical_rescan = time.time()
        self.file_io = FileIO(Path(basedir), dl_limit, ul_limit)
        self.status_func = status_func

        self.server_send_queue = asyncio.Queue()
        self.full_rescan_trigger = asyncio.Event()
        self.exit_trigger = asyncio.Event()

        self.local_batch = SyncBatch()
        self.remote_batch = SyncBatch()
        self.active_downloads: Dict[str, Tuple[str, float]] = {}  # chunk_id -> (url, max_rate)
        self.joined_swarm = False

        async def __on_upload_finished():
            await self.send_transfer_report()  # Let server know how many free upload slots peer node has

        self.fileserver = FileServer(status_func=self.status_func, upload_finished_func=__on_upload_finished)


    async def send_transfer_report(self):
        """
        Tell master our transfers stats to help plan chunk distribution
        """
        await self.server_send_queue.put({
            'action': 'report_transfers',
            'dls': [{'hash': chunk, 'url': url, 'mbps_limit': max_rate} for
                    chunk, (url, max_rate) in self.active_downloads.items()],
            'ul_count': self.fileserver.active_uploads,
            'incoming': list(self.active_downloads.keys()),
            'ul_times': list(self.fileserver.upload_times)
        })
        self.fileserver.upload_times.clear()


    async def local_file_fixups(self, max_recursions=4):
        """
        Compare local and remote batch and try to get them in sync:
         - filter out local chunks that have no useful content
         - copy chunks from already downloaded files to missing ones if possible
         - delete dangling (extraneous) files
         - set modification time to the target time when file matches remote specs (to speed up rescans)
         - set modification time to current time when file contents differ (=local file is incomplete)
        """
        if not self.remote_batch:
            self.status_func(log_info=f"LOCAL: Remote batch is empty or missing; will not touch local files.")
            return

        self.local_batch.sanity_checks()
        chunk_diff = self.local_batch.chunk_diff(self.remote_batch)
        path_diff = self.local_batch.file_tree_diff(self.remote_batch)

        self.status_func(log_info=f'LOCAL: Difference stats: '
                                  f'{len(chunk_diff.there_only)} missing chunks. '
                                  f'Files: {len(path_diff.there_only)} missing / '
                                  f'{len(path_diff.with_different_attribs)} different / '
                                  f'{len(path_diff.here_only)} dangling.')

        try:
            # Create missing directories
            for p in path_diff.there_only:
                f = self.remote_batch.files[p]
                if f.is_dir:
                    await self.file_io.create_folders(p)
                    await self.file_io.change_mtime(p, f.mtime)
                    self.local_batch.add(files=[f])

            # Precreate large sparse files
            with ThreadPoolExecutor(max_workers=Defaults.MAX_WORKERS) as executor:
                files = [self.remote_batch.files[p] for p in path_diff.there_only]
                def doit(f):
                    if self.file_io.try_precreate_large_sparse_file(f.path, f.size):
                        self.status_func(log_info=f"Precreated sparse file: '{f.path}' ({int(f.size/1024/1024)} MB)")
                executor.map(doit, (f for f in files if (
                        not f.is_dir and f.size >= max(Defaults.SPARSE_FILE_MIN_SIZE, self.remote_batch.chunk_size))))

            # Check each missing chunk to see if we've already got it in another local file
            for missing in chunk_diff.there_only:
                dupe = self.local_batch.first_chunk_with(missing.hash)
                if dupe:
                    self.status_func(log_info=f'LOCAL: Copying {missing.hash} from "{dupe.path}"/{dupe.pos}'
                                              f' to "{missing.path}"/{missing.pos}')
                    await self.file_io.copy_chunk_locally(copy_from=dupe, copy_to=missing)
                    self.full_rescan_trigger.set()  # changes to file contents, need to re-hash them

            # Filter out chunks with no useful hashes
            self.local_batch.discard(chunks=chunk_diff.here_only)

            # Delete dangling files
            path_diff = self.local_batch.file_tree_diff(self.remote_batch)
            for path in path_diff.here_only:
                self.status_func(log_info=f'LOCAL: Deleting dangling file: "{path}"')
                await self.file_io.remove_file_and_paths(path)
            self.local_batch.discard(paths=path_diff.here_only)

            # Fix timestamps on complete / incomplete files
            for f in path_diff.with_different_attribs:
                here = self.local_batch.files[f.path]
                there = self.remote_batch.files[f.path]

                if here.is_dir != there.is_dir:
                    self.status_func(log_info=f'LOCAL: "{f.path}" is dir here and file there (or vice versa). Deleting.')
                    await self.file_io.remove_file_and_paths(f.path)
                    self.local_batch.discard(paths=[f.path])
                elif here.is_dir:
                    if here.mtime != there.mtime:
                        self.status_func(log_info=f'LOCAL: Fixing mtime for dir "{here.path}"')
                        await self.file_io.change_mtime(here.path, there.mtime)
                        here.mtime = there.mtime
                else:
                    assert(there.chain_hash is not None)
                    if here.chain_hash == there.chain_hash:
                        if here.size == there.size:
                            assert here.mtime != there.mtime
                            self.status_func(log_info=f'LOCAL: File complete, setting mtime: "{here.path}"')
                            await self.file_io.change_mtime(here.path, there.mtime)
                            here.mtime = there.mtime
                        else:
                            self.status_func(log_error=f'LOCAL: Hash matches but size differs. Forgetting file. Here: {str(here)}, there: {str(there)}')
                            self.local_batch.discard(paths=[f.path])

                    elif here.mtime == there.mtime:
                        self.status_func(log_info=f'LOCAL: File "{here.path}" has wrong content but was'
                                                  f' set to target time. Resetting it to "now".')
                        here.mtime = time.time()
                        await self.file_io.change_mtime(here.path, here.mtime)

            self.status_func(log_debug=f"LOCAL: Local fixups done.")
            self.local_batch.sanity_checks()

            # Are we in sync yet?
            if self.local_batch == self.remote_batch:
                self.status_func(log_info='Up to date.', cur_status='Up to date.', progress=-1)
            else:
                # If we've got all hashes, local changes and scans should get us up to date. Run multiple times if needed.
                if self.local_batch.have_all_hashes(self.remote_batch.all_hashes()):
                    self.status_func(log_info='Have all chunks but local dir not in sync yet. Rescanning.')
                    self.full_rescan_trigger.set()

        except FileNotFoundError as e:
            self.status_func(log_info=f"Some files disappeared while doing local_file_fixups. Rescanning.")
            self.status_func(log_debug=f" - associated FileNotFoundError: {str(e)}")
            self.full_rescan_trigger.set()
        except OSError as e:
            self.status_func(log_error=f"OSError while doing local fixups: ({type(e).__name__}) '{str(e)}'. Rescanning.")
            self.full_rescan_trigger.set()

    async def download_task(self, chunk_hash, url, http_session, timeout, max_rate):
        """
        Async task to download chunk with given hash from given URL and writing it to relevant files.
        """
        if self.local_batch.first_chunk_with(chunk_hash):
            self.status_func(log_info=f"Aborting download of {chunk_hash}; already got it.")
            return

        if chunk_hash in self.active_downloads:
            self.status_func(log_info=f"Aborting download of {chunk_hash}; already in active downloads.")
            return

        max_rate = max_rate or float('inf')
        target = self.remote_batch.first_chunk_with(chunk_hash)
        if not target:
            self.status_func(log_error=f'Bad download command from master, or old filelist? Chunk {chunk_hash} is unknown.')
            return

        dl_task = None
        try:
            self.active_downloads[chunk_hash] = (url, max_rate)
            await self.send_transfer_report()

            def progress(got, total):
                self.status_func(cur_status=f'Downloading...',
                                 log_info=f'From {url} (lim: {int(max_rate*8/1024/1024+0.5)} Mbps) [{int(float(got) / (total or 1) * 100 + 0.5)}%]',
                                 progress=len(self.local_batch.all_hashes()) / (len(self.remote_batch.all_hashes()) or 1))

            async with async_timeout.timeout(timeout):
                dl_task = asyncio.create_task(
                    self.file_io.download_chunk(chunk=target, url=url, http_session=http_session,
                                                file_size=self.remote_batch.files[target.path].size,
                                                max_rate=max_rate, progr_func=progress))
                await asyncio.wait([dl_task, asyncio.create_task(self.exit_trigger.wait())],
                                   return_when=asyncio.FIRST_COMPLETED)
            if not dl_task.done():
                raise asyncio.CancelledError()

            dl_task.result()  # raises exception if one happened inside the task

            self.local_batch.add(chunks=(target,))
            await self.server_send_queue.put({
                'action': 'add_hashes',
                'hashes': (chunk_hash,)})

            # Rescan when it looks like we've got everything
            if self.local_batch.have_all_hashes(self.remote_batch.all_hashes()):
                self.status_func(log_info=f'All chunks apparently complete. Rescanning to make sure.')
                self.full_rescan_trigger.set()

        except asyncio.TimeoutError as e:
            self.status_func(log_info=f'Timeout. GET {url} took over {float("%.2g" % timeout)}s.')
        except asyncio.CancelledError as e:
            self.status_func(log_debug=f'Program exiting. Download aborted from {url}.')
        except (IOError, OSError) as e:
            self.status_func(log_error=f'Failed download from {url}: {str(e)}')
        except aiohttp.client_exceptions.ClientError as e:
            self.status_func(log_error=f'Failed download from {url}, aiohttp ClientError: {str(e)}')
        except (KeyboardInterrupt, concurrent.futures.CancelledError) as e:
            pass
        except Exception as e:
            self.status_func(log_error=f'Exception in download_task: \n' + traceback.format_exc(), popup=True)
            raise e
        finally:
            if dl_task:
                dl_task.cancel()
            self.active_downloads.pop(chunk_hash)
            await self.send_transfer_report()


    async def process_server_msg(self, msg, http_session):
        """
        Ingest messages from websocket connection with master.
        """
        try:
            async def error(txt):
                self.status_func(log_error=f'Error handling server message: {txt}, orig msg="{str(msg)}"')
                await self.server_send_queue.put({'action': 'error', 'orig_msg': msg, 'message': txt})

            action = msg.get('action')

            if action == 'download':
                chunk_id, url, timeout, rate = msg.get('hash'), msg.get('url'), msg.get('timeout'), msg.get('max_rate')
                if None in (chunk_id, url, timeout, rate):
                    return await error('Bad download command from server')
                rate = min(rate, self.file_io.dl_limiter.permits_per_sec)
                asyncio.create_task(self.download_task(chunk_id, url, http_session, timeout, rate))

            elif action == 'rehash':
                self.status_func(log_info=f'Server requested rescan: "{msg.get("message")}"')
                self.full_rescan_trigger.set()

            elif action == 'initial_batch':
                self.status_func(log_info=f'Initial sync batch received.')
                new_batch = SyncBatch.from_dict(msg.get('data'))
                self.status_func(log_info=f'Chunks size is {int(new_batch.chunk_size/1024/1024+0.5)} MB '
                                          f'({new_batch.chunk_size} bytes).')
                self.remote_batch = new_batch
                # self.status_func(log_debug='Initial sync batch:' + str(self.remote_batch))
                self.full_rescan_trigger.set()

            elif action == 'new_batch':
                new_batch = SyncBatch.from_dict(msg.get('data'))
                if new_batch != self.remote_batch:
                    self.status_func(log_info=f'New sync batch received.')
                    self.remote_batch = new_batch
                    await self.local_file_fixups()
                else:
                    self.status_func(log_info=f"Got sync batch update from master, but nothing has changed. Ignoring.")

            elif action == 'error':
                self.status_func(log_error='Error from server:' + str(json.dumps(msg, indent=2)))
            elif action == 'fatal':
                self.status_func(log_error='FATAL error from server:' + str(json.dumps(msg, indent=2)), popup=True)
                self.status_func(log_info='Exiting because of fatal error.')
                self.exit_trigger.set()
            elif action == 'ok':
                self.status_func(log_debug='OK from server:' + str(json.dumps(msg)))
            else:
                return await error(f"Unknown action '{str(action)}'")

        except (KeyboardInterrupt, concurrent.futures.CancelledError) as e:
            pass
        except Exception as e:
            #await error('Exception raised: ' + str(e))
            self.status_func(log_error=f'Error while handling server message "{str(msg)}": \n' +
                                       traceback.format_exc(), popup=True)


    async def server_connection_loop(self, server_url: str):

        # Connect server
        while not self.exit_trigger.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(server_url, compress=15) as ws:
                        self.status_func(log_info=f'Master server connected.')

                        # Read send_queue and pass them to websocket
                        async def send_loop():
                            self.status_func(log_info=f'Websocket message sender starting.')
                            while not (ws.closed or self.exit_trigger.is_set()):
                                with suppress(asyncio.TimeoutError):
                                    msg = await asyncio.wait_for(self.server_send_queue.get(), timeout=1)
                                    if self.exit_trigger.is_set():
                                        await ws.close()
                                    elif msg:
                                        await ws.send_json(msg)
                            self.status_func(log_info=f'Websocket message sender terminating.')

                        asyncio.create_task(send_loop())

                        # Read messages from websocket and handle them
                        async for msg in ws:
                            try:
                                if msg.type == WSMsgType.ERROR:
                                    self.status_func(
                                        log_error=f'Connection to master closed with error: %s' % ws.exception())
                                elif msg.type == WSMsgType.TEXT:
                                    await self.process_server_msg(msg.json(), session)
                            except Exception as e:
                                self.status_func(log_error=f'Error ("{str(e)}") handling server msg: {msg.data}'
                                                            'traceback: ' + traceback.format_exc())
                                await self.server_send_queue.put({
                                    'action': 'error', 'orig_msg': msg.data, 'message': 'Exception: ' + str(e)})
                            finally:
                                if self.exit_trigger.is_set():
                                    await ws.close()
                        self.status_func(log_info=f'Connection to master closed.')

            except aiohttp.client_exceptions.WSServerHandshakeError:
                self.status_func(log_error=f'Websock handshake to {server_url} failed. Bad URL? Aborting.', popup=True)
                return

            except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ClientOSError):
                self.status_func(
                    log_error=f'Websocket connect to server {server_url} failed. Retrying in a bit...',
                    cur_status='Connecting master...')
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait([self.exit_trigger.wait()], timeout=5)
            finally:
                # Return into initial state for new connection
                self.remote_batch = SyncBatch()
                self.next_periodical_rescan = time.time()
                self.joined_swarm = False


    async def file_rescan_loop(self, concurrent_transfer_limit: int):
        self.status_func(log_info=f'File scanner loop starting.')

        def __hash_dir_progress_func(cur_filename, file_progress, total_progress):
            self.status_func(progress=total_progress,
                             cur_status=f'Hashing ({cur_filename} / at {int(file_progress*100+0.5)}%)')

        while not self.exit_trigger.is_set():
            # TODO: integrate with inotify (watchdog package) to avoid frequent rescans when up-to-date.
            #  (on any event, schedule next scan 10s in the future to avoid trashing when modification is on-going)

            if self.remote_batch.chunk_size > 0:
                # Time for a periodical rescan after sync is complete?
                full_periodical_now = time.time() >= self.next_periodical_rescan and \
                                      self.local_batch == self.remote_batch

                # Wait until server has give us a remote batch (cannot chunk files without knowing chunk size)
                if self.full_rescan_trigger.is_set() or full_periodical_now:
                    self.next_periodical_rescan = time.time() + self.local_rescan_interval
                    self.full_rescan_trigger.clear()

                    self.status_func(log_debug='Rescanning local files.')

                    try:
                        def scandir_blocking():
                            return asyncio.run(scan_dir(
                                self.file_io, max_chunk_size=self.remote_batch.chunk_size,
                                max_sub_chunk_size=self.remote_batch.sub_chunk_size,
                                old_batch=self.local_batch, progress_func=__hash_dir_progress_func, test_compress=False))
                        loop = asyncio.get_event_loop()
                        new_local_batch, errors = await loop.run_in_executor(None, scandir_blocking)
                        for i, e in enumerate(errors):
                            self.status_func(log_error=f'- Dir scan error #{i}: {e}')

                        self.status_func(log_debug='Rescan finished.')

                        new_local_batch.copy_chunk_compress_ratios_from(self.remote_batch)
                        different_from_remote = self.remote_batch != new_local_batch

                        if not self.joined_swarm or new_local_batch != self.local_batch or different_from_remote:
                            self.local_batch = new_local_batch
                            self.fileserver.batch = self.local_batch
                            await self.local_file_fixups()
                            if not self.joined_swarm:
                                self.joined_swarm = True
                                await self.server_send_queue.put({'action': 'version',
                                                                  'protocol': Defaults.PROTOCOL_VERSION,
                                                                  'app': Defaults.APP_VERSION})
                                await self.server_send_queue.put({
                                    'action': 'join_swarm',
                                    'hashes': tuple(self.local_batch.all_hashes()),
                                    'dl_url': self.fileserver.base_url + '/blob/{hash}',
                                    'nick': self.fileserver.hostname,
                                    'concurrent_transfers': concurrent_transfer_limit
                                })
                            else:
                                await self.server_send_queue.put({
                                    'action': 'set_hashes',
                                    'hashes': list(self.local_batch.all_hashes())})

                    except FileNotFoundError as e:
                        self.status_func(log_info=f'NOTE: Dir scan failed, trying again in a bit: {e}')

            with suppress(asyncio.TimeoutError):
                await asyncio.wait(
                    (self.full_rescan_trigger.wait(), self.exit_trigger.wait()),
                    timeout=4, return_when=asyncio.FIRST_COMPLETED)


    async def run(self, port: int, server_url: str, concurrent_transfer_limit: int, max_workers: int):
        """Run all async loops."""

        # Mute asyncio task exceptions on KeyboardInterrupt / thread CancelledError
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(lambda l, c: loop.default_exception_handler(c) if not self.exit_trigger.is_set() else None)
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=max_workers))

        def sig_exit(*args):
            raise KeyboardInterrupt()
        try:
            signal.signal(signal.SIGINT, sig_exit)
            signal.signal(signal.SIGTERM, sig_exit)

            # TODO: Runaway loop detection and avoidance with ratelimiter

            await self.fileserver.create_http_server(port=port, fileio=self.file_io)
            await asyncio.wait([
                self.file_rescan_loop(concurrent_transfer_limit),
                self.server_connection_loop(server_url),
                self.exit_trigger.wait()
            ], return_when=asyncio.FIRST_COMPLETED)

        except (KeyboardInterrupt, concurrent.futures.CancelledError):
            self.status_func(log_info='User exit.')
            self.exit_trigger.set()
        except Exception:
            self.status_func(log_error='PeerNode exited with errors. See log for details.', popup=True)
            self.status_func(log_error='PeerNode error in run():\n' + traceback.format_exc())

# --------------------------------------------------------------------------------------------------------


async def run_file_client(base_dir: str, server_url: str, status_func=None,
                          port: int = Defaults.TCP_PORT_PEER,
                          rescan_interval: float = Defaults.DIR_SCAN_INTERVAL_MASTER,
                          dl_limit: float = Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC,
                          ul_limit: float = Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC,
                          max_workers: int = Defaults.MAX_WORKERS,
                          concurrent_transfer_limit: int = Defaults.CONCURRENT_TRANSFERS_PEER):
    pn = PeerNode(basedir=base_dir, status_func=status_func, file_rescan_interval=rescan_interval,
                  dl_limit=dl_limit, ul_limit=ul_limit)
    await pn.run(port, server_url, concurrent_transfer_limit, max_workers)



async def async_main():
    args = parse_cli_args(is_master=False)
    status_func = json_status_func if args.json else make_human_cli_status_func(log_level_debug=args.debug)
    await run_file_client(
        base_dir=args.dir, server_url=f'ws://{args.server}/join', port=args.port,
        rescan_interval=args.rescan_interval,
        dl_limit=args.dl_limit, ul_limit=args.ul_limit, concurrent_transfer_limit=args.ct,
        max_workers=args.max_workers,
        status_func=status_func)

def main():
    with suppress(KeyboardInterrupt):
        asyncio.run(async_main())

if __name__ == "__main__":  # pragma: no cover
    main()
