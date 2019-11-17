from typing import List, Callable
from filechunks import FileChunk, hash_dir, monitor_folder_forever, chunks_to_json, json_to_chunks
from datetime import datetime, timedelta
import asyncio, aiohttp, aiohttp.client_exceptions, aiofiles, aiofiles.os
import os, hashlib, random, traceback
from pathlib import Path
from fileserver import FileServer
from contextlib import suppress

class FileClient(object):

    def __init__(self, basedir: str, server_url: str, status_func: Callable=None, port: int = 14435):
        self._basedir: str = basedir
        self._local_rescan_interval: float = 60
        self._next_folder_rescan = datetime.utcnow()

        self._local_chunks = []
        self._remote_chunks = []

        self._peers = {}
        self._peers_etag: str = '-'
        self._peers_expires = datetime.utcnow()
        self._peers_refresh_interval = 30

        self._port = port
        self._server_url: str = server_url
        self._manifest_etag: str = '-'
        self._manifest_expires = datetime.utcnow()
        self._manifest_refresh_interval: float = 30

        self._status_func = status_func
        self._peer_server = None

    async def _refresh_peers(self, http_session):
        if datetime.utcnow() >= self._peers_expires:
            url = f'{self._server_url}/peers'
            headers = {'If-None-Match': self._peers_etag}
            try:
                async with http_session.get(url, headers=headers) as resp:
                    if resp.status == 304:  # etag matches
                        pass
                    elif resp.status != 200:  # some error
                        self._status_func(log_error=f'HTTP error GET {resp.status} on {url}: {await resp.text()}.')
                    else:
                        try:
                            self._peers_etag = resp.headers.get('ETag') or ''
                            self._peers = await resp.json()
                            rnd_time = self._peers_refresh_interval * random.randrange(75, 125)/100.0
                            self._peers_expires = datetime.utcnow() + timedelta(seconds=rnd_time)
                            self._status_func(log_info=f'Fetched new peer list from server.')
                        except Exception as e:
                            self._status_func(log_error=f'Error parsing GET {url}: {str(e)}')
            except aiohttp.client_exceptions.ClientConnectorError as e:
                self._status_func(log_error=f'WARNING: failed refresh peers from server: {str(e)}.')

    async def _report_local_chunks_to_master(self, http_session):
        my_base_url = self._peer_server.base_url()
        # Find which local chunks server doesn't know about yet:
        new_chunks = []
        for c in self._local_chunks:
            if (c.sha1 not in self._peers) or not any([u.startswith(my_base_url) for u in self._peers[c.sha1]]):
                new_chunks.append(c)
        # Report them, if any
        if new_chunks:
            try:
                data = [['ADD', c.sha1, f'{my_base_url}/chunk/{c.sha1}'] for c in new_chunks]
                async with http_session.post(f'{self._server_url}/peers', json=data) as resp:
                    if resp.status != 200:
                        self._status_func(log_error=f'HTTP error POST {resp.status} on {url}.')
                    else:
                        # Update peer list while we are at it
                        self._peers = await resp.json()
                        self._peers_etag = resp.headers.get('ETag') or '-'
                        self._status_func(log_info=f'Posted {len(new_chunks)} new peer URLs for local chunks to master server')
            except Exception as e:
                self._status_func(log_error=f'Warning: Failed to report new chunks to server: {str(e)}')


    async def _fetch_chunk(self, c, http_session):
        if not self._peers.get(c.sha1):
            self._status_func(log_error=f'No download URLs for chunk {str(c.sha1)} of {c.filename}')
            return None

        my_base_url = self._peer_server.base_url()
        for attempt in range(5):
            url = random.choice([u for u in self._peers.get(c.sha1) if not u.startswith(my_base_url)])
            self._status_func(log_info=f'Downloading chunk {str(c.sha1)} from {url}')
            try:
                async with http_session.get(url) as resp:
                    if resp.status != 200:  # some error
                        self._status_func(log_error=f'HTTP error GET {resp.status} on {url}.')
                    else:
                        try:
                            data = await resp.read()
                            sha1 = hashlib.sha1(data).hexdigest()
                            if sha1 != c.sha1:
                                self._status_func(log_error=f'Checksum error verifying {c.sha1} from {url}!')
                            else:
                                # SUCCESS
                                return data
                        except Exception as e:
                                self._status_func(log_error=f'Error parsing GET {url}: {str(e)}')
            except aiohttp.client_exceptions.ClientConnectorError as e:
                self._status_func(log_error=f'HTTP connection failed on {url}: {str(e)}')

            # Fetch failed -> report bad peer URL
            try:
                data = [['DEL', c.sha1, url]]
                async with http_session.post(f'{self._server_url}/peers', json=data) as resp:
                    if resp.status != 200:
                        self._status_func(log_error=f'HTTP error POST {resp.status} on {url}.')
                    else:
                        # Update peer list while we are at it, so we won't retry bad URLs
                        self._peers = await resp.json()
                        self._peers_etag = resp.headers.get('ETag') or '-'
            except Exception as e:
                self._status_func(log_error=f'Warning: Failed to report bad peer: {str(e)}')

        return None



    async def sync_folder(self):

        # If no (GUI) status function is given, make a dummy one
        if not self._status_func:
            def dummy_status_func(progress: float=None, cur_status: str=None, log_error: str=None, log_info: str=None, popup: bool = False):
                pass
            self._status_func = dummy_status_func

        def hash_dir_progress_func(cur_filename, file_progress, total_progress):
            self._status_func(progress=total_progress, cur_status=f'Hashing ({cur_filename} / {int(file_progress*100+0.5)}%)')

        # Scan/hash local destination directory
        self._status_func(log_info='Scanning local folder...')
        self._local_chunks = await hash_dir(self._basedir, self._remote_chunks, progress_func=hash_dir_progress_func)
        self._status_func(progress=1.0, cur_status=f'Local files hashed.')

        # Start serving local chunks
        if self._peer_server:
            self._peer_server.set_chunks(self._local_chunks)

        if not self._remote_chunks:
            self._status_func(log_info=f'NOTE: no remote manifest - skipping sync for now.')
            return

        async with aiohttp.ClientSession() as http_session:
            await self._report_local_chunks_to_master(http_session)  # Report local chunks to server

            # Do we need to sync?
            if chunks_to_json(self._local_chunks) == chunks_to_json(self._remote_chunks):
                self._status_func(progress=-1, cur_status='Up to date.')
            else:
                await self._refresh_peers(http_session)
                self._status_func(log_info=f'Downloading new files now...', popup=True)
                base_path = Path(self._basedir)
                remote_files_by_name = {c.filename:c for c in self._remote_chunks}
                local_files_by_name = {c.filename:c for c in self._local_chunks}

                # 1) Delete dangling and changed local files
                removed = []
                for fn, lc in local_files_by_name.items():
                    local_path = base_path / Path(fn)
                    if base_path not in local_path.parents:
                        self._status_func(log_error=f'FATAL BUG: Local path pointing outside base dir?? ABORTING! "{fn}"', popup=True)
                        return
                    else:
                        rc = remote_files_by_name.get(fn)
                        abs_path = str(local_path.absolute())
                        if rc is None:
                            self._status_func(log_info=f'SYNC: File not in remote dir. Deleting: "{fn}"')
                            os.remove(abs_path)
                            removed.append(fn)
                        else:
                            if lc.sha1 != rc.sha1:
                                self._status_func(log_info=f'SYNC: File changed on remote. Deleting before resync: "{fn}"')
                                os.remove(abs_path)
                                removed.append(fn)
                            elif lc.file_mtime != rc.file_mtime:
                                self._status_func(log_info=f'SYNC: Checksum matches but fixing mtime on: "{fn}"')
                                os.utime(abs_path, (rc.file_mtime, rc.file_mtime))

                for fn in removed:
                    del(local_files_by_name[fn])

                missing_files = [c for c in remote_files_by_name.values() if c.filename not in local_files_by_name]
                download_total_bytes = sum([c.file_size for c in missing_files])
                remaining_bytes = download_total_bytes

                errors = False

                # 2) Download missing files
                for rc in missing_files:
                    fn = rc.filename
                    remaining_before_file_dl = remaining_bytes

                    local_path = base_path / Path(fn)
                    if base_path not in local_path.parents:
                        self._status_func(log_error=f'SECURITY ERROR: Remote path pointing outside base dir?? Skip! "{fn}"', popup=True)
                    else:
                        self._status_func(log_info=f'SYNC: Ready to download "{fn}"')
                        await self._refresh_peers(http_session)
                        abs_path = str(local_path.absolute())
                        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
                        async with aiofiles.open(abs_path, 'wb') as f:
                            for c in [c for c in self._remote_chunks if c.filename == fn]:
                                self._status_func(
                                    progress=1.0 - float(remaining_bytes) / download_total_bytes,
                                    cur_status=f'Downloading "{fn}" chunk {c.sha1}...')
                                data = await self._fetch_chunk(c, http_session)
                                if data:
                                    await f.write(data)
                                    remaining_bytes -= c.size
                                else:
                                    self._status_func(log_error=f'SYNC: Failed to get file: {fn}', cur_status='Sync failed.')
                                    break

                        # Set correct modification time if file is now complete
                        st = await aiofiles.os.stat(abs_path)
                        if st.st_size == rc.file_size:
                            os.utime(abs_path, (rc.file_mtime, rc.file_mtime))

                            # Remember new local chunks
                            new_chunks = [c for c in self._remote_chunks if c.filename == fn]
                            self._local_chunks.extend(new_chunks)
                            self._local_chunks = sorted(self._local_chunks, key=lambda c: c.filename + f'{c.pos:016}')
                            # Start serving them to peers
                            self._peer_server.set_chunks(self._local_chunks)
                            # Report them to master for peer-to-peer transfer
                            await self._report_local_chunks_to_master(http_session)

                        else:
                            # ...otherwise delete incomplete one
                            self._status_func(log_info=f'Deleting incompletely downloaded file: {fn}')
                            os.remove(abs_path)
                            errors = True

                    # Recalc progress (in case some chunks failed to download)
                    remaining_bytes = remaining_before_file_dl - rc.file_size
                    self._status_func(progress=1.0-float(remaining_bytes)/download_total_bytes, cur_status=f'Syncing...')

                if errors:
                    self._status_func(cur_status=f'Sync incomplete. Trying again in a bit.')
                else:
                    self._status_func(progress=-1, cur_status=f'Sync done.')


    async def run_syncer(self):
        self._next_folder_rescan = datetime.utcnow()
        # Periodically rescan local folder
        async def folder_monitor():
            self._status_func(log_info=f'Local folder scanner starting.')
            while True:
                await asyncio.sleep(1)
                if datetime.utcnow() >= self._next_folder_rescan:
                    await self.sync_folder()
                    self._next_folder_rescan = datetime.utcnow() + timedelta(seconds=self._local_rescan_interval)

        # Monitor master server for file manifest changes
        async def server_monitor():
            self._status_func(log_info=f'Manifest monitor starting.')
            async with aiohttp.ClientSession() as http_session:
                while True:
                    if datetime.utcnow() >= self._manifest_expires:
                        # Schedule next remote manifest refresh
                        rnd_time = self._manifest_refresh_interval * random.randrange(75, 125) / 100.0
                        self._manifest_expires = datetime.utcnow() + timedelta(seconds=rnd_time)

                        # Fetch it
                        url = f'{self._server_url}/manifest'
                        headers = {'If-None-Match': self._manifest_etag}
                        try:
                            async with http_session.get(url, headers=headers) as resp:
                                if resp.status == 304: # etag matches
                                    pass
                                elif resp.status != 200: # some error
                                    txt = await resp.text()
                                    self._status_func(
                                        log_error=f'HTTP error GET {resp.status} on {url}: {txt}. Trying again later.')
                                else:
                                    try:
                                        self._manifest_etag = resp.headers.get('ETag') or '-'
                                        self._remote_chunks = json_to_chunks(await resp.text())
                                        # Schedule immediate local folder scan and peer list fetch
                                        self._next_folder_rescan = datetime.utcnow()
                                        self._peers_expires = datetime.utcnow()
                                        self._status_func(log_info=f'Got new file manifest form server: {url}.')
                                    except Exception as e:
                                        self._status_func(log_error=f'Error parsing GET {url}: {str(e)}.')
                        except aiohttp.client_exceptions.ClientConnectorError as e:
                            self._status_func(log_error=f'WARNING: failed contact server: {str(e)}.')
                    await asyncio.sleep(1)

        # Serve chunks to peers
        async def peer_server():
            self._peer_server = FileServer(self._basedir, status_func=self._status_func)
            await self._peer_server.run_server(port=self._port, serve_manifest=False)

        # Start all
        try:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(folder_monitor(), server_monitor(), peer_server())
        except GeneratorExit as e:
            pass
        except:
            self._status_func(log_error='FILECLIENT error:\n'+traceback.format_exc(), popup=True)


async def run_file_client(base_dir: str, server_url: str, port: int, status_func=None):
    client = FileClient(basedir=base_dir, server_url=server_url, status_func=status_func, port=port)
    return await client.run_syncer()


async def async_main():
    BASE_DIR = "sync-target/"
    def test_status_func(progress: float = None, cur_status: str = None,
                         log_error: str = None, log_info: str = None, popup: bool = False):
        if progress is not None:
            print(f" | Progress: {int(progress*100+0.5)}%")
        if cur_status is not None:
            print(f" | Cur status: {cur_status}")
        if log_error is not None:
            print(f" | ERROR: {log_error}")
        if log_info is not None:
            print(f" | INFO: {log_info}")
    await run_file_client(BASE_DIR, 'http://localhost:14433', port=14435, status_func=test_status_func)
    print("exiting!")


if __name__== "__main__":
    asyncio.run(async_main())
