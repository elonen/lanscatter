from typing import List, Callable
from filechunks import FileChunk, hash_dir, monitor_folder_forever, chunks_to_json, json_to_chunks
from datetime import datetime, timedelta
import asyncio, aiohttp, aiohttp.client_exceptions, aiofiles, aiofiles.os
import os, hashlib, random, traceback, json, io, time, argparse
from pathlib import Path
from fileserver import FileServer
from contextlib import suppress
from common import *

class CachedHttpObject(object):
    '''
    Cached container for (JSON) object from a remote HTTP(s) server.
    '''

    def __init__(self, url: str, status_func: Callable, name: str, cache_time: float = 50, cache_jitter: float = 10):
        self.data = {}
        self._data_name = name
        self._url = url
        self._cache_time = cache_time
        self._cache_jitter = min(cache_jitter, cache_time/3)
        self._status_func = status_func
        self._cache_expires = datetime.utcnow()
        self._etag = ''

    def ingest_data(self, data, etag):
        self.data = data
        self._etag = etag
        self._cache_expires = datetime.utcnow() + timedelta(
            seconds=self._cache_time + self._cache_jitter * random.uniform(-1, 1))

    async def refresh(self, http_session, force_expire: bool = False):
        '''
        Query object from URL or return a cached one if possible.

        :param http_session: aiohttp session to use for HTTP requests
        :return: True if got new data, false if cache stays
        '''
        if force_expire:
            self._cache_expires = datetime.utcnow()
        if datetime.utcnow() >= self._cache_expires:
            headers = {'If-None-Match': self._etag}
            try:
                async with http_session.get(self._url, headers=headers) as resp:
                    if resp.status == 200:  # HTTP OK
                        try:
                            self.ingest_data(data=await resp.json(), etag=(resp.headers.get('ETag') or ''))
                            self._status_func(log_info=f'Got new {self._data_name} from server ({self._url}).')
                            return True
                        except Exception as e:
                            self._status_func(log_error=f'Error parsing GET {url}: {str(e)}')
                    elif resp.status == 304:  # etag match
                        self.ingest_data(self.data, self._etag)  # Touch cache time
                    else:
                        self._status_func(log_error=f'HTTP error GET {resp.status} on {self._url}: {await resp.text()}')
            except aiohttp.client_exceptions.ClientConnectorError as e:
                self._status_func(log_error=f'WARNING: failed refresh {self._data_name} from server: {str(e)}.')
            return False



class FileClient(object):
    '''
    Process that scans sync directory, hashes it, and keeps it synced with a "manifest" (file list) master server.
    '''

    def __init__(self,
                 basedir: str,                          # Sync directory path
                 server_url: str,                       # URL of master server
                 status_func: Callable,                 # Callback for status reporting
                 port: int = 14435,                     # TCP port for listening to peers
                 manifest_cache_time: float = 45,       # How often to recheck server for file changes
                 peer_list_cache_time: float = 45,      # How often to recheck server for new peer URL list
                 file_rescan_interval: float = 60):     # How often to rescan sync directory (seconds)

        self._basedir: str = basedir
        self._local_rescan_interval = file_rescan_interval
        self._next_folder_rescan = datetime.utcnow()

        self._port = port
        self._server_url: str = server_url
        self._status_func = status_func

        self._local_chunks = []                     # Hashed file parts that exist here, locally
        self._remote_chunks = []                    # Hashed file parts that exist on server (copy of "manifest")

        self._peer_server = None                    # FileServer instance for serving chunks to peers

        # Cached HTTP data from server
        self._peers = CachedHttpObject(             # Map of hash -> URL (p2p data sources)
                url=server_url+'/peers', status_func=status_func, name='peer list', cache_time=peer_list_cache_time)
        self._manifest = CachedHttpObject(          # _remote_chunks as plain JSON table
                url=server_url+'/manifest', status_func=status_func, name='manifest', cache_time=manifest_cache_time)


    async def _report_new_local_chunks_to_server(self, http_session):
        '''
        Add local peer server's hash download URLs to master's peer list.
        '''
        my_url = self._peer_server.base_url()
        # Find which local chunks server doesn't know about yet:
        new_chunks = []
        for c in self._local_chunks:
            if (c.hash not in self._peers.data) or not any([u.startswith(my_url) for u in self._peers.data[c.hash]]):
                new_chunks.append(c)
        if new_chunks:
            try:
                data = [['ADD', c.hash, f'{my_url}/chunk/{c.hash}'] for c in new_chunks]
                async with http_session.post(f'{self._server_url}/peers', json=data) as resp:
                    if resp.status != 200:
                        self._status_func(log_error=f'HTTP error POST {resp.status} on {url}.')
                    else:
                        # Update peer list while we are at it
                        self._peers.ingest_data(data=await resp.json(), etag=(resp.headers.get('ETag') or '-'))
                        self._status_func(log_info=f'Posted {len(new_chunks)} local chunk URLs to master server')
            except Exception as e:
                self._status_func(log_error=f'Warning: Failed to report new chunks to server: {str(e)}')


    async def _fetch_chunk(self, c: FileChunk, http_session):
        if not self._peers.data.get(c.hash):
            self._status_func(log_error=f'No download URLs for chunk {str(c.hash)} of {c.filename}')
            return None
        # Try to download at most 5 times from random peers:
        my_url = self._peer_server.base_url()
        for attempt in range(5):
            url = random.choice([u for u in self._peers.data.get(c.hash) if not u.startswith(my_url)])
            self._status_func(log_info=f'Downloading chunk {str(c.hash)} from {url}')
            try:
                async with http_session.get(url) as resp:
                    if resp.status != 200:  # some error
                        self._status_func(log_error=f'HTTP error GET {resp.status} on {url}.')
                    else:
                        try:
                            data, read = io.BytesIO(), b'-'
                            h = hashlib.blake2b(digest_size=12)
                            while read:
                                read = await resp.content.read(16*1024)
                                h.update(read)
                                data.write(read)
                            if h.hexdigest() == c.hash:
                                return data.getbuffer()
                            else:
                                self._status_func(log_error=f'Checksum error verifying {c.hash} from {url}!')
                        except Exception as e:
                            self._status_func(log_error=f'Error parsing GET {url}: {str(e)}')
            except aiohttp.client_exceptions.ClientConnectorError as e:
                self._status_func(log_error=f'HTTP connection failed on {url}: {str(e)}')

            # HTTP request failed? Report bad peer URL
            try:
                async with http_session.post(f'{self._server_url}/peers', json=[['DEL', c.hash, url]]) as resp:
                    if resp.status != 200:
                        self._status_func(log_error=f'HTTP error POST {resp.status} on {url}.')
                    else:
                        self._peers.ingest_data(data=await resp.json(), etag=(resp.headers.get('ETag') or '-'))
            except Exception as e:
                self._status_func(log_error=f'Warning: Failed to report bad peer: {str(e)}')

        return None



    async def sync_folder(self):
        '''
        Scan local folder, compare it to remote_chunks from server, and sync if not identical.
        '''
        # Scan/hash local sync dir
        self._status_func(log_info='Scanning local folder...')
        def hash_dir_progress_func(cur_filename, file_progress, total_progress):
            self._status_func(progress=total_progress, cur_status=f'Hashing ({cur_filename} / {int(file_progress*100+0.5)}%)')
        self._local_chunks = await hash_dir(self._basedir, self._remote_chunks, progress_func=hash_dir_progress_func)
        self._status_func(progress=1.0, cur_status=f'Files hashed.')

        # Start serving local chunks to peers
        if self._peer_server:
            self._peer_server.set_chunks(self._local_chunks)

        if not self._remote_chunks:
            self._status_func(log_info=f'NOTE: no remote manifest - skipping sync for now.')
            return

        async with aiohttp.ClientSession() as http_session:
            await self._report_new_local_chunks_to_server(http_session)

            # Do we need to sync?
            if chunks_to_json(self._local_chunks) == chunks_to_json(self._remote_chunks):
                self._status_func(progress=-1, cur_status='Up to date.')
            else:
                await self._peers.refresh(http_session)
                self._status_func(log_info=f'Downloading new files...', popup=True)
                base_path = Path(self._basedir)
                remote_files_by_name = {c.filename: c for c in self._remote_chunks}
                local_files_by_name = {c.filename: c for c in self._local_chunks}

                def remove_dir_and_paths(p, uppermost_parent):
                    os.remove(p)
                    for d in p.parents:
                        if d == uppermost_parent:
                            break
                        with suppress(OSError):
                            d.rmdir()

                # 1) Delete dangling and changed local files
                removed = []
                for fn, lc in local_files_by_name.items():
                    local_path = base_path / Path(fn)   # path concatenation
                    if base_path not in local_path.parents:
                        self._status_func(log_error=f'SECURITY BUG: Local path pointing outside base dir?? ABORTING! "{fn}"', popup=True)
                        return
                    else:
                        rc = remote_files_by_name.get(fn)
                        if rc is None:
                            self._status_func(log_info=f'SYNC: File not in remote dir. Deleting: "{fn}"')
                            remove_dir_and_paths(local_path, base_path)
                            removed.append(fn)
                        else:
                            if lc.hash != rc.hash:  # compare last chunks
                                self._status_func(log_info=f'SYNC: File changed on remote. Deleting local: "{fn}"')
                                remove_dir_and_paths(local_path, base_path)
                                removed.append(fn)
                            elif lc.file_mtime != rc.file_mtime:
                                self._status_func(log_info=f'SYNC: Checksum matches but fixing mtime on: "{fn}"')
                                os.utime(str(local_path.absolute()), (rc.file_mtime, rc.file_mtime))
                for fn in removed:
                    del(local_files_by_name[fn])

                # 2) Download missing files
                missing_files = [c for c in remote_files_by_name.values() if c.filename not in local_files_by_name]
                download_total_bytes = sum([c.file_size for c in missing_files])
                remaining_bytes = download_total_bytes
                megabytes_per_sec = 0.0
                errors = False
                for rc in missing_files:
                    fn = rc.filename
                    remaining_before_file_dl = remaining_bytes

                    local_path = base_path / Path(fn)
                    if base_path not in local_path.parents:
                        self._status_func(log_error=f'SECURITY ERROR: Remote path pointing outside base dir?? Skip! "{fn}"', popup=True)
                    else:
                        self._status_func(log_info=f'SYNC: Ready to download "{fn}"')
                        await self._peers.refresh(http_session)
                        abs_path = str(local_path.absolute())
                        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
                        async with aiofiles.open(abs_path, 'wb') as f:
                            i = 0
                            file_chunks = [c for c in self._remote_chunks if c.filename == fn]
                            for c in file_chunks:
                                i += 1
                                start_time = time.perf_counter()
                                self._status_func(
                                    progress=1.0 - float(remaining_bytes) / download_total_bytes,
                                    cur_status=f'Downloading {round(megabytes_per_sec,1)} MB/s, "{fn}" chunk {i}/{len(file_chunks)}')
                                data = await self._fetch_chunk(c, http_session)
                                end_time = time.perf_counter()
                                megabytes_per_sec = (c.size/1024/1024) / (end_time-start_time)
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

                            self._peer_server.set_chunks(self._local_chunks)
                            await self._report_new_local_chunks_to_server(http_session)
                        else:
                            # Otherwise delete the stub
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
        '''
        Infinite task that periodically rescans local folder and resyncs from server when necessary
        '''
        self._next_folder_rescan = datetime.utcnow()
        async def resync_loop():
            self._status_func(log_info=f'Resync loop starting.')

            async with aiohttp.ClientSession() as http_session:
                while True:
                    # Get new manifest?
                    if await self._manifest.refresh(http_session):
                        self._remote_chunks = json_to_chunks(json.dumps(self._manifest.data))
                        self._next_folder_rescan = datetime.utcnow()
                        self._peers_expires = datetime.utcnow()

                    # Rescan / resync?
                    if datetime.utcnow() >= self._next_folder_rescan:
                        await self.sync_folder()
                        self._next_folder_rescan = datetime.utcnow() + timedelta(seconds=self._local_rescan_interval)

                    await asyncio.sleep(1)

        # Serve chunks to peers over HTTP
        async def peer_server():
            self._peer_server = FileServer(self._basedir, status_func=self._status_func)
            await self._peer_server.run_server(port=self._port, serve_manifest=False)

        # Run all
        try:
            with suppress(asyncio.CancelledError, GeneratorExit):
                await asyncio.gather(resync_loop(), peer_server())
        except:
            self._status_func(log_error='FILECLIENT error:\n'+traceback.format_exc(), popup=True)
        self._status_func(log_info='fileclient run_syncer() exiting.')


async def run_file_client(base_dir: str, server_url: str, port: int, status_func=None,
                          cache_interval: float = 45, rescan_interval: float = 60):
    client = FileClient(
        basedir=base_dir, server_url=server_url, status_func=status_func, port=port,
        manifest_cache_time=cache_interval, peer_list_cache_time=cache_interval, file_rescan_interval=rescan_interval)
    return await client.run_syncer()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', help='Sync directory')
    parser.add_argument('--url', default='http://localhost:14433', help='Master server URL')
    parser.add_argument('-p', '--port', dest='port', type=int, default=14435, help='HTTP(s) port for P2P transfers')
    parser.add_argument('--rescan-interval', dest='rescan_interval', type=float, default=60, help='How often rescan files')
    parser.add_argument('--cache-time', dest='cache_time', type=float, default=45, help='HTTP cache expiration time')
    parser.add_argument('--json', dest='json', action='store_true', default=False, help='Show status as JSON (for GUI usage)')
    args = parser.parse_args()
    status_func = json_status_func if args.json else human_cli_status_func
    with suppress(KeyboardInterrupt):
        asyncio.run(run_file_client(base_dir=args.dir, server_url=args.url, port=args.port,
                                    cache_interval=args.cache_time, rescan_interval=args.rescan_interval,
                                    status_func=status_func))
