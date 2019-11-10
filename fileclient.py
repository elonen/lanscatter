from typing import List
from filechunks import FileChunk, hash_dir, monitor_folder_forever, chunks_to_json, json_to_chunks
from datetime import datetime, timedelta
import asyncio, aiofiles, socket, json, hashlib


class FileClient(object):

    def __init__(self, basedir: str, server_url: str, status_func: None):
        self._local_chunks = []
        self._remote_chunks = []
        self._basedir = basedir
        self._server_url = server_url
        self._manifest_etag = '-'
        self._manifest_expires = datetime.utcnow()
        self._status_func = status_func


    async def sync_folder(self):

        # Delegate progress reports from file hasher
        def progress(cur_filename, file_progress, total_progress):
            if self._status_func:
                self._status_func(progress=total_progress, status_text=f'Hashing ({cur_filename} / {int(file_progress*100+0.5)}%)')

        self._local_chunks = await hash_dir(self._basedir, self._remote_chunks, progress_func=progress)
        if chunks_to_json(self._local_chunks) == chunks_to_json(self._remote_chunks):
            # Remote and local chunks are identical
            self._status_func(progress=None, status_text='Up to date with server.')
        else:
            # Delete old and changed files
            # Download missing files
                # Get chunks in order
                  # Download
                  # Check hash
                    # If mismatch or fail, delete peer from server, get new manifest + retry N times
                  # Update local_chunks + reset them to local server
                # Touch mtime + update local chunks
                # Report as peer to server


    def run_syncer(self, recheck_interval: 60, status_func: None):
        pass

        next_folder_rescan = datetime.utcnow()

        async def folder_monitor(status_func=None):
            nonlocal  next_folder_rescan
            while True:
                await asyncio.sleep(5)
                if datetime.utcnow() >= next_folder_rescan:
                    await sync_folder()
                    next_folder_rescan = datetime.utcnow() + timedelta(seconds=recheck_interval)

        async def manifest_monitor(status_func=None):
            nonlocal  next_folder_rescan
s            while True:
                await asyncio.sleep(5)
                if datetime.utcnow() >= self._manifest_expires:
                    async with aiohttp.ClientSession() as session:
                        url = f'{self._server_url}/manifest'
                        headers = {'If-None-Match': self._manifest_etag}
                        async with session.get(url, headers=headers) as resp:
                            if resp.status == 304: # etag matches?
                                pass
                            elif resp.status != 200: # some error
                                txt = await resp.text()
                                status_func(error=f'HTTP error GET {resp.status} on {url}: {txt}. Trying again later.')
                            else:
                                try:
                                    self._manifest_etag = resp.headers.get('ETag') or '-'
                                    self._remote_chunks = await resp.json()
                                    next_folder_rescan = datetime.utcnow()
                                except Exception as e:
                                    status_func(error=f'Error parsing GET {url}: {str(e)}.')
