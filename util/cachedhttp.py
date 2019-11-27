from typing import Callable
import random, aiohttp, time

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
        self._cache_expires = time.time()-1
        self._etag = ''

    def ingest_data(self, data, etag):
        self.data = data
        self._etag = etag
        self._cache_expires = time.time() + (self._cache_time + self._cache_jitter * random.uniform(-1, 1))

    async def refresh(self, http_session, force_expire: bool = False):
        '''
        Query object from URL or return a cached one if possible.

        :param http_session: aiohttp session to use for HTTP requests
        :param force_expire: If true, disregard expiration date and make a request in any case (still uses etag though)
        :return: True if got new data, false if cache stays
        '''
        if force_expire:
            self._cache_expires = time.time()-1
        if time.time() >= self._cache_expires:
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
