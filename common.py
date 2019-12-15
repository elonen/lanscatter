from datetime import datetime
import json

class defaults:
    TCP_PORT = 10565
    CHUNK_SIZE = 64 * 1024 * 1024
    BANDWIDTH_LIMIT_MBITS_PER_SEC = 10000

    FILE_BUFFER_SIZE = 256 * 1024
    DOWNLOAD_BUFFER_MAX = 256 * 1024
    NETWORK_BUFFER_MIN = 8 * 1024

    CONCURRENT_TRANSFERS_MASTER = 4
    DIR_SCAN_INTERVAL_MASTER = 60

    CONCURRENT_TRANSFERS_PEER = 2
    DIR_SCAN_INTERVAL_PEER = 60

def make_human_cli_status_func(log_level_debug = False):

    def func(progress: float = None, cur_status: str = None,
             log_error: str = None, log_info: str = None, log_debug: str = None, popup: bool = False):
        dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sep = '*' if popup else '|'
        parts = []
        if progress is not None:
            p = str(int(progress*100+0.5)).rjust(3)+'%' if progress >= 0 else '  - '
            parts.append(f"{p}")
        else:
            parts.append('    ')
        if cur_status is not None:
            parts.append(f"STATUS  {sep} {cur_status}")
        if log_error is not None:
            parts.append(f"ERROR   {sep} {log_error}")
        if log_info is not None:
            parts.append(f"INFO    {sep} {log_info}")
        if log_level_debug and log_debug is not None:
            parts.append(f"DEBUG    {sep} {log_debug}")

        if parts and (''.join(parts)).strip():
            print(f"{dt} {sep} " + ' | '.join(parts))

    return func


def json_status_func(progress: float = None, cur_status: str = None,
                     log_error: str = None, log_info: str = None, log_debug: str = None, popup: bool = False):
    print(json.dumps({
            'timestamp': datetime.utcnow().timestamp(),
            'progress': progress,
            'cur_status': cur_status,
            'log_error': log_error,
            'log_info': log_info,
            'log_debug': log_debug,
            'popup': popup
        }))
