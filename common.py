from datetime import datetime
import json

def make_human_cli_status_func(log_level_debug = False):

    def func(progress: float = None, cur_status: str = None,
             log_error: str = None, log_info: str = None, log_debug: str = None, popup: bool = False):
        dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sep = '*' if popup else '|'
        if progress is not None:
            p = str(int(progress*100+0.5))+'%' if progress >= 0 else '-'
            print(f"{dt} PROGRESS{sep} {p}")
        if cur_status is not None:
            print(f"{dt} STATUS  {sep} Cur status: {cur_status}")
        if log_error is not None:
            print(f"{dt} ERROR   {sep} {log_error}")
        if log_info is not None:
            print(f"{dt} INFO    {sep} {log_info}")
        if log_level_debug and log_debug is not None:
            print(f"{dt} DEBUG    {sep} {log_debug}")

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
