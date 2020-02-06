from datetime import datetime
from typing import Callable, Iterable, List
import json, argparse, asyncio, time
import psutil, platform, os
from contextlib import suppress

class Defaults:
    APP_NAME = 'LANScatter'
    APP_VENDOR = 'LANScatter'

    TCP_PORT_PEER = 10565
    TCP_PORT_MASTER = 10564
    BANDWIDTH_LIMIT_MBITS_PER_SEC = 10000

    CHUNK_SIZE = 128 * 1024 * 1024
    HASH_TASKS_PER_CHUNK = 8  # How many parts to split chunks when tree-hashing it

    FILE_BUFFER_SIZE = 256 * 1024
    DOWNLOAD_BUFFER_MAX = 256 * 1024
    NETWORK_BUFFER_MIN = 8 * 1024

    CONCURRENT_TRANSFERS_MASTER = 4
    DIR_SCAN_INTERVAL_MASTER = 60

    CONCURRENT_TRANSFERS_PEER = 2
    DIR_SCAN_INTERVAL_PEER = 60

    MAX_WORKERS = 8
    TIMEOUT_WHEN_NO_PROGRESS = 8
    MIN_LOG_RESOUCE_USAGE_PERIOD = 30

    SPARSE_FILE_MIN_SIZE = 128 * 1024 * 1024  # Sparse file creation on Windows entails slow shell calls

    APP_VERSION = '0.1.4'
    PROTOCOL_VERSION = '3.0.0'


def drop_process_priority():
    if "indows" in platform.system():
        psutil.Process(os.getpid()).nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
    else:
        psutil.Process(os.getpid()).nice(10)


def parse_cli_args(is_master: bool):
    desc = f"LANScatter {'master' if is_master else 'peer'} -- " \
        f"app version {Defaults.APP_VERSION}, protocol version {Defaults.PROTOCOL_VERSION}"
    parser = argparse.ArgumentParser(description=desc, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    def check_server_str(value):
        s = str(value).split(':')
        if len(s) == 1:
            return value + ':' + str(Defaults.TCP_PORT_MASTER)
        elif len(s) != 2:
            raise argparse.ArgumentTypeError("Server address must be in format <address>[:<port>]. Was: '%s'" % value)
        return value

    if not is_master:
        parser.add_argument('server', type=check_server_str,
                            help=f"Master's address[:port]. E.g. 'my-server:{Defaults.TCP_PORT_MASTER}'")
    parser.add_argument('dir', help='Sync directory')
    if not is_master:
        parser.add_argument('--dl-rate', dest='dl_limit', type=float,
                            default=Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC, help='Rate limit downloads, Mb/s')
    parser.add_argument('--ul-rate', dest='ul_limit', type=float,
                        default=Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC, help='Rate limit uploads, Mb/s')
    parser.add_argument('-c', '--concurrent-transfers', dest='ct', type=int,
                        default=Defaults.CONCURRENT_TRANSFERS_MASTER if is_master else Defaults.CONCURRENT_TRANSFERS_PEER,
                        help='Max concurrent transfers')
    default_port = Defaults.TCP_PORT_MASTER if is_master else Defaults.TCP_PORT_PEER
    parser.add_argument('-p', '--port', dest='port', type=int, default=default_port, help='TCP port to listen')
    parser.add_argument('-s', '--rescan-interval', dest='rescan_interval', type=float,
                        default=Defaults.DIR_SCAN_INTERVAL_MASTER, help='Seconds to wait between sync dir rescans')
    if is_master:
        parser.add_argument('--chunksize', dest='chunksize', type=int,
                            default=Defaults.CHUNK_SIZE, help='Chunk size for splitting files (in bytes)')
        parser.add_argument('--no-compress', dest='no_compress', action='store_true', default=False,
                            help="Disable LZ4 compression")

        '''
        parser.add_argument('--sslcert', type=str, default=None, help='SSL certificate file for HTTPS (optional)')
        parser.add_argument('--sslkey', type=str, default=None, help='SSL key file for HTTPS (optional)')
        '''

    parser.add_argument('-w', '--max-workers', dest='max_workers', type=int,
                        default=Defaults.MAX_WORKERS, help='Max thread workers to allocate.')


    parser.add_argument('--json', dest='json', action='store_true', default=False, help='Show status as JSON (for GUI usage)')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', default=False,
                        help='Show debug level log messages. No effect if --json is specified.')
    parser.add_argument('--no-nice', dest='no_nice', action='store_true', default=False, help="Don't lower CPU priority")

    parsed = parser.parse_args()

    if not parsed.no_nice:
        drop_process_priority()

    return parsed


pru_time = 0  # time.time()
def print_resource_usage(log_func):
    global pru_time
    if time.time() - pru_time > Defaults.MIN_LOG_RESOUCE_USAGE_PERIOD:
        pru_time = time.time()
        process = psutil.Process(os.getpid())
        ram_usage_mb = int(process.memory_info().rss / 1024 / 1024 + 0.5)
        file_handles = len(process.open_files())
        log_func(log_debug=f'Resource usage: {ram_usage_mb} MB RAM, {file_handles} file handles')
    pass


def make_human_cli_status_func(log_level_debug=False, print_func=None):
    print_func = print_func or (lambda *args: print(*args, flush=True))

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
            parts.append(f"DEBUG   {sep} {log_debug}")

        if parts and (''.join(parts)).strip():
            print_func(f"{dt} {sep} " + ' | '.join(parts))

        print_resource_usage(func)

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


async def process_multibuffer_io(producer: Callable, consumer: Callable, initial_buffers: Iterable,
                                 timeout=float('inf'), parallel_consumers=False):
    """
    Buffered async producer/consumer loop framework, with optional timeout.
    Timeout guard raises a TimeoutError if neither producer nor consumer do any work in 'timeout' seconds.

    By default, consumer processes buffers in linear order, which is usually fine/required for IO tasks.
    If parallel_consumers=True, it runs multiple threads and requires consumer() to be able to process
    buffers in arbitrary order. Useful for CPU bound tasks.

    :param producer: Async function that takes a buffer, writes stuff to it and returns the buffer or None if all done.
    :param consumer: Async function that takes a buffer and does something with the data (or not)
    :param initial_buffers: Arbitrary number of pre-create buffers to use for IO.
    :param timeout: Timeout after this many seconds if no progress is made.
    :param parallel_consumers: If true, consumer loop will use multithreading in arbitrary order.
    """
    end_trigger = asyncio.Event()
    last_progress_t = time.time()

    free_buffers, full_buffers = asyncio.Queue(), asyncio.Queue()
    for b in initial_buffers:
        await free_buffers.put(b)

    async def producer_loop():
        nonlocal last_progress_t
        while True:
            b = await free_buffers.get()
            b = await producer(b)
            await full_buffers.put(b)  # Signal end for consumer with None
            last_progress_t = time.time()
            if b is None:
                break

    async def consumer_task(buff):
        nonlocal last_progress_t
        await consumer(buff)
        await free_buffers.put(buff)
        last_progress_t = time.time()

    async def consumer_loop():
        tasks: List[asyncio.Task] = []
        try:
            while True:
                b = await full_buffers.get()
                tasks = [t for t in tasks if not t.done()]

                if b is None:  # None = loop termination from producer
                    await asyncio.gather(*tasks)
                    end_trigger.set()  # Let no_progress_timeout_guard() know we're done
                    break
                else:
                    if parallel_consumers:
                        tasks.append(asyncio.create_task(consumer_task(b)))
                    else:
                        await consumer_task(b)
        finally:
            for t in (t for t in tasks if not t.done()):
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def no_progress_timeout_guard():
        nonlocal end_trigger, last_progress_t
        while not end_trigger.is_set():
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(end_trigger.wait(), timeout=timeout/4)
            if time.time() - last_progress_t > timeout:
                raise TimeoutError(f"Timeout. No progress in {Defaults.TIMEOUT_WHEN_NO_PROGRESS} seconds.")

    await asyncio.gather(producer_loop(), consumer_loop(), no_progress_timeout_guard())


def file_read_producer(inf, size: int) -> Callable:
    """Turn async file handle into a process_multibuffer_io() compatible producer"""
    remaining = size

    async def read_file(buff: bytearray):
        """Producer for producer/consumer loop."""
        nonlocal remaining
        if remaining > 0:
            if remaining < len(buff):
                buff = memoryview(buff)[:remaining]
            cnt = await inf.readinto(buff)
            if cnt != len(buff):
                raise IOError(f'Filesize mismatch; "{str(inf.name if hasattr(inf, "name") else str(inf))}" '
                              f'changed? Expected {len(buff)} bytes but got {cnt}.')
            remaining -= cnt
            return buff

    return read_file
