from datetime import datetime
import json, argparse


class Defaults:
    APP_NAME = 'LANScatter'
    APP_VENDOR = 'LANScatter'

    TCP_PORT_PEER = 10565
    TCP_PORT_MASTER = 10564
    CHUNK_SIZE = 128 * 1024 * 1024
    BANDWIDTH_LIMIT_MBITS_PER_SEC = 10000

    FILE_BUFFER_SIZE = 256 * 1024
    DOWNLOAD_BUFFER_MAX = 256 * 1024
    NETWORK_BUFFER_MIN = 8 * 1024

    CONCURRENT_TRANSFERS_MASTER = 4
    DIR_SCAN_INTERVAL_MASTER = 60

    CONCURRENT_TRANSFERS_PEER = 2
    DIR_SCAN_INTERVAL_PEER = 60

    APP_VERSION = '0.1.1'
    PROTOCOL_VERSION = '1.0.0'


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
        parser.add_argument('--sslcert', type=str, default=None, help='SSL certificate file for HTTPS (optional)')
        parser.add_argument('--sslkey', type=str, default=None, help='SSL key file for HTTPS (optional)')

    parser.add_argument('--json', dest='json', action='store_true', default=False, help='Show status as JSON (for GUI usage)')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', default=False,
                        help='Show debug level log messages. No effect if --json is specified.')
    return parser.parse_args()


def make_human_cli_status_func(log_level_debug=False, print_func=print):

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
