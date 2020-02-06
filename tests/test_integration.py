from pytest_cov.embed import cleanup_on_sigterm
cleanup_on_sigterm()

from contextlib import suppress
import multiprocessing as mp
import pytest, shutil, time, os, io, random, traceback, sys, threading, filecmp, json, itertools, signal, platform
from pathlib import Path
from types import SimpleNamespace
import asyncio, aiohttp

from lanscatter import common, masternode, peernode


"""
Integration tests. Creates some empty and non-empty directories, runs a master and several peer nodes in
separate processes using command line arguments. Checks that sync target dirs become identical to
source dir, and without errors in the logs.

Several edge cases are tested, too, such as empty files, different sized files and overwriting some files in
the middle of sync.
"""

random.seed()

TEST_DIR = './temp_test_dir'
TEST_FILES_PER_DIR = 3
CHUNK_SIZE = 50000
PORT_BASE = 53000 + int(2000*random.random())
TEST_PEER_NAMES = {0: 'peer_empty', 1: 'peer_corrupt', 2: 'peer_non_empty', 3: 'peer_crashed'}

# Make sure buffers don't cover whole chunks, for realistic testing
common.Defaults.FILE_BUFFER_SIZE = int(CHUNK_SIZE * 0.7)
common.Defaults.DOWNLOAD_BUFFER_MAX = int(CHUNK_SIZE * 0.3)
common.Defaults.NETWORK_BUFFER_MIN = int(CHUNK_SIZE * 0.1)


@pytest.fixture()
def test_dir_factory(tmp_path):

    def create_file(p: Path, total_size: int, pattern=None):
        pattern = bytearray(random.getrandbits(8) for _ in range(50)) if pattern is None else pattern
        sz = 0
        with p.open('wb') as f:
            while sz < total_size:
                f.write(pattern)
                sz += len(pattern)
            f.truncate(total_size)
        mtime = float(time.time() * random.random())
        os.utime(str(p), (mtime, mtime))

    rnd_postfix = 0
    def rnd_name(prefix: str, postfix: str = ''):
        nonlocal rnd_postfix
        rnd_postfix += 1
        return prefix + '%05d' % int(random.random()*1000) + '_%d'%rnd_postfix + postfix

    def setup_test_content(base: Path, sub_dir: str, keep_empty=False):
        base = base / sub_dir
        base.mkdir(parents=True, exist_ok=True)
        if not keep_empty:

            # Create some empty dirs
            (base / rnd_name('empty_dir')).mkdir(parents=True, exist_ok=True)
            p = base / rnd_name('empty_dir') / 'a'
            (p / 'b' / 'c').mkdir(parents=True, exist_ok=True)
            create_file(p / rnd_name('file', '.bin'), CHUNK_SIZE)

            # Create some dirs with content
            for d in ('.', 'dir1', rnd_name('dir_'), 'dir2/dir2_nested'):
                p = base / d
                p.mkdir(parents=True, exist_ok=True)
                create_file(p / 'empty', 0)
                create_file(p / 'another_empty', 0)
                create_file(p / '1chunk.bin', CHUNK_SIZE)
                create_file(p / '1chunk_plus.bin', CHUNK_SIZE+2)
                create_file(p / '3chunks.bin', CHUNK_SIZE * 3)
                create_file(p / 'fbuf_size.bin', common.Defaults.FILE_BUFFER_SIZE)
                create_file(p / 'fbuf_size_almost.bin', common.Defaults.FILE_BUFFER_SIZE - 1)
                create_file(p / 'fbuf_size_plus.bin', common.Defaults.FILE_BUFFER_SIZE + 1)
                create_file(p / 'dlbuf_size.bin', common.Defaults.DOWNLOAD_BUFFER_MAX)
                create_file(p / 'dlbuf_size_almost.bin', common.Defaults.DOWNLOAD_BUFFER_MAX - 1)
                create_file(p / 'dlbuf_size_plus.bin', common.Defaults.DOWNLOAD_BUFFER_MAX + 2)
                create_file(p / 'many_chunks.bin', int(CHUNK_SIZE * 5.5))
                create_file(p / 'to_be_corrupted.bin', int(CHUNK_SIZE * 5.5))
                create_file(p / 'zeroes.bin', int(CHUNK_SIZE * 3.1), pattern=b'\0' * CHUNK_SIZE)
                create_file(p / 'zeroes_to_corrupt.bin', int(CHUNK_SIZE * 3.1), pattern=b'\0' * CHUNK_SIZE)
                create_file(p / 'less_zeroes.bin', int(CHUNK_SIZE * 1.1), pattern=b'\0' * CHUNK_SIZE)
                for x in range(5):
                    create_file(p / rnd_name('rnd_file', '.bin'), int(random.random() * CHUNK_SIZE * 7))
        return str(base.resolve())

    def _factory(node_name: str, keep_empty=False):
        print(f"Creating {'empty ' if keep_empty else ''}test dir for '{node_name}'")
        return setup_test_content(tmp_path, node_name, keep_empty=keep_empty)

    yield _factory


# To be run in separate process:
# run syncer and send stdout + exceptions through a pipe.
def _sync_proc(conn, is_master, argv, use_gui):
    try:
        class PipeOut(io.RawIOBase):
            def write(self, b):
                conn.send(b)
        out = PipeOut()
        sys.stdout, sys.stderr = out, out
        sys.argv = argv
        if use_gui:
            print("Spawning gui...")
            from lanscatter import gui
            gui.main()
        elif is_master:
            masternode.main()
        else:
            peernode.main()
        conn.send((None, None))
    except Exception as e:
        conn.send((e, traceback.format_exc()))


def _spawn_sync_process(name: str, is_master: bool, sync_dir: str, peer_port: int, master_port: int, extra_opts=(), gui=False):
    print(f"Spawning process for {'master' if is_master else 'peer'} node '{name}' at port {master_port if is_master else peer_port}, gui={gui}...")
    out = io.StringIO()
    def comm_thread(conn):
        with suppress(EOFError):
            while conn:
                o = conn.recv()
                if isinstance(o, tuple):
                    pass  # process exit
                else:
                    out.write(str(o))
    conn_recv, conn_send = mp.Pipe(duplex=False)

    if gui:
        cfg_file = f'{sync_dir}/gui.conf'
        print(f"Writing gui config file at '{cfg_file}'...")
        with open(cfg_file, 'wb') as outf:
            conf = {'sync_dir': sync_dir, 'local_master_port': str(master_port), 'local_peer_port': str(peer_port),
                    'remote_address': 'localhost', 'remote_port': str(master_port), 'is_master': is_master, 'autostart': True}
            outf.write(json.dumps(conf).encode('utf-8'))
        argv = ['GUI_MASTER' if is_master else 'GUI_PEER', '-c', cfg_file, '--test-mode']
    else:
        argv = ['MASTER', sync_dir, '-d', '--port', str(master_port), '-c', '1', *extra_opts] if is_master else \
               ['PEER', f'localhost:{master_port}', sync_dir, '-d', '--port', str(peer_port), '--rescan-interval', '3', *extra_opts]

    proc = mp.Process(target=_sync_proc, name='sync-worker_'+name, args=(conn_send, is_master, argv, gui))
    threading.Thread(target=comm_thread, args=(conn_recv,)).start()
    proc.start()
    return SimpleNamespace(proc=proc, is_master=is_master, out=out, name=name, dir=sync_dir)

def _wait_seconds(s):
    for x in range(s):
        print(f"Waiting {s-x} seconds...")
        time.sleep(1)

def _terminate_procs(*args):

    if any(platform.win32_ver()):
        # Test coverage stats don't get updated if Process is .terminate()d, so try extra hard to exit nicely.
        # Windows propagates Ctrl-C to subprocesses up to parent. This is a hack to prevent it from killing the tests.
        with suppress(KeyboardInterrupt):
            for x in args:
                if x.proc.is_alive():
                    print(f"Sending ctrl-c to '{x.name}'...")
                    os.kill(x.proc.pid, signal.CTRL_C_EVENT)
            time.sleep(2)  # Sleep until the parent receives the KeyboardInterrupt, then ignore it
    else:
        for x in args:
            if x.proc.is_alive():
                os.kill(x.proc.pid, signal.SIGINT)

    print("Waiting for clean exit...")
    time.sleep(2)

    if any(x.proc.is_alive() for x in args):
        print("Waiting some more...")
        time.sleep(3)
    if any(x.proc.is_alive() for x in args):
        for x in args:
            if x.proc.is_alive():
                print("Process '{x.name}' still not finished. Terminating by force.")
                x.proc.terminate()

def _assert_node_basics(p, check_sync_results=True):
    assert 'Exception' not in p.out.getvalue(), f'Exception(s) on {p.name}'
    assert 'egmentation fault' not in p.out.getvalue()
    if check_sync_results:
        assert p.is_master or 'Up to date' in p.out.getvalue(), 'Node never reached up-to-date state.'
    assert 'traceback' not in str(p.out.getvalue()).lower()

def _print_process_stdout(p):
    print(f'\n>>>>>>>>>>>>>>>>>>>> stdout for {p.name} >>>>>>>>>>>>>>>>>>>>\n')
    print(p.out.getvalue())
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")


def _assert_identical_dir(p1, p2):
    cmp = filecmp.dircmp(p1.dir, p2.dir)
    assert not cmp.diff_files, f'Differing files between {p1.name} and {p2.name}: {str(cmp.diff_files)}'
    assert not cmp.funny_files, f'"Funny" files between {p1.name} and {p2.name}: {str(cmp.funny_files)}'
    assert not cmp.left_only, f'Files found only from {p1.name}: {str(cmp.left_only)}'
    assert not cmp.right_only, f'Files not found from {p1.name}: {str(cmp.right_only)}'


def _lines_with_match(text, match_str):
    return tuple(li for li in zip(itertools.count(0, 1), text.split('\n')) if match_str in li[1])


def _line_idx_with(text, match_str, idx):
    lines = _lines_with_match(text, match_str)
    try:
        return lines[idx][0] if lines else None
    except IndexError:
        return None


@pytest.mark.timeout(5)
def test_peer_bad_websock(test_dir_factory):
    peer_dir = test_dir_factory('leecher', keep_empty=True)
    peer = _spawn_sync_process('leecher', False, peer_dir, PORT_BASE+20, PORT_BASE+20)  # connect to itself
    peer.proc.join()
    assert "Websock handshake" in peer.out.getvalue(), "Handshake error not in found log."
    _print_process_stdout(peer)
    _assert_node_basics(peer, check_sync_results=False)


@pytest.mark.timeout(5)
@pytest.mark.parametrize("is_master", [True, False])
def test_bad_dir(is_master):
    name = 'master' if is_master else 'leecher'
    p = _spawn_sync_process(name, is_master, 'nonexisting_12345', PORT_BASE+20, PORT_BASE+20)
    p.proc.join()
    assert "not a directory" in p.out.getvalue(), "No directory error printed."
    _print_process_stdout(p)
    _assert_node_basics(p, check_sync_results=False)


def test_minimal_downloads(test_dir_factory):
    """Test that each chunk is downloaded only once."""
    master = _spawn_sync_process(f'master', True, test_dir_factory('master'), 0, PORT_BASE, ['--no-compress', '--rescan-interval', '2'])
    peer = _spawn_sync_process(f'leecher', False, test_dir_factory('leecher', keep_empty=True), PORT_BASE+1, PORT_BASE,)

    _wait_seconds(8)
    _terminate_procs(master, peer)
    master_log = master.out.getvalue()

    # Count hash downloads from master's log
    master_lines = str(master_log).split('\n')
    dl_lines = [l for l in master_lines if 'GET /blob/' in l]
    assert len(dl_lines) > 0, "No download log messages on master"
    downloaded_hashes = [l.split('/')[-1] for l in dl_lines]
    for h in downloaded_hashes:
        assert downloaded_hashes.count(h) == 1, f"Hash {h} was downloaded more than once."

    assert 'Compression ratio: 0.' not in master_log, 'Got compression stats even with --no-compress'

    master_log = master.out.getvalue()
    assert 'Hashing' in master_log, "Master didn't hash files."
    assert 'New file batch' in master_log, "Master didn't report new file batch."
    assert _line_idx_with(master_log, 'New file batch', 0) >_line_idx_with(master_log, 'Hashing', -1), \
        'Master hashed files multiple times'

    # Also check for basic conditions
    for p in (master, peer):
        _print_process_stdout(p)
        _assert_node_basics(p)
        _assert_identical_dir(p, master)
        print(f'Peer {p.name} test ok')


@pytest.mark.gui
@pytest.mark.slow
def test_gui(test_dir_factory):
    """Test that each chunk is downloaded only once."""
    import wx, wx.adv  # Test import

    master_dir = test_dir_factory('master')
    peer_dir = test_dir_factory('leecher', keep_empty=True)
    master = _spawn_sync_process(f'master', True, master_dir, 0, PORT_BASE+100, gui=True)
    peer = _spawn_sync_process(f'leecher', False, peer_dir, PORT_BASE+101, PORT_BASE+100, gui=True)

    _wait_seconds(14)
    for x in (master, peer):
      x.proc.terminate()

    _print_process_stdout(master)
    _print_process_stdout(peer)
    _assert_identical_dir(peer, master)


@pytest.mark.slow
def test_swarm__corruption__bad_protocol__uptodate__errors(test_dir_factory):
    """
    Integration test. Creates a small local swarm with peers having
    different initial contents, runs it for a bit and checks that all synced up ok.
    """
    master_dir = test_dir_factory('master')
    peer_dirs = [test_dir_factory(TEST_PEER_NAMES[i], keep_empty=(i == 0)) for i in TEST_PEER_NAMES.keys()]

    master_port = PORT_BASE+10

    master = None
    peers = []
    for i, name in TEST_PEER_NAMES.items():
        peers.append(_spawn_sync_process(f'{name}', False, peer_dirs[i], master_port+1+i, master_port))
        time.sleep(0.1)  # stagger peer generation a bit
        if i == 1:
            # Start server after the first peer to test start order
            master = _spawn_sync_process(f'master', True, master_dir, 0, master_port, ['--rescan-interval', '3'])

    # Kill one peer
    _wait_seconds(2)
    print(f"Killing peer '{peers[3].name}' to emulate crashed client...")
    os.kill(peers[3].proc.pid, (signal.SIGKILL if hasattr(signal, 'SIGKILL') else signal.SIGTERM))
    del peers[3]  # forget it -- don't perform tests etc

    # Alter files on one peer in the middle of a sync
    _wait_seconds(2)
    print(f"Corrupting some files on '{peers[1].name}'...")
    try:
        shutil.rmtree(peers[1].dir + '/dir2')                       # Delete a dir
        with open(peers[1].dir+'/to_be_corrupted.bin', 'wb') as f:      # Rewrite a file
            f.write(b'dummycontent')
        with open(peers[1].dir+'/zeroes_to_corrupt.bin', 'r+b') as f:          # Overwrite beginning
            f.write(b'dummycontent')
    except PermissionError as e:
        print("WARNING: File corruption during test failed because of file locking or something: " + str(e))

    # Alter files on master in the middle of a sync
    print("Creating extra file on master...")
    with open(master.dir + '/master_extra_file.bin', 'wb') as f:  # Write an extra file
        f.write(b'abcdefgh')
        f.flush()

    # Make some non-protocol requests to the server
    async def request_tests():

        async def read_respose_msgs(ws):
            recvd = []
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    assert 'traceback' not in msg.data.lower()
                    recvd.append(json.loads(msg.data))
            return recvd

        async with aiohttp.ClientSession() as session:
            print("Request: try bad message action")
            async with session.ws_connect(f'ws://localhost:{master_port}/join') as ws:
                await ws.send_json({'action': 'BAD_COMMAND'})
                recvd = await read_respose_msgs(ws)
                assert not recvd or ('fatal' in recvd[-1]['action'])

            print("Request: try bad message without 'action'")
            async with session.ws_connect(f'ws://localhost:{master_port}/join') as ws:
                await ws.send_json({'NOT_AN_ACTION': 123})
                recvd = await read_respose_msgs(ws)
                assert not recvd or ('fatal' in recvd[-1]['action'])

            print("Request: try bad protocol version")
            async with session.ws_connect(f'ws://localhost:{master_port}/join') as ws:
                await ws.send_json({'action': 'version', 'protocol': '0.0.0', 'app': '0.0.0'})
                recvd = await read_respose_msgs(ws)
                assert not recvd or ('fatal' in recvd[-1]['action'])

            print("Request: try bad json")
            async with session.ws_connect(f'ws://localhost:{master_port}/join') as ws:
                await ws.send_str('INVALID_JSON')
                recvd = await read_respose_msgs(ws)
                assert not recvd or ('fatal' in recvd[-1]['action'])

            print("Request: HTTP on websocet endpoint")
            async with session.get(f'http://localhost:{master_port}/join') as resp:
                assert resp.status != 200, "Websocket endpoint answered plain HTML request with 200."

            print("Request: request HTML status page")
            async with session.get(f'http://localhost:{master_port}/') as resp:
                assert resp.status == 200, "Server status page returned HTTP error: " + str(resp.status)
                assert '<html' in (await resp.text()).lower(), "Status query didn't return HTML."

            print("Request: HTTP on noexisting peer blob")
            for port in (master_port, master_port+2):
                async with session.get(f'http://localhost:{port}/blob/NONEXISTINGBLOB') as resp:
                    assert resp.status != 200, "Fileserver returned success on non-existing blob"

    try:
        print("Doing request tests")
        asyncio.new_event_loop().run_until_complete(
            asyncio.wait_for(request_tests(), timeout=30))
    except asyncio.TimeoutError:
        assert False, "Request tests timed out."

    _wait_seconds(10)
    _terminate_procs(master, *peers)

    print(f"All nodes terminated. Testing results...")

    assert any([('GET /blob/' in p.out.getvalue()) for p in peers]), 'No P2P transfers happened'

    for p in (master, *peers):
        _print_process_stdout(p)
        _assert_node_basics(p)
        _assert_identical_dir(p, master)
        if p is master:
            assert ('Compression ratio: 0.' in p.out.getvalue()), 'No compression happened'
        else:
            assert 'New sync batch received' in p.out.getvalue(), 'No new sync batch update from server'
        print(f'Peer {p.name} test ok')
