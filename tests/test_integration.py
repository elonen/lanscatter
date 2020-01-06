from contextlib import suppress
import multiprocessing as mp
import pytest, shutil, time, os, io, random, traceback, sys, threading, filecmp, json
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
CHUNK_SIZE = 1000
PORT_BASE = 53000 + int(2000*random.random())
TEST_PEER_NAMES = {0: 'peer_empty', 1: 'peer_corrupt', 2: 'peer_non_empty'}

# Make sure buffers don't cover whole chunks, for realistic testing
common.Defaults.FILE_BUFFER_SIZE = int(CHUNK_SIZE * 0.7)
common.Defaults.DOWNLOAD_BUFFER_MAX = int(CHUNK_SIZE * 0.3)
common.Defaults.NETWORK_BUFFER_MIN = int(CHUNK_SIZE * 0.1)


@pytest.fixture()
def test_dir_factory(tmp_path):

    def create_file(p: Path, total_size: int, pattern=None):
        pattern = bytearray(random.getrandbits(8) for _ in range(1234)) if pattern is None else pattern
        sz = 0
        with p.open('wb') as f:
            while sz < total_size:
                f.write(pattern)
                sz += len(pattern)
            f.truncate(total_size)
        mtime = int(time.time() * random.random())
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
def _sync_proc(conn, is_master, argv):
    try:
        class PipeOut(io.RawIOBase):
            def write(self, b):
                conn.send(b)
        out = PipeOut()
        sys.stdout, sys.stderr = out, out
        sys.argv = argv
        if is_master:
            masternode.main()
        else:
            peernode.main()
        conn.send((None, None))
    except Exception as e:
        conn.send((e, traceback.format_exc()))


def _spawn_sync_process(name: str, is_master: bool, sync_dir: str, port: int, master_addr: str):
    print(f"Spawning process for {'master' if is_master else 'peer'} node '{name}' at port {port}...")
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
    argv = ['masternode', sync_dir, '--port', str(port), '--concurrent-transfers', '1'] if is_master else \
           ['peernode', master_addr, sync_dir, '--port', str(port), '--rescan-interval', '3']
    proc = mp.Process(target=_sync_proc, name='sync-worker', args=(conn_send, is_master, argv))
    threading.Thread(target=comm_thread, args=(conn_recv,)).start()
    proc.start()
    return SimpleNamespace(proc=proc, is_master=is_master, out=out, name=name, dir=sync_dir)

def _wait_seconds(s):
    for x in range(s):
        print(f"Waiting {s-x} seconds...")
        time.sleep(1)

def _kill_procs(*args):
    for x in args:
        print(f"Terminating '{x.name}'...")
        x.proc.terminate()

def _assert_node_basics(p):
    assert 'Exception' not in p.out.getvalue(), f'Exception(s) on {p.name}'
    assert 'egmentation fault' not in p.out.getvalue()
    assert p.is_master or 'Up to date' in p.out.getvalue(), 'Node never reached up-to-date state.'
    assert 'traceback' not in str(p.out.getvalue()).lower()

def _print_process_stdout(p):
    print(f'\n>>>>>>>>>>>>>>>>>>>> stdout for {p.name} >>>>>>>>>>>>>>>>>>>>\n')
    print(p.out.getvalue())
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")


def _assert_identical_dir(p1, p2):
    cmp = filecmp.dircmp(p1.dir, p2.dir)
    assert not cmp.diff_files, f'Differing files between {p.name} and {p2.name}: {str(cmp.diff_files)}'
    assert not cmp.funny_files, f'"Funny" files between {p.name} and {p2.name}: {str(cmp.funny_files)}'
    assert not cmp.left_only, f'Files found only from {p.name}: {str(cmp.left_only)}'
    assert not cmp.right_only, f'Files not found from {p.name}: {str(cmp.right_only)}'




def test_minimal_downloads(test_dir_factory):
    """Test that each chunk is downloaded only once."""
    master = _spawn_sync_process(f'seed', True, test_dir_factory('seed'), PORT_BASE, '')
    peer = _spawn_sync_process(f'leecher', False, test_dir_factory('leecher', keep_empty=True), PORT_BASE+1, f'localhost:{PORT_BASE}')

    _wait_seconds(4)
    _kill_procs(master, peer)

    # Count hash downloads from master's log
    master_lines = str(master.out.getvalue()).split('\n')
    dl_lines = [l for l in master_lines if 'GET /blob/' in l]
    assert len(dl_lines) > 0, "No download log messages on master"
    downloaded_hashes = [l.split('/')[-1] for l in dl_lines]
    for h in downloaded_hashes:
        assert downloaded_hashes.count(h) == 1, f"Hash {h} was downloaded more than once."

    # Also check for basic conditions
    for p in (master, peer):
        _print_process_stdout(p)
        _assert_node_basics(p)
        _assert_identical_dir(p, master)
        print(f'Peer {p.name} test ok')



def test_swarm__corruption__bad_protocol__uptodate__errors(test_dir_factory):
    """
    Integration test. Creates a small local swarm with peers having
    different initial contents, runs it for a bit and checks that all synced up ok.
    """
    seed_dir = test_dir_factory('seed')
    peer_dirs = [test_dir_factory(TEST_PEER_NAMES[i], keep_empty=(i == 0)) for i in TEST_PEER_NAMES.keys()]

    master = None
    peers = []
    for i, name in TEST_PEER_NAMES.items():
        peers.append(_spawn_sync_process(f'{name}', False, peer_dirs[i], PORT_BASE+1+i, f'localhost:{PORT_BASE}'))
        time.sleep(0.1)  # stagger peer generation a bit
        if i == 1:
            # Start server after the first two peers to test start order
            master = _spawn_sync_process(f'master', True, seed_dir, PORT_BASE, '')

    # Alter files on one peer in the middle of a sync
    _wait_seconds(4)
    print(f"Corrupting some files on '{peers[1].name}'...")
    try:
        shutil.rmtree(peers[1].dir + '/dir2')                       # Delete a dir
        with open(peers[1].dir+'/to_be_corrupted.bin', 'wb') as f:      # Rewrite a file
            f.write(b'dummycontent')
        with open(peers[1].dir+'/zeroes_to_corrupt.bin', 'r+b') as f:          # Overwrite beginning
            f.write(b'dummycontent')
    except PermissionError as e:
        print("WARNING: File corruption during test failed because of file locking or something: " + str(e))

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
            async with session.ws_connect(f'ws://localhost:{PORT_BASE}/join') as ws:
                await ws.send_json({'action': 'BAD_COMMAND'})
                recvd = await read_respose_msgs(ws)
                assert not recvd or ('fatal' in recvd[-1]['action'])

            print("Request: try bad json")
            async with session.ws_connect(f'ws://localhost:{PORT_BASE}/join') as ws:
                await ws.send_str('INVALID_JSON')
                recvd = await read_respose_msgs(ws)
                assert not recvd or ('fatal' in recvd[-1]['action'])

            print("Request: HTTP on websocet endpoint")
            async with session.get(f'http://localhost:{PORT_BASE}/join') as resp:
                assert resp.status != 200, "Websocket endpoint answered plain HTML request with 200."

            print("Request: request HTML status page")
            async with session.get(f'http://localhost:{PORT_BASE}/') as resp:
                assert resp.status == 200, "Server status page returned HTTP error: " + str(resp.status)
                assert '<html' in (await resp.text()).lower(), "Status query didn't return HTML."

    try:
        print("Doing request tests")
        asyncio.new_event_loop().run_until_complete(
            asyncio.wait_for(request_tests(), timeout=20))
    except asyncio.TimeoutError:
        assert False, "Request tests timed out."

    _wait_seconds(10)
    _kill_procs(master, *peers)

    print(f"All nodes killed. Testing results...")

    assert any([('GET /blob/' in p.out.getvalue()) for p in peers]), 'No P2P transfers happened'

    for p in (master, *peers):
        _print_process_stdout(p)
        _assert_node_basics(p)
        _assert_identical_dir(p, master)
        print(f'Peer {p.name} test ok')
