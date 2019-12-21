from contextlib import suppress
import multiprocessing as mp
import pytest, shutil, time, os, io, random, traceback, sys, threading, filecmp
from pathlib import Path
from types import SimpleNamespace
import common

"""
Integration tests. Creates some empty and non-empty directories, runs a master and several peer nodes in
separate processes using command line arguments. Checks that sync target dirs become identical to
source dir, and without errors in the logs.

Several edge cases are tested, too, such as empty files, different sized files and overwriting some files in
the middle of sync.
"""

TEST_DIR = './temp_test_dir'
TEST_FILES_PER_DIR = 3
CHUNK_SIZE = 1000
PORT_BASE = 53741
TEST_PEER_NAMES = {0: 'peer_empty', 1: 'peer_corrupt', 2: 'peer_non_empty'}

# Make sure buffers don't cover whole chunks, for realistic testing
common.Defaults.FILE_BUFFER_SIZE = int(CHUNK_SIZE * 0.7)
common.Defaults.DOWNLOAD_BUFFER_MAX = int(CHUNK_SIZE * 0.3)
common.Defaults.NETWORK_BUFFER_MIN = int(CHUNK_SIZE * 0.1)


@pytest.fixture()
def make_test_dirs(tmp_path):

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
                create_file(p / 'zeroes.bin', int(CHUNK_SIZE * 3.1), pattern=b'\0' * CHUNK_SIZE)
                create_file(p / 'less_zeroes.bin', int(CHUNK_SIZE * 1.1), pattern=b'\0' * CHUNK_SIZE)
                for x in range(5):
                    create_file(p / rnd_name('rnd_file', '.bin'), int(random.random() * CHUNK_SIZE * 7))
        return str(base.resolve())

    print(f"Creating seed dir contents in '{tmp_path}'...")
    seed_dir = setup_test_content(tmp_path, 'seed')
    peer_dirs = []
    for i, name in TEST_PEER_NAMES.items():
        print(f"Creating files for '{name}'")
        peer_dirs.append(setup_test_content(tmp_path, name, keep_empty=(i == 0)))
    return seed_dir, peer_dirs


# To be run in separate process:
# run syncer and send stdout + exceptions through a pipe.
def sync_proc(conn, is_master, argv):
    try:
        class PipeOut(io.RawIOBase):
            def write(self, b):
                conn.send(b)
        out = PipeOut()
        sys.stdout, sys.stderr = out, out
        sys.argv = argv
        if is_master:
            import masternode
            masternode.main()
        else:
            import peernode
            peernode.main()
        conn.send((None, None))
    except Exception as e:
        conn.send((e, traceback.format_exc()))



def test_actual_swarm_on_localhost(make_test_dirs):
    """
    Integration test. Creates a small local swarm with peers having
    different initial contents, runs it for a bit and checks that all synced up ok.
    """
    seed_dir, peer_dirs = make_test_dirs

    def spawn_sync_process(name: str, is_master: bool, sync_dir: str, port: int, master_url: str):
        print(f"Spawning process for '{name}'...")
        out = io.StringIO()
        def comm_thread(conn):
            with suppress(EOFError):
                while conn:
                    o = conn.recv()
                    if isinstance(o, tuple):
                        pass  # process exit
                    else:
                        # print(str(o))
                        out.write(str(o))
        conn_recv, conn_send = mp.Pipe(duplex=False)
        argv = ['masternode.py', sync_dir, '--port', str(port), '--concurrent-transfers', '1'] if is_master else \
               ['peernode.py', sync_dir, master_url, '--port', str(port), '--rescan-interval', '3']
        proc = mp.Process(target=sync_proc, name='sync-worker', args=(conn_send, is_master, argv))
        threading.Thread(target=comm_thread, args=(conn_recv,)).start()
        proc.start()
        return SimpleNamespace(proc=proc, is_master=is_master, out=out, name=name, dir=sync_dir)

    peers = []
    for i, name in TEST_PEER_NAMES.items():
        peers.append(spawn_sync_process(name=f'{name}', is_master=False, sync_dir=peer_dirs[i], port=PORT_BASE+1+i,
                                        master_url=f'ws://localhost:{PORT_BASE}/ws'))
        time.sleep(0.1)  # stagger peer generation a bit
        if i == 1:
            # Start server after the first two peers to test start order
            master = spawn_sync_process(name=f'master', is_master=True, sync_dir=seed_dir, port=PORT_BASE, master_url='')

    # Alter files on one peer in the middle of a sync
    time.sleep(4)
    print(f"Corrupting some files on '{peers[1].name}'...")
    shutil.rmtree(peers[1].dir + '/dir2')                       # Delete a dir
    with open(peers[1].dir+'/many_chunks.bin', 'wb') as f:      # Rewrite a file
        f.write(b'dummycontent')
    with open(peers[1].dir+'/zeroes.bin', 'r+b') as f:          # Overwrite beginning
        f.write(b'dummycontent')

    # Wait
    for x in range(8):
        print(f"Waiting {8-x} seconds for nodes before terminating...")
        time.sleep(1)

    # Kill processes
    for x in (master, *peers):
        print(f"Terminating '{x.name}'...")
        x.proc.terminate()

    print(f"All nodes killed. Testing results...")

    for p in (master, *peers):
        cmp = filecmp.dircmp(p.dir, master.dir)
        try:
            assert 'Exception' not in p.out.getvalue(), f'Exception(s) on {p.name}'
            assert 'egmentation fault' not in p.out.getvalue()
            assert p.is_master or 'Up to date' in p.out.getvalue(), 'Node never reached up-to-date state.'
            assert not cmp.diff_files, f'Differing files between {p.name} and master: {str(cmp.diff_files)}'
            assert not cmp.funny_files, f'"Funny" files between {p.name} and master: {str(cmp.funny_files)}'
            assert not cmp.left_only, f'Files found only from {p.name}: {str(cmp.left_only)}'
            assert not cmp.right_only, f'Files not found from {p.name}: {str(cmp.right_only)}'

            print(f'\n>>>>>>>>>>>>>>>>>>>> stdout for {p.name} >>>>>>>>>>>>>>>>>>>>\n')
            print(p.out.getvalue())

        except:
            print(f'\n>>>>>>>>>>>>>>>>>>>> stdout for {p.name} >>>>>>>>>>>>>>>>>>>>\n')
            print(p.out.getvalue())
            print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
            raise

        print(f'Peer {p.name} test ok')
