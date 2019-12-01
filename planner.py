from typing import Iterable, MutableSequence, Optional, Dict, List, Set
from types import SimpleNamespace
import asyncio, random, time
from contextlib import suppress
import statistics

"""
HIGH LEVEL DISTRIBUTION STRATEGY PLANNER

This takes chunk and node (peer) IDs and
calculates efficient transfer plans to
minimize total transfer speed, maximize
network load distribution and resilience
against errors and slowdowns.

The planner ignores most implementation details
like network protocol, hash algorithms, system
time etc, making it easy to tune and debug.

Running this file as a python module from CLI
graphically simulates a swarm and calculates
efficiency compared to ideal, error-free
multicast to all nodes.
"""

class Transfer(object):
    def __init__(self, to_node: int, from_node: int, chunk, timeout: float):
        self.from_node = from_node
        self.to_node = to_node
        self.chunk = chunk
        self.timeout_secs = timeout
    def __hash__(self):
        return hash(str(self.chunk) + str(self.from_node) + str(self.to_node))
    def __eq__(self, b):
        return self.chunk == b.chunk and self.from_node == b.from_node and self.to_node == b.to_node

class Node(object):
    def __init__(self, node_id: int, chunks: Iterable, concurrent_dls: int, concurrent_uls: int):
        self.node_id = node_id
        self.chunks: set = set(chunks)
        self.max_concurrent_dls: int = concurrent_dls
        self.max_concurrent_uls: int = concurrent_uls
        self.downloads: MutableSequence[Transfer] = []
        self.uploads: MutableSequence[Transfer] = []
        self.avg_ul_time = None
    def __hash__(self):
        return hash(self.node_id)
    def __eq__(self, b):
        return self.node_id == b.chunk



class ChunkDistributionPlanner(object):

    def __init__(self, chunks: Iterable):
        self.all_chunks = frozenset(chunks)
        self.nodes: Dict[int, Node] = {}
        self.chunk_popularity = {c: 0 for c in chunks}  # Approximately: how many copies of chunks there are in swarm
        self.all_done = False  # optimization, turns True when everyone's gat everything

        self._next_node_id: int = 0
        self._master_nodes: Set[int] = set()

    def on_node_join(self, chunks: Iterable, concurrent_dls: int, concurrent_uls: int, master_node=False) -> int:
        """
        Node joins the swarm.
        :param: master_node  If true, clients will never be instructed to timeout downloads from this node
        :return: ID for the newly joined node
        """ 
        node_id = self._next_node_id
        self._next_node_id += 1
        self.nodes[node_id] = Node(node_id=node_id, chunks=set(chunks),
                                   concurrent_dls=concurrent_dls, concurrent_uls=concurrent_uls)
        self.all_done = False
        self.on_node_got_chunks(node_id, chunks)
        if master_node:
            self._master_nodes.add(node_id)
        return node_id

    def on_node_leave(self, node_id: int) -> None:
        """Node leaves the swarm"""
        if node_id in self.nodes:
            del self.nodes[node_id]
        # Cleanup stale transfers from removed node
        for ni in self.nodes.keys():
            n = self.nodes[ni]
            n.downloads = [d for d in n.downloads if d.from_node != node_id]

    def on_node_got_chunks(self, node_id: int, new_chunks: Iterable) -> None:
        """# Node reports new chunks it has"""
        node = self.nodes.get(node_id)
        if node is not None:
            node.chunks.update(new_chunks)
            for c in new_chunks:
                self.chunk_popularity[c] += 1
            # Sanity check
            if node.chunks - self.all_chunks:
                raise UserWarning(f'Bug? Node {node_id} has unknown chunks.')
            # If current node's got all chunks, check if others have too
            if len(node.chunks) == len(self.all_chunks):
                self.all_done = all(len(n.chunks) == len(self.all_chunks) for n in self.nodes.values())

    def on_node_report_transfers(self, node_id: int, transfers: Iterable[Transfer], last_ul_time=None) -> None:
        """Node reports its currently ongoing transfers
        :param last_ul_time: How long latest upload from node took, in seconds. Used for avoiding slow nodes.
        """
        node = self.nodes.get(node_id)
        if node is not None:
            node.downloads = [t for t in transfers if t.to_node == node_id]
            node.uploads = [t for t in transfers if t.from_node == node_id]
            if last_ul_time:
                node.avg_ul_time = (node.avg_ul_time or last_ul_time) * 0.8 + last_ul_time * 0.2
            if any(t.to_node != node_id and t.from_node != node_id for t in transfers):
                raise UserWarning(f"Bug? Node {node_id} reported transfers it's not part of.")

    def avg_ul_time(self, node_id: int) -> Optional[float]:
        """Return average upload time of given node"""
        return self.nodes[node_id].avg_ul_time

    def plan(self) -> Iterable[Transfer]:
        """
        Calculates a transfer plan by pairing up nodes with free download and upload slots.
        :return: List of Transfers to initiate
        """
        # Consider nodes with least chunks first for both DL and UL, for optimal speed and network load distribution
        free_downloaders = sorted((n for n in self.nodes.values() if len(n.downloads) < n.max_concurrent_dls),
                                  key=lambda n: len(n.chunks))
        free_uploaders = sorted((n for n in self.nodes.values() if len(n.uploads) < n.max_concurrent_uls),
                                key=lambda n: len(n.chunks) * (n.avg_ul_time or 999))  # favor fast uploaders
        if not free_uploaders or not free_downloaders: return ()

        # Timeout P2P downloads quickly at first, then use actual statistics as we get
        ul_times = [float(n.avg_ul_time or 0) for n in self.nodes.values() if n.avg_ul_time]
        median_time = statistics.median(ul_times) if ul_times else 1

        # Avoid slow peers but not completely (proportional to their speed)
        free_uploaders = (n for n in free_uploaders if n.node_id in self._master_nodes or
                          not n.avg_ul_time or random.random() < (median_time/n.avg_ul_time))

        # Match uploaders and downloaders to transfer rarest chunks first:
        proposed_transfers = []
        for ul in free_uploaders:
            for _ in range(ul.max_concurrent_uls - len(ul.uploads)):
                for dl in free_downloaders:
                    if len(dl.downloads) < dl.max_concurrent_dls:  # need to recheck every iteration
                        new_chunks = ul.chunks - dl.chunks - set(t.chunk for t in dl.downloads)
                        if new_chunks:
                            # Pick the rarest chunk first for best distribution
                            chunk = min(new_chunks, key=lambda c: self.chunk_popularity[c])
                            timeout = median_time*4 if (ul.node_id not in self._master_nodes) else 9999999
                            t = Transfer(from_node=ul.node_id, to_node=dl.node_id, chunk=chunk, timeout=timeout)
                            proposed_transfers.append(t)
                            # To prevent overbooking and to help picking different chunks, assume transfer will succeed
                            self.chunk_popularity[chunk] += 1
                            dl.downloads.append(t)  # Replaced later by on_node_report_transfers(), but
                            ul.uploads.append(t)  # setting it optimistically helps in planning.
                            break

        assert (all(t.to_node != t.from_node for t in proposed_transfers))
        assert (len(proposed_transfers) == len(set(proposed_transfers)))

        return proposed_transfers


# --------------------------------------------------------------------------------------------------------------------


def simulate() -> None:
    '''
    Simulate file swarm, controlled by ChunkDistributionPlanner.
    Prints a block diagram to stdout until all blocks are done.
    '''
    N_CHUNKS = 72
    N_NODES = 38
    SEEDER_UL_SLOTS = 4
    NODE_UL_SLOT = 2
    TRANSFER_TIME_MIN, TRANSFER_TIME_MAX = 0.2, 0.3  # Limits for randomizing one chunk transfer time
    SPEED_VARIABILITY_PER_NODE = 1.1  # "normal speed" nodes will take 1-N x average speed

    # For simulating network errors and slowdowns
    ERROR_PROBABILITY = 1/100  # Probability of individual transfer failing

    DROPOUT_PROBABILITY = 1/(N_CHUNKS * N_NODES)*8  # Probability of node dropping out of swarm completely
    JOIN_PROBABILITY = 10/N_CHUNKS*3

    SLOWDOWN_PROBABILITY = 1/8  # every N't node will be very slow uploader
    SLOWDOWN_FACTOR = 100  # transfer time multiplier for "very slow" nodes

    test_chunks = tuple(range(N_CHUNKS))
    planner = ChunkDistributionPlanner(test_chunks)

    plan_now = asyncio.Event()  # asyncio event to wake planner
    joins_left = N_NODES
    ext_nodes: Dict[int, SimpleNamespace] = {}

    # Create seeder node
    seeder_id = planner.on_node_join(test_chunks, 0, SEEDER_UL_SLOTS, master_node=True)  # initial seed node
    ext_nodes[seeder_id] = SimpleNamespace(id=seeder_id, chunks=set(test_chunks), tfers=set(), speed_fact=1)

    def add_node():
        nonlocal ext_nodes, joins_left
        joins_left -= 1
        speed_fact = SLOWDOWN_FACTOR if (random.random() < SLOWDOWN_PROBABILITY) else \
            random.uniform(1, SPEED_VARIABILITY_PER_NODE)
        assert (speed_fact >= 1)
        i = planner.on_node_join((), NODE_UL_SLOT, NODE_UL_SLOT)
        ext_nodes[i] = SimpleNamespace(id=i, chunks=set(), tfers=set(), speed_fact=speed_fact)

    for _ in range(int(N_NODES/2)):
        add_node()

    async def simulate_transfer(t: Transfer):
        from_n, to_n = ext_nodes.get(t.from_node), ext_nodes.get(t.to_node)
        if not from_n or not to_n: return  # node left swarm
        rnd_time = random.uniform(TRANSFER_TIME_MIN, TRANSFER_TIME_MAX) * from_n.speed_fact
        try:
            if random.random() < ERROR_PROBABILITY/2: return  # simulate initialization failure sometimes
            # Mark transfer as ongoing
            for n in (to_n, from_n):
                n.tfers.add(t)
                planner.on_node_report_transfers(n.id, n.tfers)
            # Wait = simulate transfer
            if rnd_time < t.timeout_secs:
                await asyncio.sleep(rnd_time)
            else:
                await asyncio.sleep(t.timeout_secs)
                print("Slow download. Giving up.")
                return
            if random.random() < ERROR_PROBABILITY / 2: return  # simulate transfer failure sometimes
            # Mark chunk as received
            to_n.chunks.add(t.chunk)
            planner.on_node_got_chunks(to_n.id, (t.chunk,))
        finally:
            # Cleanup
            for n in (to_n, from_n):
                with suppress(KeyError):
                    n.tfers.remove(t)
                planner.on_node_report_transfers(n.id, n.tfers, last_ul_time=(rnd_time if n == from_n else None))
                plan_now.set()

    async def planner_loop():
        while not planner.all_done:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(plan_now.wait(), timeout=1)
            plan_now.clear()

            if random.random() < JOIN_PROBABILITY and joins_left > 0:
                print("Node join")
                add_node()

            for t in planner.plan():
                # Simulate node dropout
                if random.random() < DROPOUT_PROBABILITY:
                    print("DROPOUT " + str(t.to_node))
                    del (ext_nodes[t.to_node])
                    planner.on_node_leave(t.to_node)
                else:
                    # Start "transfer":
                    asyncio.create_task(simulate_transfer(t))

    async def runner():
        nonlocal plan_now
        plan_now = asyncio.Event()
        plan_now.set()

        def print_status():
            print("")
            for n in ext_nodes.values():
                dls = [t for t in n.tfers if t.to_node == n.id]
                uls = [t for t in n.tfers if t.from_node == n.id]
                print('%02d' % n.id, ''.join(('#' if c in n.chunks else '.') for c in test_chunks), len(dls),
                      len(uls), '  %.1f' % (planner.avg_ul_time(n.id) or -1))

        start_t = time.time()
        asyncio.create_task(planner_loop())
        while not planner.all_done:
            print_status()
            await asyncio.sleep(0.5)
        print_status()

        best_t = (TRANSFER_TIME_MIN+TRANSFER_TIME_MAX)/2 * N_CHUNKS / min(SEEDER_UL_SLOTS, NODE_UL_SLOT)
        print("ALL DONE. Efficiency vs. ideal multicast = %.1f%%" % (best_t / (time.time() - start_t) * 100))

    asyncio.run(runner())


if __name__ == "__main__":
    simulate()
