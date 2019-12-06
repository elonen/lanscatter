from typing import Iterable, Optional, Set, Any
from types import SimpleNamespace
import asyncio, random, time
from contextlib import suppress
import statistics
from abc import ABC, abstractmethod

"""
HIGH LEVEL DISTRIBUTION STRATEGY PLANNER

This keeps track of which nodes have which chunks
and  calculates efficient transfer plans to
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

ChunkId = str

# Abstract base class for a p2p swarm node, used by the coordinator
class Node(ABC):
    chunks: Set[ChunkId]            # Set of chunks the node has
    max_concurrent_dls: int         # How many concurrent downloads the node allows
    max_concurrent_uls: int         # --||-- uploads --||--
    active_downloads: int           # Number of currently actually ongoing downloads
    active_uploads: int             # --||-- uploads --||--
    incoming_chunks: Set[ChunkId]   # Which chunks the node is currently downloading
    avg_ul_time: Optional[float]    # (Rolling) average time it has taken for the node upload one chunk
    client: Any = None              # Reserved for implementing classes

    @abstractmethod
    def destroy(self) -> None:
        """Remove node from the swarm"""
        pass

    @abstractmethod
    def add_chunks(self, new_chunks: Iterable[ChunkId], clear_first=False) -> Iterable[ChunkId]:
        """
        Mark node as having given (new) chunks
        :param new_chunks: Chunks to add
        :param clear_first: If False, add given chunks to old ones, otherwise clear old and then add.
        :return: List of unknown chunks, if any
        """
        if clear_first:
            self.chunks.clear()
        self.chunks.update(new_chunks)
        return ()

    def set_active_transfers(self, incoming_chunks: Iterable[ChunkId], n_downloads: int, n_uploads: int) -> None:
        """Set currently ongoing transfers on the node
        :param incoming_chunks: List of chunks the node is currently downloading
        :param n_downloads: Number of currently active downloads
        :param n_uploads: Number of currently active uploads
        """
        self.incoming_chunks = set(incoming_chunks)
        self.active_downloads, self.active_uploads = n_downloads, n_uploads

    def update_transfer_speed(self, last_ul_time: float) -> None:
        """Update upload speed average for smart scheduling.
        :param last_ul_time: Duration of latest upload from this node
        """
        # update Exponential Moving Average (EMA) of upload time
        self.avg_ul_time = (self.avg_ul_time or last_ul_time) * 0.8 + last_ul_time * 0.2


class Transfer(object):
    def __init__(self, to_node: Node, from_node: Node, chunk: ChunkId, timeout: float):
        self.from_node = from_node
        self.to_node = to_node
        self.chunk = chunk
        self.timeout_secs = timeout


class SwarmCoordinator(object):

    def __init__(self):
        '''
        Make an empty planner with no chunks or nodes.
        '''
        self.all_chunks: Set[ChunkId] = set()
        self.chunk_popularity = {}  # Approximately: how many copies of chunks there are in swarm
        self.nodes = []
        self._master_nodes: Set[Node] = set()
        self.all_done = False  # optimization, turns True when everyone's gat everything

    def reset_chunks(self, new_chunks: Iterable[ChunkId]):
        new_chunks = set(new_chunks)
        self.all_done = False
        self.all_chunks = new_chunks
        for c in new_chunks:
            assert (isinstance(c, ChunkId))
        for n in self.nodes:
            n.chunks.intersection_update(new_chunks)  # forget obsolete chunks on nodes
        self.chunk_popularity = {c: (self.chunk_popularity.get(c) or 0) for c in new_chunks}

    def node_join(self, chunks: Iterable[ChunkId], concurrent_dls: int, concurrent_uls: int, master_node=False) -> Node:
        """
        Node joins the swarm.
        :param: master_node  If true, clients will never be instructed to timeout downloads from this node
        :return: Newly joined node
        """
        swarm = self

        class _NodeImpl(Node):

            def __init__(self):
                self.chunks = set()
                self.max_concurrent_dls = concurrent_dls
                self.max_concurrent_uls = concurrent_uls
                self.active_downloads = 0
                self.active_uploads = 0
                self.incoming_chunks = set()
                self.avg_ul_time = None
                self.add_chunks(chunks)

            def destroy(self) -> None:
                """Remove node from the swarm"""
                swarm.nodes = [n for n in swarm.nodes if n != self]

            def add_chunks(self, new_chunks: Iterable[ChunkId], clear_first=False) -> Iterable[ChunkId]:
                new_chunks = set(new_chunks)
                unknown_chunks = new_chunks - swarm.all_chunks
                new_chunks -= unknown_chunks
                super().add_chunks(new_chunks, clear_first)
                for c in new_chunks:
                    assert(isinstance(c, ChunkId))
                    swarm.chunk_popularity[c] += 1
                # If current node's got all chunks, check if others have too
                if len(self.chunks) == len(swarm.all_chunks):
                    swarm.all_done = all(len(n.chunks) == len(swarm.all_chunks) for n in swarm.nodes)
                return unknown_chunks

        n = _NodeImpl()
        self.nodes.append(n)
        self.all_done &= (len(n.chunks) == self.all_chunks)
        if master_node:
            self._master_nodes.add(n)
        return n


    def plan_transfers(self) -> Iterable[Transfer]:
        """
        Calculates a transfer plan by pairing up nodes with free download and upload slots.
        :return: List of Transfers to initiate
        """
        # Consider nodes with least chunks first for both DL and UL, for optimal speed and network load distribution
        free_downloaders = sorted((n for n in self.nodes if n.active_downloads < n.max_concurrent_dls and
                                   len(n.chunks) < len(self.all_chunks)),
                           key=lambda n: len(n.chunks))
        free_uploaders = sorted((n for n in self.nodes if n.active_uploads < n.max_concurrent_uls),
                                key=lambda n: len(n.chunks) * (n.avg_ul_time or 999))  # favor fast uploaders
        if not free_uploaders or not free_downloaders: return ()

        # Timeout P2P downloads quickly at first, then use actual statistics as we get
        ul_times = [float(n.avg_ul_time or 0) for n in self.nodes if n.avg_ul_time]
        median_time = statistics.median(ul_times) if ul_times else 1

        # Avoid slow peers but not completely (proportional to their speed)
        free_uploaders = [n for n in free_uploaders if n in self._master_nodes or
                          not n.avg_ul_time or random.random() < (median_time/n.avg_ul_time)]

        # Match uploaders and downloaders to transfer rarest chunks first:
        proposed_transfers = []
        for ul in free_uploaders:
            for _ in range(ul.max_concurrent_uls - ul.active_uploads):
                for dl in free_downloaders:
                    if dl.active_downloads < dl.max_concurrent_dls:  # need to recheck every iteration
                        new_chunks = ul.chunks - dl.chunks - dl.incoming_chunks
                        if new_chunks:
                            # Pick the rarest chunk first for best distribution
                            chunk = min(new_chunks, key=lambda c: self.chunk_popularity[c])
                            timeout = median_time*4 if (ul not in self._master_nodes) else 9999999
                            t = Transfer(from_node=ul, to_node=dl, chunk=chunk, timeout=timeout)
                            proposed_transfers.append(t)
                            # To prevent overbooking and to help picking different chunks, assume transfer will succeed
                            self.chunk_popularity[chunk] += 1
                            # These are replareplaced later by client's own report of actual transfers
                            # (on_node_report_transfers()) but we'll assume the transfers start ok (to aid planning):
                            assert(chunk not in dl.incoming_chunks)
                            dl.incoming_chunks.add(chunk)
                            dl.active_downloads += 1
                            ul.active_uploads += 1
                            break

        assert (all(t.to_node != t.from_node for t in proposed_transfers))
        assert (len(proposed_transfers) == len(set(proposed_transfers)))

        return proposed_transfers


# --------------------------------------------------------------------------------------------------------------------


def simulate() -> None:
    '''
    Simulate file swarm, controlled by SwarmCoordinator.
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

    swarm = SwarmCoordinator()
    swarm.reset_chunks((str(i) for i in range(N_CHUNKS)))

    plan_now_trigger = asyncio.Event()  # asyncio event to wake planner
    joins_left, next_node_num = N_NODES, 0

    def new_simu_node(master=False):
        nonlocal joins_left, next_node_num
        joins_left -= 1
        n = swarm.node_join((), NODE_UL_SLOT, NODE_UL_SLOT) if not master else \
            swarm.node_join(swarm.all_chunks, 0, SEEDER_UL_SLOTS, master_node=True)
        speed_fact = 1.0 if master else (SLOWDOWN_FACTOR if (random.random() < SLOWDOWN_PROBABILITY) else
                                         random.uniform(1, SPEED_VARIABILITY_PER_NODE))
        n.client = SimpleNamespace(simu_tfers=set(), simu_speed_fact=speed_fact, nick='%02d' % next_node_num)
        next_node_num += 1
        assert n.client.simu_speed_fact >= 1
        if master:
            unk = n.add_chunks(swarm.all_chunks)
            assert(not unk)
        return n

    seeder = new_simu_node(master=True)
    for _ in range(int(N_NODES/2)):
        new_simu_node()

    def report_transfers(node):
        dls = [t for t in node.client.simu_tfers if t.to_node is node]
        uls = [t for t in node.client.simu_tfers if t.from_node is node]
        node.set_active_transfers((t.chunk for t in dls), len(dls), len(uls))

    async def simulate_transfer(t: Transfer):
        rnd_time = random.uniform(TRANSFER_TIME_MIN, TRANSFER_TIME_MAX) * t.from_node.client.simu_speed_fact
        try:
            if random.random() < ERROR_PROBABILITY/2: return  # simulate initialization failure sometimes
            # Mark transfer as ongoing
            for n in (t.to_node, t.from_node):
                n.client.simu_tfers.add(t)
                report_transfers(n)

            # Wait = simulate transfer
            if rnd_time < t.timeout_secs:
                await asyncio.sleep(rnd_time)
            else:
                await asyncio.sleep(t.timeout_secs)
                print("Slow download. Giving up.")
                return

            if random.random() < ERROR_PROBABILITY / 2: return  # simulate transfer failure sometimes

            # Mark chunk as received
            unk = t.to_node.add_chunks([t.chunk])
            assert(not unk)
        finally:
            if rnd_time is not None:
                t.from_node.update_transfer_speed(rnd_time)
            # Cleanup
            for n in (t.to_node, t.from_node):
                with suppress(KeyError):
                    n.client.simu_tfers.remove(t)
                report_transfers(n)
                plan_now_trigger.set()


    async def planner_loop():
        while not swarm.all_done:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(plan_now_trigger.wait(), timeout=1)
            plan_now_trigger.clear()

            if random.random() < JOIN_PROBABILITY and joins_left > 0:
                print("Node join")
                new_simu_node()

            for t in swarm.plan_transfers():
                # Simulate node dropout
                if random.random() < DROPOUT_PROBABILITY:
                    print("DROPOUT " + str(t.to_node.client.nick))
                    t.to_node.destroy()
                else:
                    # Run simulated transfer
                    asyncio.create_task(simulate_transfer(t))

    async def runner():
        nonlocal plan_now_trigger
        plan_now_trigger = asyncio.Event()
        plan_now_trigger.set()

        def print_status():
            print("")
            for n in swarm.nodes:
                dls = [t for t in n.client.simu_tfers if t.to_node == n]
                uls = [t for t in n.client.simu_tfers if t.from_node == n]
                print(n.client.nick, ''.join(('#' if c in n.chunks else '.') for c in seeder.chunks), len(dls),
                      len(uls), '  %.1f' % (n.avg_ul_time or -1))

        start_t = time.time()
        asyncio.create_task(planner_loop())
        while not swarm.all_done:
            print_status()
            await asyncio.sleep(0.5)
        print_status()

        best_t = (TRANSFER_TIME_MIN+TRANSFER_TIME_MAX)/2 * N_CHUNKS / min(SEEDER_UL_SLOTS, NODE_UL_SLOT)
        print("ALL DONE. Efficiency vs. ideal multicast = %.1f%%" % (best_t / (time.time() - start_t) * 100))

    asyncio.run(runner())


if __name__ == "__main__":
    simulate()
