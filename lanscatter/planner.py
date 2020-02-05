from typing import Iterable, Optional, Set, Any, List
from types import SimpleNamespace
import asyncio, random, time
from contextlib import suppress
import statistics
from abc import ABC, abstractmethod

"""
HIGH LEVEL DISTRIBUTION STRATEGY PLANNER

This keeps track of which nodes have which hashes
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

ChunkId = str  # type alias

# Abstract base class for a p2p swarm node, used by the coordinator
class Node(ABC):
    hashes: Set[ChunkId]            # Set of hashes the node has
    max_concurrent_dls: int         # How many concurrent downloads the node allows
    max_concurrent_uls: int         # --||-- uploads --||--
    active_downloads: int           # Number of currently actually ongoing downloads
    active_uploads: int             # --||-- uploads --||--
    incoming: Set[ChunkId]          # Which hashed blobs the node is currently downloading (or rescanning)
    avg_ul_time: Optional[float]    # (Rolling) average time it has taken for the node upload one chunk
    name: str                       # Human friendly name for the node (e.g. hostname or ip address)
    is_master: bool                 # If true, downloads will never be instructed to timeout
    client: Any = None              # Reserved for implementing classes

    @abstractmethod
    def destroy(self) -> None:
        """Remove node from the swarm"""
        raise NotImplementedError

    @abstractmethod
    def add_hashes(self, new_hashes: Iterable[ChunkId], clear_first=False) -> Iterable[ChunkId]:
        """
        Mark node as having got (new) hahses
        :param new_hashes: Hashes to add
        :param clear_first: If False, add given hash to old ones, otherwise clear old and then add.
        :return: List of unknown hashes, if any
        """
        if clear_first:
            self.hashes.clear()
        self.hashes.update(new_hashes)
        return ()

    def set_active_transfers(self, incoming: Iterable[ChunkId], n_downloads: int, n_uploads: int) -> None:
        """Set currently ongoing transfers on the node
        :param incoming: List of hashes the node is currently downloading / rehashing
        :param n_downloads: Number of currently active downloads
        :param n_uploads: Number of currently active uploads
        """
        self.incoming = set(incoming)
        self.active_downloads, self.active_uploads = n_downloads, n_uploads

    def update_transfer_speed(self, upload_times: Iterable[float]) -> None:
        """Update upload speed average for smart scheduling.
        :param upload_times: List of duration of latest uploads from this node
        """
        # update Exponential Moving Average (EMA) of upload time
        for t in upload_times:
            self.avg_ul_time = ((self.avg_ul_time or t) * 0.8) + (t * 0.2)


class Transfer:
    def __init__(self, to_node: Node, from_node: Node, chunk_hash: ChunkId, timeout: float):
        self.from_node = from_node
        self.to_node = to_node
        self.hash = chunk_hash
        self.timeout_secs = timeout


class SwarmCoordinator(object):

    def __init__(self):
        """
        Make an empty planner with no hashes or nodes.
        """
        self.all_hashes: Set[ChunkId] = set()
        self.hash_popularity = {}  # Approximately: how many copies of hashes there are in swarm
        self.nodes: Set[Node] = set()
        self.all_done = False  # optimization, turns True when everyone's gat everything

    def reset_hashes(self, new_hashes: Iterable[ChunkId]):
        new_hashes = set(new_hashes)
        self.all_done = False
        self.all_hashes = new_hashes
        for c in new_hashes:
            assert (isinstance(c, ChunkId))
        for n in self.nodes:
            n.hashes.intersection_update(new_hashes)  # forget obsolete hashes on nodes
        self.hash_popularity = {c: (self.hash_popularity.get(c) or 0) for c in new_hashes}

    def node_join(self, initial_hashes: Iterable[ChunkId],
                  concurrent_dls: int, concurrent_uls: int, master_node=False) -> Node:
        """
        Join node to swarm, ready to sync.
        :param initial_hashes: Hashes that node has already.
        :param concurrent_dls: Number of simultaneous downloads allowed.
        :param concurrent_uls: Number of simultaneous uploads allowed.
        :param master_node:  If true, clients will never be instructed to timeout downloads from this node
        :return: Newly joined node
        """
        swarm = self

        class _NodeImpl(Node):

            def __init__(self):
                self.hashes = set()
                self.max_concurrent_dls = concurrent_dls
                self.max_concurrent_uls = concurrent_uls
                self.active_downloads = 0
                self.active_uploads = 0
                self.incoming = set()
                self.avg_ul_time = None
                self.add_hashes(initial_hashes)
                self.is_master = master_node
                self.name = 'anon'

            def destroy(self) -> None:
                """Remove node from the swarm"""
                swarm.nodes.discard(self)
                for c in self.hashes:
                    if c in swarm.hash_popularity:
                        swarm.hash_popularity[c] -= 1

            def add_hashes(self, new_hashes: Iterable[ChunkId], clear_first=False) -> Iterable[ChunkId]:
                new_hashes = set(new_hashes)
                unknown = new_hashes - swarm.all_hashes
                new_hashes -= unknown
                super().add_hashes(new_hashes, clear_first)
                for c in new_hashes:
                    assert(isinstance(c, ChunkId))
                    swarm.hash_popularity[c] += 1
                # If current node's got all hashes, check if others have too
                if len(self.hashes) == len(swarm.all_hashes):
                    swarm.all_done = all(len(n.hashes) == len(swarm.all_hashes) for n in swarm.nodes)
                return unknown

        n = _NodeImpl()
        self.nodes.add(n)
        self.all_done &= (len(n.hashes) == self.all_hashes)
        return n


    def plan_transfers(self) -> Iterable[Transfer]:
        """
        Calculates a transfer plan by pairing up nodes with free download and upload slots.
        :return: List of Transfers to initiate
        """
        # Timeout P2P downloads quickly at first, then use actual statistics as we get
        ul_times = [float(n.avg_ul_time or 0) for n in self.nodes if n.avg_ul_time]
        median_time = statistics.median(ul_times) if ul_times else 1

        def uploader_weight(n):
            # Favor fast non-master nodes with few hashes
            return len(n.hashes) * (n.avg_ul_time or median_time) * (1.5 if n.is_master else 1)

        # Consider nodes with least hashes first for both DL and UL, for optimal speed and network load distribution
        free_downloaders = sorted((n for n in self.nodes if n.active_downloads < n.max_concurrent_dls and
                                   len(n.hashes) < len(self.all_hashes)),
                                  key=lambda n: len(n.hashes))
        free_uploaders = sorted((n for n in self.nodes if n.active_uploads < n.max_concurrent_uls),
                                key=uploader_weight)  # lower score is selected first
        if not free_uploaders or not free_downloaders:
            return ()

        # Avoid slow peers but not completely (proportional to their speed)
        free_uploaders = [n for n in free_uploaders if n.is_master or
                          not n.avg_ul_time or random.random() < (median_time/n.avg_ul_time)]

        # Match uploaders and downloaders to transfer rarest hashes first:
        proposed_transfers = []
        for ul in free_uploaders:
            for __ in range(ul.max_concurrent_uls - ul.active_uploads):
                for dl in free_downloaders:
                    if dl.active_downloads < dl.max_concurrent_dls:  # need to recheck every iteration
                        new_hashes = ul.hashes - dl.hashes - dl.incoming
                        if new_hashes:
                            # Pick the rarest hash first for best distribution
                            h = min(new_hashes, key=lambda c: self.hash_popularity[c])
                            timeout = median_time*4 if (not ul.is_master) else 9999999
                            t = Transfer(from_node=ul, to_node=dl, chunk_hash=h, timeout=timeout)
                            proposed_transfers.append(t)
                            # To prevent overbooking and to help picking different hashes, assume transfer will succeed
                            self.hash_popularity[h] += 1
                            # These are replaced later by client's own report of actual transfers
                            # (on_node_report_transfers()) but we'll assume the transfers start ok, to aid planning:
                            assert(h not in dl.incoming)
                            dl.incoming.add(h)
                            dl.active_downloads += 1
                            ul.active_uploads += 1
                            break

        assert (all(t.to_node != t.from_node for t in proposed_transfers))
        assert (len(proposed_transfers) == len(set(proposed_transfers)))

        return proposed_transfers

    def get_status_table(self):
        hashes = sorted(list(self.all_hashes))
        nodes = []
        for n in self.nodes:
            nodes.append({
                'name': n.name,
                'dls': n.active_downloads, 'uls': n.active_uploads, 'busy': n.incoming,
                'hashes': [(1 if c in n.hashes else (0.5 if c in n.incoming else 0)) for c in hashes],
                'avg_ul_time': n.avg_ul_time or -1
            })
        return {'all_hashes': hashes, 'nodes': nodes, 'all_done': self.all_done}


# --------------------------------------------------------------------------------------------------------------------
# << End of production code. The rest is for testing:
# --------------------------------------------------------------------------------------------------------------------

def simulate() -> None:
    """
    Simulate file swarm, controlled by SwarmCoordinator.
    Prints a block diagram to stdout until all blocks are done.
    """
    N_HASHES = 72
    N_NODES = 38
    SEEDER_UL_SLOTS = 4
    NODE_UL_SLOT = 2
    TRANSFER_TIME_MIN, TRANSFER_TIME_MAX = 0.2, 0.3  # Limits for randomizing one hash transfer time
    SPEED_VARIABILITY_PER_NODE = 1.1  # "normal speed" nodes will take 1-N x average speed

    # For simulating network errors and slowdowns
    ERROR_PROBABILITY = 1/100  # Probability of individual transfer failing

    DROPOUT_PROBABILITY = 1/(N_HASHES * N_NODES)*8  # Probability of node dropping out of swarm completely
    JOIN_PROBABILITY = 10/N_HASHES*3

    SLOWDOWN_PROBABILITY = 1/8  # every N't node will be very slow uploader
    SLOWDOWN_FACTOR = 100  # transfer time multiplier for "very slow" nodes

    swarm = SwarmCoordinator()
    swarm.reset_hashes((str(i) for i in range(N_HASHES)))

    plan_now_trigger = asyncio.Event()  # asyncio event to wake planner
    joins_left, next_node_num = N_NODES, 0

    def new_simu_node(master=False):
        nonlocal joins_left, next_node_num
        joins_left -= 1
        n = swarm.node_join((), NODE_UL_SLOT, NODE_UL_SLOT) if not master else \
            swarm.node_join(swarm.all_hashes, 0, SEEDER_UL_SLOTS, master_node=True)
        speed_fact = 1.0 if master else (SLOWDOWN_FACTOR if (random.random() < SLOWDOWN_PROBABILITY) else
                                         random.uniform(1, SPEED_VARIABILITY_PER_NODE))
        n.client = SimpleNamespace(simu_tfers=set(), simu_speed_fact=speed_fact)
        n.name = 'N%02d' % next_node_num
        next_node_num += 1
        assert n.client.simu_speed_fact >= 1
        if master:
            unk = n.add_hashes(swarm.all_hashes)
            assert(not unk)
        return n

    seeder = new_simu_node(master=True)
    for _ in range(int(N_NODES/2)):
        new_simu_node()

    def report_transfers(node):
        dls = [t for t in node.client.simu_tfers if t.to_node is node]
        uls = [t for t in node.client.simu_tfers if t.from_node is node]
        node.set_active_transfers((t.hash for t in dls), len(dls), len(uls))

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
                print(f"Slow download. Giving up. (from {t.from_node.name})")
                return

            if random.random() < ERROR_PROBABILITY / 2: return  # simulate transfer failure sometimes

            # Mark hash as received
            unk = t.to_node.add_hashes([t.hash])
            assert(not unk)
        finally:
            if rnd_time is not None:
                t.from_node.update_transfer_speed([rnd_time])
            # Cleanup
            for n in (t.to_node, t.from_node):
                n.client.simu_tfers.discard(t)
                report_transfers(n)
                plan_now_trigger.set()


    async def planner_loop():
        while not swarm.all_done:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(plan_now_trigger.wait(), timeout=1)
            plan_now_trigger.clear()

            if random.random() < JOIN_PROBABILITY and joins_left > 0:
                n = new_simu_node()
                print("Node join: " + n.name)

            for t in swarm.plan_transfers():
                # Simulate node dropout
                if random.random() < DROPOUT_PROBABILITY:
                    print("DROPOUT " + str(t.to_node.name))
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
                print(n.name, ''.join(('#' if c in n.hashes else '.') for c in seeder.hashes), len(dls),
                      len(uls), '  %.1f' % (n.avg_ul_time or -1))

        start_t = time.time()
        asyncio.create_task(planner_loop())
        while not swarm.all_done:
            print_status()
            await asyncio.sleep(0.5)
        print_status()

        best_t = (TRANSFER_TIME_MIN+TRANSFER_TIME_MAX)/2 * N_HASHES / min(SEEDER_UL_SLOTS, NODE_UL_SLOT)
        print("ALL DONE. Efficiency vs. ideal multicast = %.1f%%" % (best_t / (time.time() - start_t) * 100))

    asyncio.run(runner())


if __name__ == "__main__":  # pragma: no cover
    simulate()
