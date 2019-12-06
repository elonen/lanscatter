import time
import asyncio


class RateLimiter(object):
    """
    Asyncio rate limiter that generates 'rate_limit' new permits per given 'period'.
    Some typical use cases are "Mbits per second" (period=1), and "API calls per minute" (period=60).

    Class user(s) "await acquire(...)" how many permits they'd like to consume.
    If rate limit has been reached, the call will then yield until at
    least 'n_min' permits become available.

    The class also tracks realized use rate.
    """

    def __init__(self, rate_limit: float, period: float = 1.0, burst_factor: float = 1.0, slow_start=True):
        """
        :param rate_limit: How many new permits per 'period' seconds to generate
        :param period: Permit generation period
        :param burst_factor: Allow bursts of max 'burst_factor' x 'rate_limit'.
        """
        assert(period > 0)
        assert(rate_limit >= 0)

        self.permits_per_sec = rate_limit / period
        self.period = period
        self.permit_cap = rate_limit * max(burst_factor, 1.0)

        self.cur_permits = 0
        self.last_refill = time.time() if slow_start else 0

        self.report_start_time = time.time()
        self.report_total_consumed = 0


    async def acquire(self, n_permits: float, n_min: float = -1):
        """
        Try to get n_permits. If that many are not
        available, wait until at least n_min permits are available.

        If n_min < 0, n_min = n_permits (i.e. wait until n_permits become available)
        If n_min == 0, return immediately (non-blocking)
        If n_min > n_permits, n_permits is used as a minimum

        :param n_permits: How many permits caller would like to have.
        :param n_min: Don't return until at least this many permits become available.
        :return: Number of permits acquired. Always at most n_permits, and at least n_min.
        """
        assert(n_permits >= 0)
        n_min = n_min if n_min >= 0 else n_permits
        n_min = min(n_min, n_permits)
        if n_min > self.permit_cap:
            raise ValueError(f"n_min ({n_min}) > max burst permits ({self.permit_cap}); call would block forever")

        while True:
            # Refill permits
            refill = (time.time() - self.last_refill) * self.permits_per_sec
            new_permits = self.cur_permits + refill
            if new_permits > self.cur_permits:  # avoid rounding to zero
                self.cur_permits = new_permits
                self.last_refill = time.time()
            self.cur_permits = min(self.cur_permits, self.permit_cap)

            # Block until we have enough
            if self.cur_permits >= n_min:
                break
            else:
                time_until_enough = (n_min - self.cur_permits) / self.permits_per_sec
                await asyncio.sleep(time_until_enough)

        used = max(min(n_permits, self.cur_permits), n_min)
        self.cur_permits -= used
        self.report_total_consumed += used
        return used


    def unspend(self, n_permits: float):
        """
        Return unused permits.
        :param n_permits: Number of permits to add back
        :return:
        """
        self.cur_permits = min(self.cur_permits + n_permits, self.permit_cap)
        self.report_total_consumed -= n_permits


    def report_rate(self):
        '''
        :return: Realized consume rate since beginning (or latest reset_reporting())
        '''
        return self.report_total_consumed / (time.time() - self.report_start_time) * self.period

    def reset_reporting(self):
        """
        Start report measuring from zero.
        """
        self.report_total_consumed = 0
        self.report_start_time = time.time()


# --------------------------------------------

def main():
    async def test():
        print("Printing 0-9, two times in a second")
        limiter = RateLimiter(2.0)
        cnt = 0
        while cnt < 10:
            await limiter.acquire(1)
            print(cnt)
            cnt += 1
            if cnt == 4:
                print("Short pause to test accumulation...")
                await asyncio.sleep(6)
    asyncio.run(test())


if __name__ == "__main__":
    main()
