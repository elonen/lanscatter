import pytest, time, asyncio
from lanscatter import ratelimiter


@pytest.mark.parametrize("rate, period", [(3, 2), (3, 0.0001), (0.1, 200)])
def test_initial_acquires(rate, period):
    assert ratelimiter.RateLimiter(rate, period).try_acquire(rate/2) is None
    assert ratelimiter.RateLimiter(rate, period, slow_start=False).try_acquire(rate/2) == rate/2
    assert ratelimiter.RateLimiter(rate, period, slow_start=False).try_acquire(rate) == rate
    with pytest.raises(ValueError):
        ratelimiter.RateLimiter(rate, period).try_acquire(rate+1)


def test_burst():
    assert ratelimiter.RateLimiter(3, 2, burst_factor=2).try_acquire(6) is None
    assert ratelimiter.RateLimiter(3, 2, burst_factor=2, slow_start=False).try_acquire(6) == 6.0
    with pytest.raises(ValueError):
        ratelimiter.RateLimiter(3, 2, burst_factor=2, slow_start=False).try_acquire(7)

def test_min():
    assert ratelimiter.RateLimiter(3, 2, slow_start=False).try_acquire(6, 3) == 3
    with pytest.raises(ValueError):
        assert ratelimiter.RateLimiter(3, 2, slow_start=False).try_acquire(6, 4)

def test_inf():
    assert ratelimiter.RateLimiter(1, 2, burst_factor=float('inf'), slow_start=False).try_acquire(99999) == 99999
    assert ratelimiter.RateLimiter(1, 2, burst_factor=float('inf')).try_acquire(99999) is None

def test_zero():
    assert ratelimiter.RateLimiter(0, 2, slow_start=False).try_acquire(0) == 0
    assert ratelimiter.RateLimiter(0, 2).try_acquire(0) == 0

    assert ratelimiter.RateLimiter(1, 0).try_acquire(1000) == 1000

    with pytest.raises(ValueError):
        ratelimiter.RateLimiter(0, 2).try_acquire(0.00001)


@pytest.mark.parametrize("rate, period, burst", [(4, 1, 1), (0.5, 1, 2)])
def test_wait(rate, period, burst):

    async def test(mul):
        start_time = time.time()
        limiter = ratelimiter.RateLimiter(rate, period=period, burst_factor=burst)
        await limiter.acquire(rate*mul)
        elapsed = time.time() - start_time
        assert abs(elapsed - period*mul) < 0.05

    asyncio.run(test(burst))
    asyncio.run(test(1))
    asyncio.run(test(0.5))
    with pytest.raises(ValueError):
        asyncio.run(test(burst*1.01))


def test_wait_burst():
    async def test():
        start_time = time.time()
        limiter = ratelimiter.RateLimiter(2, 1, 1.5)
        await limiter.acquire(2) # takes 1s
        await asyncio.sleep(1.5) # stores 3 tokens, takes 1.5s
        await limiter.acquire(3) # returns immediately
        await limiter.acquire(1) # takes 0.5s
        elapsed = time.time() - start_time
        assert abs(elapsed - 3.0) < 0.05

    asyncio.run(test())
