import time
from threading import Lock

import pytest

from fork_join_pool.thread_pool_executor import ThreadPoolExecutor


N_INCREMENTS = 100
SLEEP_TIME = 0.001


@pytest.mark.timeout(1)
def test_threadPoolExecutor_synchronizedCounter_correctCount() -> None:
    count = 0
    count_lock = Lock()

    def increment_counter() -> None:
        nonlocal count, count_lock
        with count_lock:
            curr_count = count
            time.sleep(SLEEP_TIME)
            count = curr_count + 1

    with ThreadPoolExecutor(n_workers=10) as executor:
        for i in range(N_INCREMENTS):
            executor.add_task(increment_counter)

    assert count == N_INCREMENTS


@pytest.mark.timeout(1)
def test_threadPoolExecutor_unsynchronizedCounter_incorrectCount() -> None:
    count = 0

    def increment_counter() -> None:
        nonlocal count
        curr_count = count
        time.sleep(SLEEP_TIME)
        count = curr_count + 1

    with ThreadPoolExecutor(n_workers=10) as executor:
        for i in range(N_INCREMENTS):
            executor.add_task(increment_counter)

    assert count != N_INCREMENTS
