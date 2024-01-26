import time
from threading import Lock

import pytest

from forkjoinpy.executor import ForkJoinPoolExecutor
from forkjoinpy.task import ForkJoinPoolTask, T

COUNTER_INCREMENTS = 100
SLEEP_TIME = 0.001


@pytest.mark.timeout(1)
def test_forkJoinPoolExecutor_synchronizedCounterNoForks_correctCount() -> None:
    count = 0
    count_lock = Lock()

    class IncCounter(ForkJoinPoolTask[None]):
        def compute(self) -> None:
            nonlocal count, count_lock
            with count_lock:
                curr_count = count
                time.sleep(SLEEP_TIME)
                count = curr_count + 1

    with ForkJoinPoolExecutor(n_workers=10) as executor:
        for i in range(COUNTER_INCREMENTS):
            executor.add_task(IncCounter())

    assert count == COUNTER_INCREMENTS


@pytest.mark.timeout(1)
def test_forkJoinPoolExecutor_unsynchronizedCounterNoForks_incorrectCount() -> None:
    count = 0

    class IncCounter(ForkJoinPoolTask[None]):
        def compute(self) -> None:
            nonlocal count
            curr_count = count
            time.sleep(SLEEP_TIME)
            count = curr_count + 1

    with ForkJoinPoolExecutor(n_workers=10) as executor:
        for i in range(COUNTER_INCREMENTS):
            executor.add_task(IncCounter())

    assert count != COUNTER_INCREMENTS


class FibonacciTask(ForkJoinPoolTask[int]):
    def __init__(self, n: int):
        super().__init__()
        self.n = n
        self.task_id = n  # For cleaner logs

    def compute(self) -> int:
        assert self.n >= 1
        if self.n <= 2:
            return 1

        f1 = FibonacciTask(self.n - 1)
        f2 = FibonacciTask(self.n - 2)
        self.fork(f1, f2)
        self.join(f1, f2)
        return f1.result + f2.result


@pytest.mark.parametrize(
    "n, expected_res", [(1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 8), (13, 233)]
)
@pytest.mark.timeout(1)
def test_forkJoinPoolTask_fibonacciWithForks(n, expected_res):
    task = FibonacciTask(n)
    with ForkJoinPoolExecutor(n_workers=5) as executor:
        executor.add_task(task)

    assert task.result == expected_res
