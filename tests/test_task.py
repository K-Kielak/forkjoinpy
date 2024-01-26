import pytest

from fork_join_pool.task import ForkJoinPoolTask


# Create simple fibonacci task for testing
class FibonacciTask(ForkJoinPoolTask[int]):
    def __init__(self, n: int):
        super().__init__()
        self.n = n
        print(f"Created {self.task_id} (Fibonnaci {n})")

    def compute(self) -> int:
        assert self.n >= 1
        if self.n <= 2:
            return 1

        f1 = FibonacciTask(self.n - 1)
        f2 = FibonacciTask(self.n - 2)
        self.fork(f1, f2)
        f1_res = f1.join()
        f2_res = f2.join()
        return f1_res + f2_res


@pytest.mark.parametrize(
    "n, expected_res", [(1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 8)]
)
@pytest.mark.timeout(0.1)
def test_forkJoinPoolTask_fibonacci(n, expected_res):
    res = FibonacciTask(n).compute()
    assert res == expected_res
