from __future__ import annotations

from abc import ABC, abstractmethod
from threading import Lock
from typing import TypeVar, Generic, Optional, List, Callable

from forkjoinpy.global_counter import counter
from forkjoinpy.logger import get_logger


T = TypeVar("T")
LOGGER = get_logger()


class ForkJoinPoolTask(ABC, Generic[T]):
    def __init__(self):
        self.task_id = counter.get_new()

        self.result: Optional[T] = None
        self.done = False
        self.started = Lock()
        self._joining_list: List[ForkJoinPoolTask] = []

        self._add_task_to_pool: Optional[Callable[[ForkJoinPoolTask], None]] = None

    def set_add_task_to_pool_fn(
        self, add_task_to_pool: Callable[[ForkJoinPoolTask], None]
    ) -> None:
        self._add_task_to_pool = add_task_to_pool

    @abstractmethod
    def compute(self) -> T:
        pass

    def fork(self, *tasks: ForkJoinPoolTask) -> None:
        LOGGER.debug(f"Forking {[t.task_id for t in tasks]} inside of {self.task_id}")
        if self._add_task_to_pool is None:
            raise RuntimeError(
                "ForkJoinPoolTask has to be executed using "
                "ForkJoinPoolExecutor for correct forking behaviour"
            )

        for t in tasks:
            self._add_task_to_pool(t)

    def join(self, *tasks: ForkJoinPoolTask) -> None:
        self._joining_list = tasks
        self._join_internal()

    def _join_internal(self):
        # Iterate through tasks - if all joined - return
        # Otherwise pick the first not done and do it
        while True:
            done_tasks = 0
            for t in self._joining_list:
                if t.done:
                    done_tasks += 1
                    continue

                if t.started.acquire(blocking=False):
                    # Started lock wasn't acquired so let's work on it
                    res = t.compute()
                    t.result = res
                    t.done = True

                # Otherwise, since task is started but not finished see
                # if you can help with any child tasks
                t._join_internal()

            if done_tasks == len(self._joining_list):
                return


def release_potentially_free_lock(lock: Lock) -> None:
    try:
        lock.release()
    except RuntimeError:
        pass  # Turns out lock was free
