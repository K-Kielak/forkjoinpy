from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from threading import Lock
from typing import TypeVar, Generic, Deque, Tuple, Optional

from fork_join_pool.global_counter import counter


# TODO implement some join method that takes multiple forks and if any is not finished
# TODO yet it will focus on processing that one


T = TypeVar("T")


class ForkJoinPoolTask(ABC, Generic[T]):
    def __init__(self):
        self._id = counter.get_new()

        self._todo_deque: Deque[T] = deque()
        self._deque_lock = Lock()

        self._result_lock = Lock()
        self._result: Optional[T] = None
        self._done = False

    @abstractmethod
    def compute(self) -> T:
        pass

    def fork(self, *tasks: Tuple[ForkJoinPoolTask, ...]) -> None:
        with self._deque_lock:
            self._todo_deque.extend(tasks)

    def join(self) -> T:
        with self._result_lock:
            if not self._done:
                self._result = self.compute()
                self._done = True

        return self._result


def release_potentially_free_lock(lock: Lock) -> None:
    try:
        lock.release()
    except RuntimeError:
        pass  # Turns out lock was free


