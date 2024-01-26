from __future__ import annotations

from collections import deque
from threading import Lock, Semaphore, Thread
from types import TracebackType
from typing import Callable, Any, Optional, Type, Deque, Tuple

from fork_join_pool.logger import get_logger

logger = get_logger()


AnyFunction = Callable[[Any, ...], Any]
AnyArgs = Tuple[Any, ...]


class ThreadPoolExecutor:
    def __init__(self, n_workers):
        self.n_workers = n_workers

        self._task_queue: Deque[Tuple[AnyFunction, AnyArgs]] = deque()
        self._tasks_lock = Lock()
        self._queue_elements = Semaphore()
        self._threads = []

    def __enter__(self) -> ThreadPoolExecutor:
        self._threads = [
            Thread(target=self._run_worker, args=[i]) for i in range(self.n_workers)
        ]
        for t in self._threads:
            t.start()

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        for t in self._threads:
            t.join()

        return False

    def add_task(self, task: AnyFunction, *args: AnyArgs) -> None:
        with self._tasks_lock:
            self._task_queue.append((task, args))
            self._queue_elements.release()

    def _run_worker(self, task_id: int) -> None:
        logger.info(f"Started thread pool worker with id: {task_id}")
        self._queue_elements.acquire()
        with self._tasks_lock:
            task, args = self._task_queue.popleft()

        logger.debug(f"Thread pool worker {task_id} starts new task")
        task(*args)
