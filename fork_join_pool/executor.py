from __future__ import annotations

from collections import deque
from threading import Lock, Semaphore, Thread
from types import TracebackType
from typing import Optional, Type, Deque

from fork_join_pool.logger import get_logger
from fork_join_pool.task import ForkJoinPoolTask

logger = get_logger()


class ForkJoinPoolExecutor:
    def __init__(self, n_workers):
        self.n_workers = n_workers

        self._task_queue: Deque[ForkJoinPoolTask] = deque()
        self._tasks_lock = Lock()
        self._queue_elements = Semaphore()
        self._no_more_tasks = Lock()
        self._threads = []

        self._is_active = False

    def __enter__(self) -> ForkJoinPoolExecutor:
        self._is_active = True
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
        self._no_more_tasks.acquire()

        self._is_active = False
        for _ in range(len(self._threads)):
            # Unlock all threads - since self._is_active is False they should
            # terminate instead of polling from the queue
            self._queue_elements.release()

        for t in self._threads:
            t.join()

        return False

    def add_task(self, task: ForkJoinPoolTask) -> None:
        with self._tasks_lock:
            if len(self._task_queue) == 0:
                self._no_more_tasks.acquire()

            task.set_add_task_to_pool_fn(self.add_task)
            self._task_queue.append(task)
            self._queue_elements.release()

    def _run_worker(self, task_id: int) -> None:
        logger.info(f"Started thread pool worker with id: {task_id}")
        while True:
            self._queue_elements.acquire()
            if not self._is_active:
                return

            with self._tasks_lock:
                task = self._task_queue.popleft()
                if len(self._task_queue) == 0:
                    self._no_more_tasks.release()

            if task.started.acquire(blocking=False):
                logger.debug(f"Thread pool worker {task_id} starts new task")
                task.result = task.compute()
                task.done = True