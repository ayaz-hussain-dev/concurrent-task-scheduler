from __future__ import annotations

import asyncio
import heapq
import inspect
import logging
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from .models import TaskContext, TaskResult, TaskSpec, TaskStatus


class SchedulerError(RuntimeError):
    pass


class CycleError(SchedulerError):
    pass


class TaskScheduler:
    """Runs dependency-aware tasks with a configurable worker limit."""

    def __init__(
        self,
        tasks: Iterable[TaskSpec],
        *,
        max_workers: int = 4,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")

        task_list = list(tasks)
        self._tasks: dict[str, TaskSpec] = {}
        self._order: dict[str, int] = {}
        for index, task in enumerate(task_list):
            if task.name in self._tasks:
                raise SchedulerError(f"Duplicate task name: {task.name}")
            self._tasks[task.name] = task
            self._order[task.name] = index

        self.max_workers = max_workers
        self.logger = logger or logging.getLogger("task_scheduler")
        self._validate_dependencies()
        self._validate_acyclic()

    def _validate_dependencies(self) -> None:
        for task in self._tasks.values():
            missing = [name for name in task.dependencies if name not in self._tasks]
            if missing:
                raise SchedulerError(
                    f"Task {task.name!r} has unknown dependencies: {', '.join(missing)}"
                )

    def _graph(self) -> tuple[dict[str, int], dict[str, list[str]]]:
        indegree = {name: len(task.dependencies) for name, task in self._tasks.items()}
        dependents: dict[str, list[str]] = defaultdict(list)
        for task in self._tasks.values():
            for dependency in task.dependencies:
                dependents[dependency].append(task.name)
        for children in dependents.values():
            children.sort(key=self._order.__getitem__)
        return indegree, dependents

    def _validate_acyclic(self) -> None:
        indegree, dependents = self._graph()
        ready = [self._order[name] for name, degree in indegree.items() if degree == 0]
        heapq.heapify(ready)
        names_by_index = {index: name for name, index in self._order.items()}
        visited = 0

        while ready:
            name = names_by_index[heapq.heappop(ready)]
            visited += 1
            for child in dependents.get(name, []):
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(ready, self._order[child])

        if visited != len(self._tasks):
            cyclic = sorted(name for name, degree in indegree.items() if degree > 0)
            raise CycleError("Dependency cycle detected involving: " + ", ".join(cyclic))

    async def _call(self, task: TaskSpec, context: TaskContext) -> Any:
        if inspect.iscoroutinefunction(task.function):
            return await task.function(context)

        result = await asyncio.to_thread(task.function, context)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _execute_one(
        self, task: TaskSpec, results: dict[str, TaskResult]
    ) -> TaskResult:
        loop = asyncio.get_running_loop()
        result = results[task.name]
        result.status = TaskStatus.RUNNING
        result.started_at = loop.time()
        context = TaskContext.create(
            task.name, {name: results[name] for name in task.dependencies}
        )

        for attempt in range(1, task.retries + 2):
            result.attempts = attempt
            try:
                self.logger.info("Starting task %s (attempt %d)", task.name, attempt)
                operation = self._call(task, context)
                if task.timeout is None:
                    result.value = await operation
                else:
                    result.value = await asyncio.wait_for(operation, timeout=task.timeout)
                result.status = TaskStatus.SUCCESS
                result.error = None
                self.logger.info("Completed task %s", task.name)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                result.error = exc
                if attempt <= task.retries:
                    self.logger.warning(
                        "Task %s failed on attempt %d: %s; retrying",
                        task.name,
                        attempt,
                        exc,
                    )
                else:
                    result.status = TaskStatus.FAILED
                    self.logger.error(
                        "Task %s failed after %d attempt(s): %s",
                        task.name,
                        attempt,
                        exc,
                    )

        result.finished_at = loop.time()
        return result

    async def run(self) -> dict[str, TaskResult]:
        """Run all tasks and return results in the original definition order."""

        if not self._tasks:
            return {}

        indegree, dependents = self._graph()
        ready: list[int] = [
            self._order[name] for name, degree in indegree.items() if degree == 0
        ]
        heapq.heapify(ready)
        names_by_index = {index: name for name, index in self._order.items()}
        results = {name: TaskResult(name=name) for name in self._tasks}
        running: dict[asyncio.Task[TaskResult], str] = {}

        def release_dependents(completed_name: str) -> None:
            for child in dependents.get(completed_name, []):
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(ready, self._order[child])

        while ready or running:
            while ready and len(running) < self.max_workers:
                name = names_by_index[heapq.heappop(ready)]
                spec = self._tasks[name]
                failed_dependencies = tuple(
                    dependency
                    for dependency in spec.dependencies
                    if results[dependency].status != TaskStatus.SUCCESS
                )
                if failed_dependencies:
                    skipped = results[name]
                    skipped.status = TaskStatus.SKIPPED
                    skipped.skipped_because = failed_dependencies
                    now = asyncio.get_running_loop().time()
                    skipped.started_at = now
                    skipped.finished_at = now
                    self.logger.warning(
                        "Skipping task %s because dependencies failed: %s",
                        name,
                        ", ".join(failed_dependencies),
                    )
                    release_dependents(name)
                    continue

                future = asyncio.create_task(self._execute_one(spec, results))
                running[future] = name

            if not running:
                continue

            completed, _ = await asyncio.wait(
                running.keys(), return_when=asyncio.FIRST_COMPLETED
            )
            for future in completed:
                name = running.pop(future)
                await future
                release_dependents(name)

        return {name: results[name] for name in self._tasks}
