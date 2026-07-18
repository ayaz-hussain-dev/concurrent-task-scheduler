from __future__ import annotations

import asyncio
import time
import unittest

from task_scheduler import CycleError, TaskContext, TaskScheduler, TaskSpec, TaskStatus


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_dependencies_and_result_context(self) -> None:
        events: list[str] = []

        async def first(_: TaskContext) -> int:
            events.append("first")
            return 20

        def second(context: TaskContext) -> int:
            events.append("second")
            return context.dependency_results["first"].value + 22

        results = await TaskScheduler(
            [TaskSpec("first", first), TaskSpec("second", second, dependencies=("first",))]
        ).run()

        self.assertEqual(events, ["first", "second"])
        self.assertEqual(results["second"].value, 42)
        self.assertEqual(results["second"].status, TaskStatus.SUCCESS)

    async def test_worker_limit_allows_concurrency(self) -> None:
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def work(_: TaskContext) -> None:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.08)
            async with lock:
                active -= 1

        tasks = [TaskSpec(f"task-{index}", work) for index in range(4)]
        started = time.perf_counter()
        await TaskScheduler(tasks, max_workers=2).run()
        elapsed = time.perf_counter() - started

        self.assertEqual(peak, 2)
        self.assertLess(elapsed, 0.28)

    async def test_retry_then_success(self) -> None:
        calls = 0

        async def flaky(_: TaskContext) -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("temporary")
            return "ok"

        result = (await TaskScheduler([TaskSpec("flaky", flaky, retries=2)]).run())["flaky"]
        self.assertEqual(result.status, TaskStatus.SUCCESS)
        self.assertEqual(result.attempts, 3)
        self.assertEqual(result.value, "ok")

    async def test_timeout_and_downstream_skip(self) -> None:
        async def slow(_: TaskContext) -> None:
            await asyncio.sleep(0.2)

        results = await TaskScheduler(
            [
                TaskSpec("slow", slow, timeout=0.02),
                TaskSpec("after", lambda _: 1, dependencies=("slow",)),
            ]
        ).run()

        self.assertEqual(results["slow"].status, TaskStatus.FAILED)
        self.assertIsInstance(results["slow"].error, TimeoutError)
        self.assertEqual(results["after"].status, TaskStatus.SKIPPED)
        self.assertEqual(results["after"].skipped_because, ("slow",))

    async def test_cycle_detection(self) -> None:
        with self.assertRaises(CycleError):
            TaskScheduler(
                [
                    TaskSpec("a", lambda _: None, dependencies=("b",)),
                    TaskSpec("b", lambda _: None, dependencies=("a",)),
                ]
            )


if __name__ == "__main__":
    unittest.main()
