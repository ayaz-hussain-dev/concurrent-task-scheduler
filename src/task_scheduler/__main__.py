from __future__ import annotations

import asyncio
import logging

from .models import TaskContext, TaskSpec
from .scheduler import TaskScheduler


async def delayed_value(value: str, delay: float) -> str:
    await asyncio.sleep(delay)
    return value


async def main_async() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    tasks = [
        TaskSpec("download-users", lambda _: delayed_value("users.csv", 0.20)),
        TaskSpec("download-orders", lambda _: delayed_value("orders.csv", 0.20)),
        TaskSpec(
            "build-report",
            lambda context: {
                name: result.value for name, result in context.dependency_results.items()
            },
            dependencies=("download-users", "download-orders"),
        ),
    ]

    results = await TaskScheduler(tasks, max_workers=2).run()
    for name, result in results.items():
        print(f"{name:16} {result.status.value:8} attempts={result.attempts} value={result.value!r}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
