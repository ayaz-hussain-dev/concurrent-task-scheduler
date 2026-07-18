from __future__ import annotations

import asyncio
import logging

from task_scheduler import TaskContext, TaskScheduler, TaskSpec


async def fetch_users(_: TaskContext) -> list[str]:
    await asyncio.sleep(0.2)
    return ["Aisha", "Ben", "Chen"]


async def fetch_orders(_: TaskContext) -> list[int]:
    await asyncio.sleep(0.2)
    return [120, 80, 45]


def build_summary(context: TaskContext) -> dict[str, int]:
    users = context.dependency_results["fetch-users"].value
    orders = context.dependency_results["fetch-orders"].value
    return {"users": len(users), "order_total": sum(orders)}


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scheduler = TaskScheduler(
        [
            TaskSpec("fetch-users", fetch_users, timeout=1.0),
            TaskSpec("fetch-orders", fetch_orders, timeout=1.0),
            TaskSpec(
                "build-summary",
                build_summary,
                dependencies=("fetch-users", "fetch-orders"),
            ),
        ],
        max_workers=2,
    )
    results = await scheduler.run()
    print(results["build-summary"].value)


if __name__ == "__main__":
    asyncio.run(main())
