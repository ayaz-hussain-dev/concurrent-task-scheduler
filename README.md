# Concurrent Task Scheduler (Python)

A dependency-aware task scheduler built with `asyncio`. It runs independent tasks concurrently while respecting task dependencies, a fixed worker limit and deterministic ready-queue ordering.

## Features

- Directed acyclic graph dependency validation
- Cycle and unknown-dependency detection
- Configurable maximum worker count
- Synchronous and asynchronous task functions
- Per-task retries and timeouts
- Structured logging
- Downstream task skipping after dependency failure
- Read-only access to dependency results
- Tests covering concurrency, order, retries, timeouts and failures

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\\Scripts\\activate
python -m pip install -e .
python -m task_scheduler
python -m unittest discover -s tests -v
```

Run the fuller example:

```bash
python examples/demo.py
```

## Usage

```python
import asyncio
from task_scheduler import TaskContext, TaskScheduler, TaskSpec

async def download(_: TaskContext) -> str:
    await asyncio.sleep(0.1)
    return "data.csv"

def process(context: TaskContext) -> str:
    filename = context.dependency_results["download"].value
    return f"processed {filename}"

async def main() -> None:
    scheduler = TaskScheduler(
        [
            TaskSpec("download", download, retries=2, timeout=2.0),
            TaskSpec("process", process, dependencies=("download",)),
        ],
        max_workers=4,
    )
    results = await scheduler.run()
    print(results["process"].value)

asyncio.run(main())
```

## Scheduling behaviour

Tasks enter a heap-backed ready queue when every dependency has completed. Tasks defined earlier win ties, making execution repeatable while still allowing independent work to overlap. A failed or timed-out task causes dependent tasks to be marked `skipped` rather than run with incomplete inputs.
