from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping, TypeAlias


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class TaskContext:
    """Read-only view of completed dependency results supplied to a task."""

    task_name: str
    dependency_results: Mapping[str, "TaskResult"]

    @classmethod
    def create(
        cls, task_name: str, dependency_results: Mapping[str, "TaskResult"]
    ) -> "TaskContext":
        return cls(task_name, MappingProxyType(dict(dependency_results)))


TaskFunction: TypeAlias = Callable[[TaskContext], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class TaskSpec:
    name: str
    function: TaskFunction
    dependencies: tuple[str, ...] = ()
    retries: int = 0
    timeout: float | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Task name cannot be empty")
        if self.retries < 0:
            raise ValueError("retries cannot be negative")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.name in self.dependencies:
            raise ValueError(f"Task {self.name!r} cannot depend on itself")
        if len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError(f"Task {self.name!r} has duplicate dependencies")


@dataclass(slots=True)
class TaskResult:
    name: str
    status: TaskStatus = TaskStatus.PENDING
    value: Any = None
    error: Exception | None = None
    attempts: int = 0
    started_at: float | None = None
    finished_at: float | None = None
    skipped_because: tuple[str, ...] = field(default_factory=tuple)

    @property
    def duration(self) -> float | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at
