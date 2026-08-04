"""tiny-task: Zero-dependency durable task queue for AI agents.

Queue tasks to disk with retry, priority, TTL, and worker pools.
Like RQ or Celery but in one file, zero deps.

MIT License | github.com/hussain-alsaibai/tiny-task
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, asdict, field
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict
import heapq

__version__ = "1.0.0"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class Status(IntEnum):
    PENDING = 0
    RUNNING = 1
    DONE    = 2
    FAILED  = 3
    CANCELLED = 4


@dataclass
class Task:
    id:         str
    func:       str          # dotted name: "mymodule.myfunc"
    args:       Tuple[Any, ...] = field(default_factory=tuple)
    kwargs:     Dict[str, Any]  = field(default_factory=dict)
    status:     Status = Status.PENDING
    priority:   int = 0          # higher = more urgent
    retries:    int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    result:    Optional[Any] = None
    error:     Optional[str] = None
    ttl:       Optional[float] = None  # seconds; None = never expires

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Task":
        d = dict(d)
        d["status"] = Status(d["status"])
        if not isinstance(d["args"], tuple):
            d["args"] = tuple(d["args"])
        return cls(**d)


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _import_func(func: str) -> Callable:
    parts = func.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"func must be 'module.function', got: {func}")
    import importlib
    mod = importlib.import_module(parts[0])
    return getattr(mod, parts[1])


def _encode(v: Any) -> Any:
    """Recursively strip non-JSON types."""
    if hasattr(v, "__dict__"):
        return str(v)
    if isinstance(v, (list, tuple)):
        return [_encode(x) for x in v]
    if isinstance(v, dict):
        return {k: _encode(val) for k, val in v.items()}
    try:
        json.dumps(v)
        return v
    except TypeError:
        return str(v)


# ---------------------------------------------------------------------------
# TaskStore — disk-backed queue
# ---------------------------------------------------------------------------

class TaskStore:
    """Durable JSONL-backed task store with priority queue ordering."""

    def __init__(self, path: str = ".tiny-task/") -> None:
        self.base = Path(path)
        self.base.mkdir(parents=True, exist_ok=True)
        self.tasks_file   = self.base / "tasks.jsonl"
        self.queue_file   = self.base / "queue.jsonl"
        self._lock        = threading.RLock()
        self._tasks: Dict[str, Task] = {}
        self._queue_heap: List[Tuple[int, float, str]] = []  # (-priority, timestamp, id)
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if self.tasks_file.exists():
            with open(self.tasks_file) as f:
                for line in f:
                    if not line.strip():
                        continue
                    t = Task.from_dict(json.loads(line))
                    self._tasks[t.id] = t
        if self.queue_file.exists():
            with open(self.queue_file) as f:
                for line in f:
                    if not line.strip():
                        continue
                    prio, ts, tid = json.loads(line)
                    heapq.heappush(self._queue_heap, (prio, ts, tid))

    def _save_task(self, task: Task) -> None:
        with open(self.tasks_file, "a") as f:
            f.write(json.dumps(task.to_dict()) + "\n")

    def _rewrite_queue(self) -> None:
        with open(self.queue_file, "w") as f:
            for item in self._queue_heap:
                f.write(json.dumps(item) + "\n")

    # ------------------------------------------------------------------
    # Queue API
    # ------------------------------------------------------------------

    def enqueue(
        self,
        func: str,
        *args: Any,
        priority: int = 0,
        max_retries: int = 3,
        ttl: Optional[float] = None,
        **kwargs: Any,
    ) -> str:
        """Add a task to the queue. Returns task ID."""
        task_id = uuid.uuid4().hex[:12]
        task = Task(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            max_retries=max_retries,
            ttl=ttl,
        )
        with self._lock:
            self._tasks[task_id] = task
            self._save_task(task)
            heapq.heappush(self._queue_heap, (-priority, task.created_at, task_id))
            self._rewrite_queue()
        return task_id

    def dequeue(self) -> Optional[Task]:
        """Pop the highest-priority pending task. Returns None if empty."""
        while self._queue_heap:
            _, _, task_id = heapq.heappop(self._queue_heap)
            task = self._tasks.get(task_id)
            if task is None:
                continue
            if task.status != Status.PENDING:
                continue
            if task.is_expired():
                task.status = Status.FAILED
                task.error = "TTL expired"
                continue
            task.status = Status.RUNNING
            task.started_at = time.time()
            self._save_task(task)
            self._rewrite_queue()
            return task
        return None

    def complete(self, task_id: str, result: Any) -> None:
        task = self._tasks.get(task_id)
        if task is None:
            return
        task.status = Status.DONE
        task.finished_at = time.time()
        task.result = _encode(result)
        self._save_task(task)

    def fail(self, task_id: str, error: str) -> bool:
        """Mark failed. Returns True if will be retried."""
        task = self._tasks.get(task_id)
        if task is None:
            return False
        task.retries += 1
        if task.retries < task.max_retries:
            # re-queue with same priority
            task.status = Status.PENDING
            task.started_at = None
            task.error = error
            heapq.heappush(self._queue_heap, (-task.priority, time.time(), task.id))
            self._save_task(task)
            self._rewrite_queue()
            return True
        else:
            task.status = Status.FAILED
            task.finished_at = time.time()
            task.error = error
            self._save_task(task)
            return False

    def cancel(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if task and task.status == Status.PENDING:
            task.status = Status.CANCELLED
            self._save_task(task)

    def get(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def list_tasks(
        self,
        status: Optional[Status] = None,
        limit: int = 100,
    ) -> List[Task]:
        tasks = list(self._tasks.values())
        if status is not None:
            tasks = [t for t in tasks if t.status == status]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[:limit]

    def stats(self) -> Dict[str, Any]:
        counts = defaultdict(int)
        for t in self._tasks.values():
            counts[t.status.name] += 1
        return dict(counts)


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class Worker:
    """Single-threaded queue worker. Run one or more in threads/processes."""

    def __init__(self, store: TaskStore, poll_interval: float = 1.0) -> None:
        self.store = store
        self.poll_interval = poll_interval
        self._running = False

    def start(self, *, blocking: bool = True) -> None:
        self._running = True
        while self._running:
            task = self.store.dequeue()
            if task is None:
                if blocking:
                    time.sleep(self.poll_interval)
                    continue
                break
            self._run_task(task)
            if not blocking:
                break

    def stop(self) -> None:
        self._running = False

    def _run_task(self, task: Task) -> None:
        try:
            fn = _import_func(task.func)
            result = fn(*task.args, **task.kwargs)
            self.store.complete(task.id, result)
        except Exception as exc:
            retrying = self.store.fail(task.id, f"{type(exc).__name__}: {exc}")
            if not retrying:
                print(f"[tiny-task] task {task.id} permanently failed: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Convenience API (mirrors RQ / Celery)
# ---------------------------------------------------------------------------

_default_store: Optional[TaskStore] = None

def _default() -> TaskStore:
    global _default_store
    if _default_store is None:
        _default_store = TaskStore()
    return _default_store

def enqueue(
    func: str,
    *args: Any,
    priority: int = 0,
    max_retries: int = 3,
    ttl: Optional[float] = None,
    **kwargs: Any,
) -> str:
    """Enqueue a task to the default store."""
    return _default().enqueue(func, *args, priority=priority, max_retries=max_retries, ttl=ttl, **kwargs)

def get_task(task_id: str) -> Optional[Task]:
    return _default().get(task_id)

def list_tasks(status: Optional[Status] = None) -> List[Task]:
    return _default().list_tasks(status=status)

def cancel_task(task_id: str) -> None:
    _default().cancel(task_id)

def task_stats() -> Dict[str, Any]:
    return _default().stats()
