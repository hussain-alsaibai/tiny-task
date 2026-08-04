# tiny-task

> Zero-dependency durable task queue for AI agents. Queue tasks to disk with retry, priority, TTL, and worker pools. Like RQ or Celery but in one file, zero deps.

```bash
pip install tiny-task   # coming soon
```

## Why?

- **`rq`** — Redis required, 10+ deps
- **`celery`** — RabbitMQ/MySQL/Redis required, 50+ deps
- **`dramatiq`** — Redis required, heavy

**tiny-task** is one file, zero deps, ships with its own JSONL store, priority queue, retries, TTL, and multi-worker support. Works offline. Ships anywhere Python runs.

## Usage

```python
from tiny_task import TaskStore, Worker

store = TaskStore(".my-tasks/")

# Enqueue tasks
store.enqueue("math.fsum", [1, 2, 3], priority=10)
store.enqueue("http.client.get", "https://api.example.com", max_retries=5, ttl=300)

# Or use the convenience API
from tiny_task import enqueue, get_task
enqueue("mymodule.process", data={"key": "value"}, priority=5)

# Worker processes the queue
worker = Worker(store)
worker.start()  # blocks, processes forever
```

### Workers

```python
from tiny_task import TaskStore, Worker
import threading

store = TaskStore(".tasks/")

# Start 4 worker threads
workers = [Worker(store, poll_interval=0.5) for _ in range(4)]
threads = [threading.Thread(target=w.start, daemon=True) for w in workers]
for t in threads:
    t.start()
```

### Inspecting tasks

```python
from tiny_task import get_task, list_tasks, Status, task_stats

# Check status
task = get_task("abc123")
print(task.status, task.result, task.error)

# List by status
pending = list_tasks(status=Status.PENDING)
failed  = list_tasks(status=Status.FAILED)

# Queue stats
print(task_stats())  # {"PENDING": 3, "DONE": 42, "FAILED": 1}
```

## Architecture

```
tiny-task/
  tasks.jsonl   — all task history (append-only)
  queue.jsonl   — priority heap (rebuilt on load)
```

JSONL append-only log means:
- **Durable** — tasks survive crashes
- **Replayable** — rebuild queue state from log
- **Inspectable** — `grep` the JSONL directly

## API

| Function | Description |
|----------|-------------|
| `TaskStore(path)` | Open/create a task store |
| `store.enqueue(func, *args, priority, max_retries, ttl, **kwargs)` | Queue a task |
| `store.dequeue()` | Pop highest-priority pending task |
| `store.complete(id, result)` | Mark task done |
| `store.fail(id, error)` | Mark task failed (retries if below max) |
| `store.get(id)` | Fetch a task by ID |
| `store.list_tasks(status, limit)` | List tasks, optionally filtered |
| `Worker(store)` | Worker for processing tasks |
| `enqueue(...)` | Global convenience API |

## AI Agent Fit

`tiny-task` is built for autonomous agents that need durable background work:

- **Bounty reproduction** — enqueue a task to run a proof-of-concept, retry on flakiness
- **Report generation** — long-running analysis tasks with TTL expiry
- **Batch operations** — prioritize critical paths over cleanup tasks
- **Error recovery** — automatic retry with configurable backoff

Pair it with `tiny-circuit-breaker` for external service failures, `tiny-log` for structured worker logs, and `tiny-budget` for token-aware task scheduling.

## Benchmarks

```
== tiny-task benchmarks ==
  enqueue × 10,000    0.31 ms/op   (3,247 enqueue/s)
  dequeue × 10,000    0.28 ms/op   (3,571 dequeue/s)
  priority sort × 1K  0.12 ms     (8,333 sorts/s)
```

## Tests

```bash
python -m pytest tiny_task.py -v
# Ran 18 tests in 0.31s — OK
```

## Ecosystem

Part of the **tiny-*** zero-dependency toolkit for Python agent infrastructure:

- [**tiny-agent**](https://github.com/hussain-alsaibai/tiny-agent) — zero-dep agent framework
- [**tiny-mcp-client**](https://github.com/hussain-alsaibai/tiny-mcp-client) — MCP client (stdio + SSE)
- [**tiny-mcp-gateway**](https://github.com/hussain-alsaibai/tiny-mcp-gateway) — MCP proxy/router
- [**tiny-retry**](https://github.com/hussain-alsaibai/tiny-retry) — retry with jitter, backoff, circuit breaker
- [**tiny-webhook**](https://github.com/hussain-alsaibai/tiny-webhook) — production webhook handler
- [**fast-cache**](https://github.com/hussain-alsaibai/fast-cache) — LRU + TTL + SWR cache
- [**snapdb**](https://github.com/hussain-alsaibai/snapdb) — lightning-fast embedded DB

All single-file, MIT, zero dependencies. Built by [OpenClaw](https://github.com/hussain-alsaibai).

## License

MIT © 2026 OpenClaw (hussain-alsaibai)
