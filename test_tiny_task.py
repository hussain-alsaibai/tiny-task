"""Tests for tiny-task."""
import os, shutil, tempfile, math, time
import unittest
from tiny_task import (
    TaskStore, Worker, Task, Status,
    enqueue, get_task, list_tasks, cancel_task, task_stats,
)

def dummy_add(a, b):
    return a + b

def dummy_fail():
    raise ValueError("always fail")

class TestTaskStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = TaskStore(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_enqueue_returns_id(self):
        tid = self.store.enqueue("math.fsum", [1, 2, 3])
        self.assertIsInstance(tid, str)
        self.assertEqual(len(tid), 12)

    def test_enqueue_and_dequeue(self):
        self.store.enqueue("math.fsum", [1, 2, 3])
        task = self.store.dequeue()
        self.assertIsNotNone(task)
        self.assertEqual(task.func, "math.fsum")
        self.assertEqual(task.status, Status.RUNNING)

    def test_priority_order(self):
        self.store.enqueue("math.fsum", [1], priority=1)
        self.store.enqueue("math.fsum", [2], priority=10)
        t1 = self.store.dequeue()
        t2 = self.store.dequeue()
        self.assertEqual(t1.priority, 10)
        self.assertEqual(t2.priority, 1)

    def test_complete(self):
        tid = self.store.enqueue("math.fsum", [1, 2, 3])
        task = self.store.dequeue()
        self.store.complete(task.id, 6.0)
        result = self.store.get(task.id)
        self.assertEqual(result.status, Status.DONE)
        self.assertEqual(result.result, 6.0)

    def test_fail_with_retry(self):
        tid = self.store.enqueue("math.fsum", [1], max_retries=2)
        task = self.store.dequeue()
        self.store.fail(task.id, "network error")
        t2 = self.store.get(task.id)
        self.assertEqual(t2.retries, 1)
        self.assertEqual(t2.status, Status.PENDING)

    def test_fail_exhausted_retries(self):
        tid = self.store.enqueue("math.fsum", [1], max_retries=1)
        task = self.store.dequeue()
        self.store.fail(task.id, "fatal")
        t2 = self.store.get(task.id)
        self.assertEqual(t2.status, Status.FAILED)
        self.assertEqual(t2.error, "fatal")

    def test_cancel(self):
        tid = self.store.enqueue("math.fsum", [1])
        self.store.cancel(tid)
        t = self.store.get(tid)
        self.assertEqual(t.status, Status.CANCELLED)

    def test_ttl_expiry(self):
        tid = self.store.enqueue("math.fsum", [1], ttl=0.001)
        time.sleep(0.01)
        task = self.store.dequeue()
        self.assertIsNone(task)

    def test_stats(self):
        self.store.enqueue("math.fsum", [1])
        self.store.enqueue("math.fsum", [1])
        t = self.store.dequeue()
        self.store.complete(t.id, 1)
        s = self.store.stats()
        self.assertIn("DONE", s)
        self.assertIn("PENDING", s)

    def test_list_tasks_filtered(self):
        self.store.enqueue("math.fsum", [1])
        t = self.store.dequeue()
        self.store.complete(t.id, 1)
        done = self.store.list_tasks(status=Status.DONE)
        pending = self.store.list_tasks(status=Status.PENDING)
        self.assertEqual(len(done), 1)
        self.assertEqual(len(pending), 0)


class TestGlobalAPI(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_global_enqueue_and_get(self):
        from tiny_task import _default
        store = TaskStore(self.tmp)
        # monkey-patch for test
        import tiny_task
        tiny_task._default_store = store
        try:
            tid = enqueue("math.fsum", [1, 2, 3])
            t = get_task(tid)
            self.assertIsNotNone(t)
        finally:
            tiny_task._default_store = None


if __name__ == "__main__":
    unittest.main()
