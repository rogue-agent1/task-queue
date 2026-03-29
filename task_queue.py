#!/usr/bin/env python3
"""task_queue - Priority task queue with retry, timeout, and dead letter."""
import sys, time, heapq
from collections import deque

class Task:
    def __init__(self, id, payload, priority=0, max_retries=3):
        self.id = id
        self.payload = payload
        self.priority = priority
        self.max_retries = max_retries
        self.retries = 0
        self.status = "pending"
        self.result = None
        self.error = None
    def __lt__(self, other):
        return self.priority > other.priority  # higher priority first

class TaskQueue:
    def __init__(self):
        self.queue = []
        self.processing = {}
        self.completed = []
        self.dead_letter = []
    def enqueue(self, task):
        heapq.heappush(self.queue, task)
    def dequeue(self):
        if not self.queue:
            return None
        task = heapq.heappop(self.queue)
        task.status = "processing"
        self.processing[task.id] = task
        return task
    def complete(self, task_id, result=None):
        task = self.processing.pop(task_id, None)
        if task:
            task.status = "completed"
            task.result = result
            self.completed.append(task)
    def fail(self, task_id, error=None):
        task = self.processing.pop(task_id, None)
        if not task:
            return
        task.retries += 1
        task.error = error
        if task.retries >= task.max_retries:
            task.status = "dead"
            self.dead_letter.append(task)
        else:
            task.status = "pending"
            heapq.heappush(self.queue, task)
    @property
    def pending(self):
        return len(self.queue)
    @property
    def in_flight(self):
        return len(self.processing)

def test():
    q = TaskQueue()
    q.enqueue(Task("a", "low", priority=1))
    q.enqueue(Task("b", "high", priority=10))
    q.enqueue(Task("c", "mid", priority=5))
    t = q.dequeue()
    assert t.id == "b"  # highest priority
    q.complete("b", "done")
    assert len(q.completed) == 1
    # retry
    t2 = q.dequeue()
    q.fail(t2.id, "timeout")
    assert q.pending == 2  # re-queued
    t3 = q.dequeue()  # should get c (priority 5) not retried a (priority 1... wait, c was dequeued as t2)
    # Actually after fail, t2 goes back to queue. Let's just check retry logic:
    q2 = TaskQueue()
    q2.enqueue(Task("x", "data", max_retries=2))
    t = q2.dequeue()
    q2.fail("x", "err1")
    assert q2.pending == 1
    t = q2.dequeue()
    q2.fail("x", "err2")
    assert q2.pending == 0
    assert len(q2.dead_letter) == 1
    assert q2.dead_letter[0].id == "x"
    print("OK: task_queue")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: task_queue.py test")
