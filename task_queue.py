#!/usr/bin/env python3
"""Priority task queue."""
import heapq, time

class Task:
    _counter = 0
    def __init__(self, name, priority=0, deadline=None, data=None):
        Task._counter += 1
        self.id = Task._counter
        self.name = name; self.priority = priority
        self.deadline = deadline; self.data = data
        self.created = time.monotonic()
        self.status = "pending"
    def __lt__(self, other):
        return (-self.priority, self.created) < (-other.priority, other.created)

class TaskQueue:
    def __init__(self):
        self._heap = []; self._tasks = {}

    def push(self, task):
        heapq.heappush(self._heap, task)
        self._tasks[task.id] = task
        return task

    def pop(self):
        while self._heap:
            task = heapq.heappop(self._heap)
            if task.status == "pending":
                task.status = "running"
                return task
        return None

    def peek(self):
        for task in self._heap:
            if task.status == "pending":
                return task
        return None

    def complete(self, task_id):
        if task_id in self._tasks:
            self._tasks[task_id].status = "done"

    def cancel(self, task_id):
        if task_id in self._tasks:
            self._tasks[task_id].status = "cancelled"

    def pending(self):
        return [t for t in self._tasks.values() if t.status == "pending"]

    def __len__(self):
        return len([t for t in self._tasks.values() if t.status == "pending"])

if __name__ == "__main__":
    q = TaskQueue()
    q.push(Task("low", priority=1))
    q.push(Task("high", priority=10))
    q.push(Task("med", priority=5))
    while len(q):
        t = q.pop()
        print(f"Processing: {t.name} (priority={t.priority})")
        q.complete(t.id)

def test():
    Task._counter = 0
    q = TaskQueue()
    q.push(Task("low", priority=1))
    q.push(Task("high", priority=10))
    q.push(Task("med", priority=5))
    assert len(q) == 3
    # Highest priority first
    t = q.pop()
    assert t.name == "high"
    assert t.status == "running"
    q.complete(t.id)
    t2 = q.pop()
    assert t2.name == "med"
    # Cancel
    q.push(Task("cancel_me", priority=100))
    q.cancel(4)
    t3 = q.pop()
    assert t3.name == "low"  # cancel_me was cancelled
    # Empty
    assert q.pop() is None
    print("  task_queue: ALL TESTS PASSED")
