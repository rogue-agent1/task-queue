#!/usr/bin/env python3
"""In-memory priority task queue with workers."""
import sys, heapq, time, json, threading
from datetime import datetime

class TaskQueue:
    def __init__(self):
        self.heap = []; self.counter = 0; self.done = []
    def push(self, task, priority=0):
        self.counter += 1
        entry = (priority, self.counter, task, time.time())
        heapq.heappush(self.heap, entry)
        return self.counter
    def pop(self):
        if not self.heap: return None
        priority, tid, task, created = heapq.heappop(self.heap)
        self.done.append({'id': tid, 'task': task, 'priority': priority,
                         'created': created, 'completed': time.time()})
        return tid, task, priority
    def peek(self): return self.heap[0] if self.heap else None
    def size(self): return len(self.heap)
    def stats(self):
        if not self.done: return "No completed tasks"
        latencies = [d['completed']-d['created'] for d in self.done]
        return f"Completed: {len(self.done)} | Pending: {self.size()} | Avg latency: {sum(latencies)/len(latencies):.3f}s"

if __name__ == '__main__':
    q = TaskQueue()
    tasks = [("send email", 2), ("process payment", 0), ("generate report", 1),
             ("backup database", 0), ("send notification", 3), ("update cache", 1)]
    print("Task Queue Demo (lower priority = higher importance)\n")
    for task, pri in tasks:
        tid = q.push(task, pri)
        print(f"  Enqueued #{tid}: '{task}' (priority={pri})")
    print(f"\nProcessing {q.size()} tasks:\n")
    while q.size():
        tid, task, pri = q.pop()
        print(f"  Processing #{tid}: '{task}' (priority={pri})")
    print(f"\n{q.stats()}")
