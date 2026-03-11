#!/usr/bin/env python3
"""task_queue — Priority task queue with workers, retries, dead-letter. Zero deps."""
import heapq, time, random
from threading import Thread, Lock
from collections import deque

class Task:
    _counter = 0
    def __init__(self, name, fn, priority=0, max_retries=3):
        Task._counter += 1
        self.id = Task._counter
        self.name, self.fn, self.priority = name, fn, priority
        self.max_retries, self.retries = max_retries, 0
        self.status = 'pending'
        self.result = self.error = None
    def __lt__(self, other): return self.priority < other.priority

class TaskQueue:
    def __init__(self, workers=2):
        self.queue = []
        self.dead_letter = []
        self.completed = []
        self.lock = Lock()
        self.running = False
        self.num_workers = workers

    def submit(self, task):
        with self.lock:
            heapq.heappush(self.queue, task)

    def _worker(self):
        while self.running:
            task = None
            with self.lock:
                if self.queue:
                    task = heapq.heappop(self.queue)
            if not task:
                time.sleep(0.01)
                continue
            task.status = 'running'
            try:
                task.result = task.fn()
                task.status = 'done'
                self.completed.append(task)
            except Exception as e:
                task.retries += 1
                task.error = str(e)
                if task.retries < task.max_retries:
                    task.status = 'retry'
                    self.submit(task)
                else:
                    task.status = 'failed'
                    self.dead_letter.append(task)

    def start(self):
        self.running = True
        self.workers = [Thread(target=self._worker, daemon=True) for _ in range(self.num_workers)]
        for w in self.workers: w.start()

    def stop(self):
        self.running = False

    def wait(self, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                if not self.queue: break
            time.sleep(0.01)
        self.stop()

def main():
    random.seed(42)
    tq = TaskQueue(workers=2)
    results = []

    def ok_task(n):
        def fn():
            time.sleep(0.01)
            return n * 10
        return fn

    def flaky_task():
        if random.random() < 0.7:
            raise RuntimeError("transient failure")
        return "survived"

    for i in range(5):
        tq.submit(Task(f"task-{i}", ok_task(i), priority=i))
    tq.submit(Task("flaky", flaky_task, priority=0, max_retries=5))

    tq.start()
    tq.wait(timeout=2)

    print("Task Queue Results:\n")
    print("Completed:")
    for t in tq.completed:
        print(f"  [{t.name}] priority={t.priority} result={t.result}")
    if tq.dead_letter:
        print("Dead Letter:")
        for t in tq.dead_letter:
            print(f"  [{t.name}] retries={t.retries} error={t.error}")

if __name__ == "__main__":
    main()
