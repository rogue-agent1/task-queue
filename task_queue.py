#!/usr/bin/env python3
"""task_queue - Priority task queue with worker pool."""
import argparse, threading, time, heapq, json, random

class TaskQueue:
    def __init__(self, workers=4):
        self.queue = []; self.lock = threading.Lock(); self.workers = workers
        self.results = {}; self.task_id = 0; self.running = True

    def submit(self, func, args=(), priority=0):
        with self.lock:
            self.task_id += 1
            heapq.heappush(self.queue, (priority, self.task_id, func, args))
            return self.task_id

    def worker(self):
        while self.running:
            task = None
            with self.lock:
                if self.queue: task = heapq.heappop(self.queue)
            if task:
                priority, tid, func, args = task
                try:
                    result = func(*args)
                    self.results[tid] = {"status": "done", "result": result}
                except Exception as e:
                    self.results[tid] = {"status": "error", "error": str(e)}
            else:
                time.sleep(0.01)

    def run(self):
        threads = [threading.Thread(target=self.worker, daemon=True) for _ in range(self.workers)]
        for t in threads: t.start()
        return threads

    def stop(self): self.running = False

def main():
    p = argparse.ArgumentParser(description="Task queue demo")
    p.add_argument("-w", "--workers", type=int, default=4)
    p.add_argument("-n", "--tasks", type=int, default=20)
    args = p.parse_args()
    tq = TaskQueue(args.workers)
    def work(n):
        time.sleep(random.uniform(0.01, 0.1))
        return n * n
    for i in range(args.tasks):
        tq.submit(work, (i,), priority=random.randint(0, 5))
    threads = tq.run()
    time.sleep(2); tq.stop()
    completed = sum(1 for r in tq.results.values() if r["status"] == "done")
    print(f"Completed {completed}/{args.tasks} tasks with {args.workers} workers")
    print(json.dumps(dict(list(tq.results.items())[:5]), indent=2))

if __name__ == "__main__":
    main()
