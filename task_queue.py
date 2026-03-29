#!/usr/bin/env python3
"""task_queue - Priority task queue with retry and dead-letter support."""
import heapq, time, sys, json, uuid

class Task:
    def __init__(self, fn_name, payload=None, priority=0, max_retries=3, delay=0):
        self.id = str(uuid.uuid4())[:8]
        self.fn_name = fn_name
        self.payload = payload or {}
        self.priority = priority
        self.max_retries = max_retries
        self.retries = 0
        self.delay = delay
        self.created = time.time()
        self.run_after = time.time() + delay
        self.status = "pending"
        self.error = None
    
    def __lt__(self, other):
        if self.priority != other.priority:
            return self.priority > other.priority  # Higher = first
        return self.created < other.created

class TaskQueue:
    def __init__(self):
        self.queue = []
        self.dead_letter = []
        self.completed = []
        self.handlers = {}
    
    def register(self, name, fn):
        self.handlers[name] = fn
    
    def enqueue(self, fn_name, payload=None, priority=0, max_retries=3, delay=0):
        task = Task(fn_name, payload, priority, max_retries, delay)
        heapq.heappush(self.queue, task)
        return task.id
    
    def process_one(self):
        if not self.queue:
            return None
        task = heapq.heappop(self.queue)
        if time.time() < task.run_after:
            heapq.heappush(self.queue, task)
            return None
        handler = self.handlers.get(task.fn_name)
        if not handler:
            task.status = "failed"
            task.error = f"No handler for {task.fn_name}"
            self.dead_letter.append(task)
            return task
        try:
            handler(task.payload)
            task.status = "completed"
            self.completed.append(task)
        except Exception as e:
            task.retries += 1
            task.error = str(e)
            if task.retries >= task.max_retries:
                task.status = "dead"
                self.dead_letter.append(task)
            else:
                task.status = "retry"
                task.run_after = time.time() + (2 ** task.retries) * 0.01
                heapq.heappush(self.queue, task)
        return task
    
    def process_all(self):
        results = []
        while self.queue:
            r = self.process_one()
            if r is None:
                break
            results.append(r)
        return results
    
    def stats(self):
        return {
            "pending": len(self.queue),
            "completed": len(self.completed),
            "dead_letter": len(self.dead_letter),
        }

def test():
    q = TaskQueue()
    results = []
    q.register("greet", lambda p: results.append(f"Hi {p['name']}"))
    q.register("fail", lambda p: (_ for _ in ()).throw(ValueError("boom")))
    
    q.enqueue("greet", {"name": "Alice"}, priority=1)
    q.enqueue("greet", {"name": "Bob"}, priority=2)
    q.enqueue("fail", {}, max_retries=2)
    
    processed = q.process_all()
    assert results[0] == "Hi Bob"  # Higher priority first
    assert results[1] == "Hi Alice"
    
    # Retry then dead-letter
    while q.queue:
        time.sleep(0.05)
        q.process_all()
    
    s = q.stats()
    assert s["completed"] == 2
    assert s["dead_letter"] == 1
    assert q.dead_letter[0].retries == 2
    
    print(f"Stats: {s}")
    print("All tests passed!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: task_queue.py test")
