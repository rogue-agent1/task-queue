#!/usr/bin/env python3
"""In-memory task queue with priorities and retries."""
import time,heapq,threading,json
class Task:
    def __init__(self,id,fn,args=(),priority=0,max_retries=3):
        self.id=id;self.fn=fn;self.args=args;self.priority=priority
        self.max_retries=max_retries;self.retries=0;self.status="pending"
        self.result=None;self.error=None;self.created=time.time()
    def __lt__(self,o): return self.priority<o.priority
class TaskQueue:
    def __init__(self):
        self.queue=[];self.tasks={};self.lock=threading.Lock()
    def submit(self,id,fn,args=(),priority=0,max_retries=3):
        task=Task(id,fn,args,priority,max_retries)
        with self.lock: heapq.heappush(self.queue,task);self.tasks[id]=task
        return task
    def process_one(self):
        with self.lock:
            if not self.queue: return None
            task=heapq.heappop(self.queue)
        task.status="running"
        try: task.result=task.fn(*task.args);task.status="done"
        except Exception as e:
            task.retries+=1;task.error=str(e)
            if task.retries<task.max_retries:
                task.status="pending"
                with self.lock: heapq.heappush(self.queue,task)
            else: task.status="failed"
        return task
    def process_all(self):
        results=[]
        while self.queue: r=self.process_one();
        return [t for t in self.tasks.values()]
    def status(self):
        return {s:sum(1 for t in self.tasks.values() if t.status==s) for s in ["pending","running","done","failed"]}
if __name__=="__main__":
    q=TaskQueue()
    q.submit("a",lambda x:x*2,args=(5,),priority=1)
    q.submit("b",lambda x:x+1,args=(10,),priority=0)
    q.submit("c",lambda:1/0,priority=2,max_retries=2)
    q.process_all()
    assert q.tasks["a"].result==10;assert q.tasks["b"].result==11;assert q.tasks["c"].status=="failed"
    print(f"Queue status: {q.status()}"); print("Task queue OK")
