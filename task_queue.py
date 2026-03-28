#!/usr/bin/env python3
"""task_queue - Distributed task queue."""
import argparse, socket, threading, json, time, queue, sys, uuid

class TaskQueue:
    def __init__(self):
        self.pending = queue.Queue(); self.results = {}; self.lock = threading.Lock()
    def submit(self, task):
        tid = str(uuid.uuid4())[:8]
        self.pending.put({"id":tid, "task":task, "submitted":time.time()})
        return tid
    def get_task(self, timeout=5):
        try: return self.pending.get(timeout=timeout)
        except queue.Empty: return None
    def complete(self, tid, result):
        with self.lock: self.results[tid] = {"result":result, "completed":time.time()}
    def status(self, tid):
        with self.lock: return self.results.get(tid)

def handle(client, tq):
    try:
        buf = ""
        while True:
            data = client.recv(4096)
            if not data: break
            buf += data.decode()
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                msg = json.loads(line)
                cmd = msg.get("cmd")
                if cmd == "submit":
                    tid = tq.submit(msg["task"])
                    client.sendall(json.dumps({"id":tid}).encode()+b"\n")
                elif cmd == "fetch":
                    task = tq.get_task(timeout=1)
                    client.sendall(json.dumps({"task":task}).encode()+b"\n")
                elif cmd == "complete":
                    tq.complete(msg["id"], msg["result"])
                    client.sendall(b'{"ok":true}\n')
                elif cmd == "status":
                    r = tq.status(msg["id"])
                    client.sendall(json.dumps({"status":r}).encode()+b"\n")
    except: pass
    client.close()

def main():
    p = argparse.ArgumentParser(description="Task queue")
    p.add_argument("-p","--port",type=int,default=9091)
    a = p.parse_args()
    tq = TaskQueue()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", a.port)); srv.listen(50)
    print(f"Task queue on :{a.port}")
    while True:
        c, _ = srv.accept()
        threading.Thread(target=handle, args=(c, tq), daemon=True).start()

if __name__ == "__main__": main()
