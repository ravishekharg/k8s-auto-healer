"""Demo service with a failure mode Kubernetes can't see.

/work does the service's real job and needs the (single) worker resource, like a database
connection or a lock. POST /hang grabs that resource and never releases it, the way a deadlock or
a leaked connection pool does. From then on /work times out, but /health (liveness) and /ready
(readiness) still answer, because they don't touch the worker. The pod stays Running and Ready,
keeps receiving traffic, and fails a share of requests until something replaces it.
"""
import os
import socket
import threading

from fastapi import FastAPI, Response

app = FastAPI()
worker = threading.Lock()
HOST = socket.gethostname()
WORK_TIMEOUT = float(os.environ.get("WORK_TIMEOUT_SECONDS", "1"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    return {"ready": True}


@app.get("/work")
def work(response: Response):
    if not worker.acquire(timeout=WORK_TIMEOUT):
        response.status_code = 503
        return {"host": HOST, "error": "worker unavailable"}
    try:
        return {"host": HOST, "result": "done"}
    finally:
        worker.release()


@app.post("/hang")
def hang():
    acquired = worker.acquire(timeout=0.1)
    return {"host": HOST, "hung": acquired}
