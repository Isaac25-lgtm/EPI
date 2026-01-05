"""
Gunicorn Configuration for 10,000+ concurrent users
Optimized for production deployment
"""
import os
import multiprocessing

# Server socket
bind = os.getenv('GUNICORN_BIND', '0.0.0.0:5000')
backlog = 2048

# Worker processes
# Formula: (2 x CPU cores) + 1
workers = int(os.getenv('GUNICORN_WORKERS', multiprocessing.cpu_count() * 2 + 1))

# Worker class - use gevent for async I/O (better for DHIS2 API calls)
worker_class = os.getenv('GUNICORN_WORKER_CLASS', 'sync')  # Use 'gevent' for async

# Threads per worker (for sync workers)
threads = int(os.getenv('GUNICORN_THREADS', 4))

# Worker connections (for async workers)
worker_connections = int(os.getenv('GUNICORN_WORKER_CONNECTIONS', 1000))

# Timeout for workers (seconds)
timeout = int(os.getenv('GUNICORN_TIMEOUT', 120))

# Graceful timeout
graceful_timeout = 30

# Keep alive connections
keepalive = 5

# Maximum requests per worker before restart (prevents memory leaks)
max_requests = int(os.getenv('GUNICORN_MAX_REQUESTS', 1000))
max_requests_jitter = 50

# Preload app for faster worker spawning
preload_app = True

# Logging
accesslog = os.getenv('GUNICORN_ACCESS_LOG', '-')
errorlog = os.getenv('GUNICORN_ERROR_LOG', '-')
loglevel = os.getenv('GUNICORN_LOG_LEVEL', 'info')

# Process naming
proc_name = 'uganda-ehmis'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (for production without reverse proxy)
# keyfile = None
# certfile = None

# Server hooks
def on_starting(server):
    """Called before the master process is initialized"""
    print(f"[Gunicorn] Starting server with {workers} workers, {threads} threads each")
    print(f"[Gunicorn] Binding to {bind}")


def when_ready(server):
    """Called just after the master process is initialized"""
    print("[Gunicorn] Server is ready to accept connections")


def worker_abort(worker):
    """Called when a worker times out"""
    print(f"[Gunicorn] Worker {worker.pid} aborted due to timeout")


def pre_fork(server, worker):
    """Called just before a worker is forked"""
    pass


def post_fork(server, worker):
    """Called just after a worker is forked"""
    print(f"[Gunicorn] Worker spawned (pid: {worker.pid})")


def post_worker_init(worker):
    """Called just after a worker has initialized"""
    pass


def worker_exit(server, worker):
    """Called just after a worker has exited"""
    print(f"[Gunicorn] Worker exited (pid: {worker.pid})")


def child_exit(server, worker):
    """Called in the master process when a worker has exited"""
    pass


def on_exit(server):
    """Called just before exiting Gunicorn"""
    print("[Gunicorn] Shutting down server")

