#!/usr/bin/env python3
"""
queuectl.py - CLI job queue with workers, retries with exponential backoff, DLQ.
Single-file demo using SQLite and subprocess for running commands.

Usage examples (after making executable):
  ./queuectl.py enqueue '{"id":"job1","command":"echo Hello","max_retries":3}'
  ./queuectl.py worker start --count 2
  ./queuectl.py status
  ./queuectl.py list --state pending
  ./queuectl.py dlq list
  ./queuectl.py dlq retry job1
  ./queuectl.py config set backoff_base 2
"""

import os
import sys
import json
import sqlite3
import time
import uuid
import datetime
import subprocess
import signal
import threading
from multiprocessing import Process, Event, current_process
import click

DB_PATH = os.environ.get("QUEUECTL_DB", "queue.db")
PID_FILE = os.environ.get("QUEUECTL_PIDFILE", "queuectl_workers.pid")
DEFAULT_BACKOFF_BASE = 2
DEFAULT_MAX_RETRIES = 3
POLL_INTERVAL = 1.0  # seconds - how often a worker checks for new job

# ---------- DB helpers ----------
def get_conn():
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        max_retries INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_error TEXT,
        available_at REAL DEFAULT 0
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)
    # default config if not exists
    c.execute("INSERT OR IGNORE INTO config(key, value) VALUES(?,?)", ("backoff_base", str(DEFAULT_BACKOFF_BASE)))
    c.execute("INSERT OR IGNORE INTO config(key, value) VALUES(?,?)", ("max_retries", str(DEFAULT_MAX_RETRIES)))
    conn.commit()
    conn.close()

def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def get_config(key, default=None):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT value FROM config WHERE key=?", (key,))
    r = c.fetchone()
    conn.close()
    if r:
        return r["value"]
    return default

def set_config(key, value):
    conn = get_conn()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO config(key, value) VALUES(?,?)", (key, str(value)))
    conn.commit()
    conn.close()

# ---------- Job operations ----------
def enqueue_job(job_json):
    """
    Accepts either JSON string or dict.
    job_json should include 'id' optional, 'command', optional 'max_retries'.
    """
    if isinstance(job_json, str):
        job = json.loads(job_json)
    else:
        job = job_json
    job_id = job.get("id") or str(uuid.uuid4())
    command = job.get("command")
    if not command:
        raise ValueError("job must include 'command'")
    max_retries = int(job.get("max_retries", get_config("max_retries", DEFAULT_MAX_RETRIES)))
    created = now_iso()
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
      INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, available_at)
      VALUES (?, ?, 'pending', 0, ?, ?, ?, 0)
    """, (job_id, command, max_retries, created, created))
    conn.commit()
    conn.close()
    return job_id

def list_jobs(state=None, limit=100):
    conn = get_conn()
    c = conn.cursor()
    if state:
        c.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at LIMIT ?", (state, limit))
    else:
        c.execute("SELECT * FROM jobs ORDER BY created_at LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def job_summary():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
    rows = c.fetchall()
    summary = {r["state"]: r["cnt"] for r in rows}
    c.execute("SELECT COUNT(*) as active_workers FROM sqlite_master WHERE name='__dummy__'")  # dummy, not used
    conn.close()
    return summary

# ---------- Worker claim + locking ----------
def claim_job():
    """
    Atomically find a pending job that's available (available_at <= now)
    and transition it to 'processing' returning the job row.
    Uses a conditional update to avoid race.
    """
    conn = get_conn()
    c = conn.cursor()
    now_ts = time.time()
    # pick one pending job that's available
    # we will attempt to atomically update its state using its id
    c.execute("SELECT id FROM jobs WHERE state='pending' AND available_at <= ? ORDER BY created_at LIMIT 1", (now_ts,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    job_id = row["id"]
    # try atomic update
    updated_at = now_iso()
    res = c.execute("UPDATE jobs SET state='processing', updated_at=? WHERE id=? AND state='pending'", (updated_at, job_id))
    if res.rowcount == 1:
        # fetch the job
        c.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
        job = dict(c.fetchone())
        conn.close()
        return job
    conn.close()
    return None

def mark_job_completed(job_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (now_iso(), job_id))
    conn.commit()
    conn.close()

def mark_job_failed(job_id, err, attempts, max_retries, backoff_base):
    conn = get_conn()
    c = conn.cursor()
    attempts = int(attempts)
    max_retries = int(max_retries)
    attempts_new = attempts + 1
    if attempts_new >= max_retries:
        # move to dead
        c.execute("UPDATE jobs SET state='dead', attempts=?, last_error=?, updated_at=? WHERE id=?", (attempts_new, str(err), now_iso(), job_id))
    else:
        # schedule next attempt based on exponential backoff
        # delay = backoff_base ** attempts_new
        try:
            delay = float(backoff_base) ** attempts_new
        except Exception:
            delay = DEFAULT_BACKOFF_BASE ** attempts_new
        avail = time.time() + delay
        c.execute("UPDATE jobs SET state='failed', attempts=?, last_error=?, available_at=?, updated_at=? WHERE id=?", (attempts_new, str(err), avail, now_iso(), job_id))
    conn.commit()
    conn.close()

def retry_dlq_job(job_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM jobs WHERE id=?", (job_id,))
    r = c.fetchone()
    if not r:
        conn.close()
        raise KeyError("job not found")
    if r["state"] != "dead":
        conn.close()
        raise ValueError("job is not in DLQ")
    # reset attempts and put to pending
    c.execute("UPDATE jobs SET state='pending', attempts=0, last_error=NULL, available_at=0, updated_at=? WHERE id=?", (now_iso(), job_id))
    conn.commit()
    conn.close()

# ---------- Worker process ----------
def run_command_shell(command, timeout=None):
    # run via shell to allow builtins like "sleep 2" "echo hi" etc.
    try:
        completed = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
        return completed.returncode, completed.stdout, completed.stderr
    except subprocess.TimeoutExpired as e:
        return 124, "", f"TimeoutExpired: {e}"

def worker_loop(stop_event, worker_name, backoff_base):
    pid = os.getpid()
    click.echo(f"[{worker_name}] starting (pid={pid})")
    while not stop_event.is_set():
        job = claim_job()
        if not job:
            # nothing to do; sleep a bit
            time.sleep(POLL_INTERVAL)
            continue
        job_id = job["id"]
        cmd = job["command"]
        attempts = job["attempts"]
        max_retries = job["max_retries"]
        click.echo(f"[{worker_name}] picked job {job_id} (attempts={attempts}/{max_retries}) -> {cmd}")
        # run command
        ret, out, err = run_command_shell(cmd)
        if ret == 0:
            click.echo(f"[{worker_name}] job {job_id} completed")
            mark_job_completed(job_id)
        else:
            click.echo(f"[{worker_name}] job {job_id} failed (exit {ret}) - scheduling retry")
            mark_job_failed(job_id, f"exit={ret}; stderr={err.strip()[:200]}", attempts, max_retries, backoff_base)
        # small pause before next loop
        time.sleep(0.1)
    click.echo(f"[{worker_name}] stopping gracefully")

# ---------- Worker management (start/stop) ----------
def start_workers(n, backoff_base):
    # spawn N separate processes (not threads) so each has own interpreter
    processes = []
    stop_event = Event()
    procs = []
    for i in range(n):
        p = Process(target=worker_process_entry, args=(backoff_base, i+1))
        p.start()
        procs.append(p)
    # write PIDs to pidfile so `stop` can find them
    with open(PID_FILE, "w") as f:
        for p in procs:
            f.write(str(p.pid) + "\n")
    click.echo(f"Started {n} worker processes. PIDs written to {PID_FILE}")

def worker_process_entry(backoff_base, index):
    # Each process sets up a stop handler for SIGTERM/SIGINT
    stop_event = threading.Event()

    def _handler(signum, frame):
        click.echo(f"[worker-{index} pid={os.getpid()}] received signal {signum}, will stop after current job")
        stop_event.set()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)
    worker_loop(stop_event, f"worker-{index}", backoff_base)

def stop_workers():
    # read pidfile and send SIGTERM to each PID
    if not os.path.exists(PID_FILE):
        click.echo("No PID file found; are workers running?")
        return
    with open(PID_FILE, "r") as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
    pids = []
    for l in lines:
        try:
            pids.append(int(l))
        except:
            pass
    if not pids:
        click.echo("No PIDs found in pidfile.")
        return
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            click.echo(f"Sent SIGTERM to {pid}")
        except ProcessLookupError:
            click.echo(f"No process {pid}")
    # remove pidfile
    try:
        os.remove(PID_FILE)
    except:
        pass
    click.echo("Stop signals sent. Workers will exit after finishing current job.")

# ---------- CLI ----------
@click.group()
def cli():
    init_db()

@cli.command(help="Enqueue a job given JSON payload string or path to JSON file.")
@click.argument("payload", nargs=1)
def enqueue(payload):
    # allow passing a raw JSON string or @filename
    try:
        if payload.startswith("@"):
            path = payload[1:]
            with open(path, "r") as f:
                content = f.read()
            job_id = enqueue_job(content)
        else:
            job_id = enqueue_job(payload)
        click.echo(f"Enqueued job: {job_id}")
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)

@cli.group(help="Worker controls")
def worker():
    pass

@worker.command("start", help="Start worker processes (background).")
@click.option("--count", "-c", type=int, default=1, help="Number of worker processes to start")
def worker_start(count):
    backoff_base = float(get_config("backoff_base", DEFAULT_BACKOFF_BASE))
    start_workers(count, backoff_base)

@worker.command("stop", help="Stop running workers gracefully (send SIGTERM).")
def worker_stop():
    stop_workers()

@cli.command(help="Show summary of job states")
def status():
    s = job_summary()
    click.echo("Job summary:")
    for k in ["pending", "processing", "completed", "failed", "dead"]:
        click.echo(f"  {k:10s} : {s.get(k,0)}")
    if os.path.exists(PID_FILE):
        click.echo(f"Worker PID file: {PID_FILE}")
        with open(PID_FILE, "r") as f:
            pids = [l.strip() for l in f if l.strip()]
        click.echo(f"  Active worker PIDs (file): {', '.join(pids)}")
    else:
        click.echo("No worker PID file found.")

@cli.command(help="List jobs, optionally filter by state")
@click.option("--state", "-s", default=None, help="Job state to filter (pending/processing/completed/failed/dead)")
@click.option("--limit", default=100, help="Max rows")
def list(state, limit):
    rows = list_jobs(state=state, limit=limit)
    click.echo(json.dumps(rows, indent=2, default=str))

@cli.group(help="Dead Letter Queue operations")
def dlq():
    pass

@dlq.command("list", help="List jobs in DLQ (dead)")
def dlq_list():
    rows = list_jobs(state="dead", limit=1000)
    click.echo(json.dumps(rows, indent=2, default=str))

@dlq.command("retry", help="Retry a DLQ job (move to pending)")
@click.argument("job_id")
def dlq_retry(job_id):
    try:
        retry_dlq_job(job_id)
        click.echo(f"Moved job {job_id} from DLQ to pending")
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)

@cli.group(help="Config controls")
def config():
    pass

@config.command("set", help="Set a configuration key")
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    set_config(key, value)
    click.echo(f"Set {key} = {value}")

@config.command("get", help="Get a configuration key")
@click.argument("key")
def config_get(key):
    val = get_config(key, None)
    click.echo(val)

# ---------- Test + helper flows ----------
@cli.command("demo", help="Quick demo: enqueue sample jobs (some succeed, some fail)")
@click.option("--count", default=5, help="Total jobs")
def demo(count):
    # sample: commands that succeed and fail
    cmds = [
        "echo hello",
        "sleep 1 && echo done",
        "false",            # will exit non-zero
        "bash -c 'exit 2'", # fails
        "echo final"
    ]
    for i in range(count):
        job = {"id": f"demo-{int(time.time())}-{i}", "command": cmds[i % len(cmds)], "max_retries": 3}
        jid = enqueue_job(job)
        click.echo(f"Enqueued {jid} -> {job['command']}")
    click.echo("Run `queuectl.py worker start --count 2` to process demo jobs.")

if __name__ == "__main__":
    cli()
