#  QueueCTL – CLI-Based Background Job Queue System

**QueueCTL** is a command-line job queue system designed to manage background tasks with multiple workers, retries (using exponential backoff), and a Dead Letter Queue (DLQ) for permanently failed jobs.  

It is built as part of the **Backend Developer Internship Assignment** and demonstrates reliable background processing, persistence, and fault tolerance using a simple CLI interface.

---

##  Tech Stack

- **Language:** Python 3.9+
- **Database:** SQLite (persistent local storage)
- **CLI Framework:** Click
- **Concurrency:** Multiprocessing

---

## Features

✅ Enqueue and manage background jobs  
✅ Multiple worker processes  
✅ Retry mechanism with exponential backoff  
✅ Dead Letter Queue (DLQ) for failed jobs  
✅ Persistent storage (SQLite database)  
✅ Graceful worker shutdown  
✅ Configurable retry and backoff parameters  
✅ Clean and simple CLI interface  

---

##  Installation & Setup

```bash
# Clone repository
git clone https://github.com/<your-username>/queuectl.git
cd queuectl

# Ensure Python 3.9+ is installed
python3 --version

# Make CLI executable
chmod +x queuectl.py

# Verify commands
./queuectl.py --help
