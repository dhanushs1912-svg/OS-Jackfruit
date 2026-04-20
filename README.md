# Multi-Container Runtime — OS-Jackfruit

A lightweight Linux container runtime in C with a long-running supervisor, bounded-buffer logging, and a kernel-space memory monitor.

---

## 1. Team Information

| Name | SRN |
|------|-----|
| _Dhanush S_ | _PES1UG24AM360_ |
| _Darshan Govekar_ | _PES1UG24AM355_ |

---

## 2. Build, Load, and Run Instructions

### Prerequisites

- Ubuntu 22.04 or 24.04 in a VM (not WSL)
- Secure Boot OFF
- Root access

### Install Dependencies

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r) wget
```

### Run Environment Check

```bash
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
```

### Prepare Root Filesystem

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

### Build

```bash
make
```

Produces: `engine`, `cpu_hog`, `io_pulse`, `memory_hog`, `monitor.ko`.

### Load Kernel Module

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

### Start the Supervisor (Terminal 1)

```bash
sudo ./engine supervisor ./rootfs-base
```

Leave this running. All CLI commands go in a second terminal.

### Launch Containers (Terminal 2)

```bash
# Create per-container writable rootfs copies
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta

# Copy test workloads into rootfs before launch
cp cpu_hog memory_hog io_pulse ./rootfs-alpha/
cp cpu_hog memory_hog io_pulse ./rootfs-beta/

# Start containers
sudo ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta ./rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96

# List containers
sudo ./engine ps

# View logs
sudo ./engine logs alpha

# Stop a container
sudo ./engine stop alpha
```

### Run Memory Limit Test

```bash
cp memory_hog ./rootfs-base/memory_hog
cp -a ./rootfs-base ./rootfs-memtest
sudo ./engine start memtest ./rootfs-memtest /memory_hog --soft-mib 40 --hard-mib 64

# Wait ~10 seconds, then check
sudo dmesg | grep container_monitor
sudo ./engine ps
```

### Run Scheduling Experiments

```bash
cp cpu_hog ./rootfs-base/cpu_hog
cp io_pulse ./rootfs-base/io_pulse

# Experiment 1: CPU-bound at different nice values
cp -a ./rootfs-base ./rootfs-exp1a
cp -a ./rootfs-base ./rootfs-exp1b
sudo ./engine start exp1a ./rootfs-exp1a /cpu_hog --soft-mib 48 --hard-mib 80 --nice 0
sudo ./engine start exp1b ./rootfs-exp1b /cpu_hog --soft-mib 48 --hard-mib 80 --nice 19

# Wait for both to finish, then compare
sudo ./engine logs exp1a
sudo ./engine logs exp1b

# Experiment 2: CPU-bound vs I/O-bound
cp -a ./rootfs-base ./rootfs-exp2cpu
cp -a ./rootfs-base ./rootfs-exp2io
sudo ./engine start exp2cpu ./rootfs-exp2cpu /cpu_hog --soft-mib 48 --hard-mib 80
sudo ./engine start exp2io ./rootfs-exp2io /io_pulse --soft-mib 48 --hard-mib 80

# Wait, then compare
sudo ./engine logs exp2cpu
sudo ./engine logs exp2io
sudo ./engine ps
```

### Teardown

```bash
# Stop remaining containers
sudo ./engine stop alpha
sudo ./engine stop beta

# Ctrl+C the supervisor in Terminal 1

# Verify clean state
ps aux | grep engine
sudo dmesg | tail -5

# Unload kernel module
sudo rmmod monitor
```

---

## 3. Demo with Screenshots

### 3.1 Multi-container supervision

![Multi-container](screenshots/01-multi-container.png)

**Caption**: Two containers running under one supervisor process, each with isolated PID+UTS+MNT namespaces.

### 3.2 Metadata tracking

![Metadata](screenshots/02-metadata-ps.png)

**Caption**: `./engine ps` output showing container ID, host PID, state, soft/hard limits, nice value, exit status, and start time for each tracked container.

### 3.3 Bounded-buffer logging

![Logging](screenshots/03-logging.png)

**Caption**: Per-container log file showing timestamped output captured through the pipe → bounded buffer → logger thread → file pipeline.

### 3.4 CLI and IPC

![CLI-IPC](screenshots/04-cli-ipc.png)

**Caption**: CLI command issued via UNIX domain socket (`/tmp/mini_runtime.sock`), supervisor receives and responds with structured `control_request_t`/`control_response_t` messages.

### 3.5 Soft-limit warning

![Soft-limit](screenshots/05-soft-limit.png)

**Caption**: Kernel monitor detects process exceeding soft limit; `dmesg` shows `SOFT LIMIT` warning with container ID, PID, and RSS values.

### 3.6 Hard-limit enforcement

![Hard-limit](screenshots/06-hard-limit.png)

**Caption**: Kernel monitor kills process exceeding hard limit via SIGKILL. `dmesg` shows `HARD LIMIT` message. `./engine ps` shows container state as `killed` with `sig9`.

### 3.7 Scheduling experiment

![Scheduling](screenshots/07-scheduling.png)

**Caption**: CPU-bound workloads at different nice values showing observable differences in completion time, demonstrating CFS priority-based CPU share allocation.

### 3.8 Clean teardown

![Cleanup](screenshots/08-cleanup.png)

**Caption**: Supervisor shuts down orderly — stops containers, joins logger thread, closes sockets. No zombie processes remain. Kernel module unloads cleanly.

---

## 4. Engineering Analysis

### 4.1 Isolation Mechanisms

Our runtime achieves isolation through three Linux namespace types passed as flags to `clone()`:

**PID namespace** (`CLONE_NEWPID`): The container's init process sees itself as PID 1 inside its own process tree. The host kernel maintains a mapping between namespace-local PIDs and global PIDs. A container cannot see or signal processes outside its namespace via `/proc`, but the host supervisor can manage the container using the global PID returned by `clone()`.

**UTS namespace** (`CLONE_NEWUTS`): Each container gets its own hostname via `sethostname()` inside the child. This allows each container to have its own system identity without affecting the host or other containers.

**Mount namespace** (`CLONE_NEWNS`): After `clone()`, the child calls `chroot()` into its own rootfs directory, making the Alpine filesystem the new `/`. A fresh `/proc` is mounted scoped to the PID namespace so tools like `ps` work correctly inside the container. The host's filesystem is invisible to the container.

**What the host kernel still shares**: All containers run on the same kernel, sharing the scheduler, network stack (we don't use `CLONE_NEWNET`), IPC namespace, user namespace, and kernel memory. The kernel memory monitor in this project directly accesses shared kernel data structures (`task->mm` via `get_mm_rss`), which is possible precisely because the kernel is shared. A kernel vulnerability would affect all containers equally.

### 4.2 Supervisor and Process Lifecycle

A long-running parent supervisor is essential because containers need lifecycle management that outlives any individual container.

**Process creation**: The supervisor calls `clone()` with namespace flags. The kernel creates a child process with new namespace instances, copies the parent's address space (copy-on-write), and returns the child's host PID to the parent. The child then calls `chroot()` and `execve()` to become the container's init process.

**Parent-child relationship**: The supervisor remains the parent of all container processes. Linux delivers `SIGCHLD` to the parent when a child exits, and only the parent can `waitpid()` to reap the child and release its process table entry. Without a persistent parent, containers would become orphans adopted by PID 1 (`init`), losing lifecycle tracking.

**Reaping**: Our `SIGCHLD` handler calls `waitpid(-1, &status, WNOHANG)` in a loop to reap all exited children without blocking. `WNOHANG` prevents the handler from blocking when multiple children exit simultaneously.

**Metadata and attribution**: Each container record tracks state transitions. The `stop_requested` flag is set *before* sending any signal from the `stop` command. When `SIGCHLD` fires:
- `WIFEXITED(status)` → `CONTAINER_EXITED` (normal exit)
- `WIFSIGNALED(status)` + `stop_requested` → `CONTAINER_STOPPED` (supervisor-initiated)
- `WIFSIGNALED(status)` + `WTERMSIG == SIGKILL` + `!stop_requested` → `CONTAINER_KILLED` (hard-limit kill by kernel monitor)

This attribution rule ensures `ps` output can always distinguish the three termination causes.

### 4.3 IPC, Threads, and Synchronization

The project uses two distinct IPC mechanisms and one shared bounded buffer:

**Path A — Pipes (container → supervisor)**: Each container's stdout/stderr is captured via `pipe()`. Before `clone()`, we create a pipe; the child `dup2()`s the write end onto FDs 1 and 2; the parent reads from the read end via a dedicated pipe-reader thread. Pipes are kernel-managed and inherently safe for single-writer/single-reader.

**Path B — UNIX Domain Socket (CLI → supervisor)**: The supervisor listens on `/tmp/mini_runtime.sock`. The CLI connects, sends a structured `control_request_t`, receives a `control_response_t`, and disconnects. This is a different IPC mechanism from the logging pipes, using `AF_UNIX` stream sockets. Each CLI connection is handled sequentially in the supervisor's main loop.

**Bounded Buffer**: The buffer sits between pipe-reader threads (producers) and the logger thread (consumer). Without synchronization, the following race conditions would occur:

- **Data corruption**: Two producers writing to the same slot simultaneously would overwrite each other. We prevent this with a `pthread_mutex_t` that serializes all buffer access.
- **Index corruption**: Concurrent increment of `head` or `tail` without a lock could skip slots or write out of bounds. The mutex ensures atomic read-modify-write of these indices.
- **Busy-waiting**: Without condition variables, the consumer would spin-check the buffer. We use `pthread_cond_t not_empty` (consumer waits when empty) and `pthread_cond_t not_full` (producers wait when full).
- **Deadlock on shutdown**: The `shutting_down` flag combined with `pthread_cond_broadcast` ensures both producers and consumers unblock and exit cleanly. The consumer drains all remaining items before returning -1, guaranteeing no log lines are lost.

**Container metadata (linked list)**: Protected by a separate `pthread_mutex_t metadata_lock`. The `SIGCHLD` handler accesses metadata without locking because signal handlers cannot safely use mutexes. We mitigate this by keeping the handler's writes to simple assignments on naturally-aligned fields.

### 4.4 Memory Management and Enforcement

**What RSS measures**: RSS (Resident Set Size) counts the number of physical page frames currently mapped into a process's address space. It includes code pages, heap, stack, and shared libraries — but only pages physically in RAM.

**What RSS does not measure**: RSS does not capture swap usage, memory-mapped files not yet faulted in, or kernel memory consumed on behalf of the process (page tables, socket buffers, slab allocations). Two processes sharing a library each count the shared pages in their RSS, so summing RSS can overcount actual physical usage. Our `memory_hog` test calls `memset()` on every allocated page to force RSS to reflect true allocation.

**Why soft and hard limits differ**: Soft limits serve as early warnings — the process may be approaching dangerous territory but can still run. This gives the operator time to react. Hard limits are a safety net — once crossed, the system kills the process to prevent it from exhausting physical memory and triggering the OOM killer, which could kill unrelated processes.

**Why enforcement belongs in kernel space**: User-space polling of `/proc/[pid]/status` has several disadvantages: (1) it requires parsing text files on every check, adding latency; (2) a CPU-hogging container could starve the user-space monitor; (3) it runs at normal scheduling priority with no guarantee of timely execution. The kernel module runs in privileged context, accesses `task->mm` RSS directly without syscall overhead, and can send `SIGKILL` immediately. The tradeoff is risk — a kernel module bug can crash the entire system.

We use a `mutex` rather than a `spinlock` to protect the monitored list. Both the ioctl handlers and timer callback access this list. The ioctl path runs in process context (user syscall) where sleeping is safe. The timer callback uses `mutex_trylock` — if the lock is held, it skips that check cycle. Since the timer fires every second, missing one cycle is acceptable. This approach is simpler and safer than a spinlock, which would require disabling interrupts.

### 4.5 Scheduling Behavior

Our experiments use the CFS (Completely Fair Scheduler), Linux's default for `SCHED_NORMAL` tasks.

**Experiment 1 — CPU-bound at different nice values**: Two containers each run `cpu_hog`. Container exp1a runs at nice 0 (default weight 1024), container exp1b at nice 19 (weight ~15). CFS allocates CPU time proportional to weight, giving exp1a roughly 98% of available CPU on a single core. The result: exp1a completes significantly faster. This demonstrates CFS's **fairness** goal — "fair" means proportional to weight, not equal time.

**Experiment 2 — CPU-bound vs I/O-bound**: `cpu_hog` and `io_pulse` run simultaneously at the same priority. CFS gives the I/O-bound process a "sleeper bonus" — each time it wakes from I/O, CFS places it at the front of the run queue because its `vruntime` has not advanced during sleep. The I/O-bound process gets excellent **responsiveness** (low latency from wake to run), while the CPU-bound process utilizes whatever CPU remains. The I/O-bound process finishes at roughly the same time as if it ran alone, because it uses very little total CPU time. This demonstrates CFS's balance between **responsiveness** for interactive/I/O tasks and **throughput** for compute tasks.

---

## 5. Design Decisions and Tradeoffs

### Namespace Isolation

- **Choice**: Used `clone()` with `CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS` and `chroot()`.
- **Tradeoff**: `chroot()` is simpler but less secure than `pivot_root()` — a root process inside the container could escape via directory traversal. `pivot_root()` is production-grade (used by Docker/runc) but requires more mount setup.
- **Justification**: `chroot()` provides sufficient isolation to demonstrate namespace mechanics. The project focus is on supervisor lifecycle and IPC, not security hardening.

### Supervisor Architecture

- **Choice**: Single-threaded accept loop with `select()` timeout and sequential request handling.
- **Tradeoff**: Cannot handle simultaneous CLI requests — a slow `run` command blocks other operations.
- **Justification**: Container management commands are infrequent and fast (except `run`, which is intentionally blocking). Multi-threaded accept would add complexity without meaningful benefit.

### IPC and Logging

- **Choice**: UNIX domain socket for CLI control (Path B), pipes for container stdout (Path A), structured `control_request_t`/`control_response_t` for the message format.
- **Tradeoff**: Binary struct-based messages are rigid — adding new fields requires recompilation of both client and supervisor. A text-based protocol would be more extensible.
- **Justification**: Binary structs are simple to implement, avoid parsing overhead, and the CLI contract is fixed by the project spec. The boilerplate already defines the structs.

### Kernel Monitor

- **Choice**: Mutex-protected linked list with `mutex_trylock` in the timer callback. Timer fires every 1 second.
- **Tradeoff**: Using `mutex_trylock` in the timer means a check cycle can be skipped if an ioctl is in progress. A spinlock would never skip but disables preemption.
- **Justification**: Missing one 1-second check is acceptable. The mutex is simpler and avoids the complexity of spinlock context rules. `memory_hog` allocates slowly (8 MiB/second), so 1-second granularity is sufficient to catch both soft and hard limits.

### Scheduling Experiments

- **Choice**: Used the `--nice` flag to control CFS weight, with `cpu_hog` and `io_pulse` as workloads.
- **Tradeoff**: `nice` only affects relative weight within CFS. Real-time policies (`SCHED_FIFO`) or cgroup CPU bandwidth would provide more dramatic results.
- **Justification**: `nice` directly maps to CFS weight, making results easy to explain. It requires no additional kernel configuration and the boilerplate already supports the `--nice` flag.

---

## 6. Scheduler Experiment Results

### Experiment 1: CPU-bound with Different Nice Values

| Container | Workload | Nice | Duration | Observation |
|-----------|----------|------|----------|-------------|
| exp1a     | cpu_hog  | 0    | 10s      | Completed on time, received majority of CPU share |
| exp1b     | cpu_hog  | 19   | 10s      | Completed on time but with fewer iterations per second |

**Analysis**: On a multi-core VM both containers may each get their own core, reducing the observable difference. On a single-core system, exp1a (nice 0, weight 1024) dominates CPU time over exp1b (nice 19, weight ~15), giving a theoretical ratio of ~68:1. The `cpu_hog` output shows the `accumulator` value at each elapsed second — exp1a's accumulator grows faster, confirming it received more CPU cycles per unit time.

### Experiment 2: CPU-bound vs I/O-bound

| Container | Workload  | Observation |
|-----------|-----------|-------------|
| exp2cpu   | cpu_hog   | Runs at ~100% CPU utilization for its duration |
| exp2io    | io_pulse  | Finishes quickly, spends most time sleeping on I/O |

**Analysis**: The I/O-bound container finishes much faster because it spends most of its time sleeping between write bursts. CFS's sleeper fairness gives it prompt scheduling whenever it wakes — low latency from wake to run. The CPU-bound container's completion time is nearly identical to running alone, because the I/O process uses negligible total CPU time. This demonstrates CFS's dual goals: responsiveness for I/O-bound tasks and throughput for CPU-bound tasks.

---

## Repository Structure

```
.
├── boilerplate/           # Original boilerplate starter files
│   ├── engine.c
│   ├── monitor.c
│   ├── monitor_ioctl.h
│   ├── cpu_hog.c
│   ├── io_pulse.c
│   ├── memory_hog.c
│   ├── Makefile
│   └── environment-check.sh
├── engine.c               # Completed user-space supervisor and CLI
├── monitor.c              # Completed kernel module (LKM)
├── monitor_ioctl.h        # Shared ioctl definitions (unchanged)
├── cpu_hog.c              # CPU-bound test workload (unchanged)
├── io_pulse.c             # I/O-bound test workload (unchanged)
├── memory_hog.c           # Memory pressure workload (unchanged)
├── Makefile               # Builds all targets with 'make'
├── environment-check.sh   # VM preflight check
├── screenshots/           # Demo screenshots
└── README.md              # This file
```
