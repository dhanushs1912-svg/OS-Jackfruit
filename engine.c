/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Built on top of boilerplate starter code.
 * All TODO sections implemented:
 *   - Control-plane IPC via UNIX domain socket (Path B)
 *   - Container lifecycle with clone + namespaces
 *   - Producer/consumer bounded-buffer logging (Path A)
 *   - Signal handling (SIGCHLD, SIGINT, SIGTERM)
 *   - Graceful shutdown with thread join and resource cleanup
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int exit_code;
    int exit_signal;
    int stop_requested;  /* Set BEFORE signaling — attribution rule */
    char log_path[PATH_MAX];
    char rootfs[PATH_MAX];
    int pipe_read_fd;
    pthread_t reader_thread;
    int reader_active;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile sig_atomic_t should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Global supervisor context — needed for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

/* ═══════════════════════════════════════════════════════════════════════
 *  Usage and flag parsing (from boilerplate)
 * ═══════════════════════════════════════════════════════════════════════ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Bounded buffer implementation
 * ═══════════════════════════════════════════════════════════════════════ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * Producer-side insertion into the bounded buffer.
 * Blocks when full. Wakes consumers. Stops cleanly on shutdown.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    memcpy(&buffer->items[buffer->head], item, sizeof(log_item_t));
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * Consumer-side removal from the bounded buffer.
 * Waits while empty. Returns -1 when shutdown + empty (flush complete).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* Shutdown: drain remaining items first, then return -1 when empty */
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    memcpy(item, &buffer->items[buffer->tail], sizeof(log_item_t));
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Logging consumer thread
 *  Drains bounded buffer → writes to per-container log files.
 *  Flushes ALL remaining entries before exiting.
 * ═══════════════════════════════════════════════════════════════════════ */

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    char path[PATH_MAX];

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;  /* Shutdown + buffer empty → all flushed */

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        FILE *f = fopen(path, "a");
        if (f) {
            time_t now = time(NULL);
            struct tm tm;
            localtime_r(&now, &tm);
            char ts[32];
            strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm);
            fprintf(f, "[%s] %.*s", ts, (int)item.length, item.data);
            fclose(f);
        }
    }

    printf("[supervisor] Logger thread exiting (all entries flushed)\n");
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Pipe reader threads (one per container — producer side)
 *  Reads from container pipe → pushes into bounded buffer.
 *  Exits cleanly when pipe closes (container exits).
 * ═══════════════════════════════════════════════════════════════════════ */

typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int pipe_fd;
} pipe_reader_arg_t;

static void *pipe_reader_thread(void *arg)
{
    pipe_reader_arg_t *pra = (pipe_reader_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while ((n = read(pra->pipe_fd, item.data, sizeof(item.data) - 1)) > 0) {
        item.data[n] = '\0';
        item.length = (size_t)n;
        strncpy(item.container_id, pra->container_id, CONTAINER_ID_LEN - 1);
        item.container_id[CONTAINER_ID_LEN - 1] = '\0';

        if (bounded_buffer_push(&pra->ctx->log_buffer, &item) != 0)
            break;  /* Shutdown */
    }

    close(pra->pipe_fd);
    printf("[supervisor] Pipe reader for '%s' finished\n", pra->container_id);

    /* Mark reader as inactive */
    pthread_mutex_lock(&pra->ctx->metadata_lock);
    container_record_t *rec = pra->ctx->containers;
    while (rec) {
        if (strcmp(rec->id, pra->container_id) == 0) {
            rec->reader_active = 0;
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&pra->ctx->metadata_lock);

    free(pra);
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Clone child entrypoint — runs inside new namespaces
 * ═══════════════════════════════════════════════════════════════════════ */

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout/stderr to the logging pipe */
    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    /* Set hostname inside UTS namespace */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        perror("sethostname");

    /* Apply nice value for scheduling experiments */
    if (cfg->nice_value != 0) {
        errno = 0;
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    /* chroot into the container's rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    /* Mount /proc so ps and /proc work inside the PID namespace */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("mount /proc");

    /* Execute the command */
    char *argv[] = { cfg->command, NULL };
    char *envp[] = {
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/root",
        "TERM=xterm",
        NULL
    };
    execve(cfg->command, argv, envp);
    perror("execve");
    return 1;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Kernel monitor integration (from boilerplate)
 * ═══════════════════════════════════════════════════════════════════════ */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Container metadata helpers
 * ═══════════════════════════════════════════════════════════════════════ */

static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, id) == 0) return rec;
        rec = rec->next;
    }
    return NULL;
}

/* Check if a rootfs is already used by a running container */
static int rootfs_in_use(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING && strcmp(rec->rootfs, rootfs) == 0)
            return 1;
        rec = rec->next;
    }
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  SIGCHLD handler — reap children, update metadata
 *
 *  Attribution rule:
 *    stop_requested + signal exit → CONTAINER_STOPPED
 *    SIGKILL + !stop_requested   → CONTAINER_KILLED (hard limit)
 *    normal exit                 → CONTAINER_EXITED
 * ═══════════════════════════════════════════════════════════════════════ */

static void sigchld_handler(int sig)
{
    (void)sig;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code = 0;
                    if (rec->stop_requested) {
                        rec->state = CONTAINER_STOPPED;
                    } else if (WTERMSIG(status) == SIGKILL) {
                        rec->state = CONTAINER_KILLED;
                    } else {
                        rec->state = CONTAINER_STOPPED;
                    }
                }
                break;
            }
            rec = rec->next;
        }
    }
}

static void shutdown_handler(int sig)
{
    (void)sig;
    if (g_ctx) g_ctx->should_stop = 1;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Supervisor command handlers
 * ═══════════════════════════════════════════════════════════════════════ */

static void handle_start(supervisor_ctx_t *ctx, const control_request_t *req,
                         control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    if (find_container(ctx, req->container_id)) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: Container '%s' already exists", req->container_id);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }
    if (rootfs_in_use(ctx, req->rootfs)) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: rootfs '%s' already in use", req->rootfs);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    struct stat st;
    if (stat(req->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: rootfs '%s' not found", req->rootfs);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    /* Create logging pipe */
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: pipe(): %s", strerror(errno));
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    /* Prepare child config */
    child_config_t *cfg = malloc(sizeof(*cfg));
    if (!cfg) {
        close(pipefd[0]); close(pipefd[1]);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "ERROR: malloc failed");
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }
    strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, req->rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    /* Allocate stack for clone */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        free(cfg); close(pipefd[0]); close(pipefd[1]);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "ERROR: stack malloc failed");
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    pid_t child_pid = clone(child_fn, stack + STACK_SIZE,
                            CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD, cfg);
    if (child_pid < 0) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: clone(): %s", strerror(errno));
        free(stack); free(cfg); close(pipefd[0]); close(pipefd[1]);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    close(pipefd[1]); /* Parent closes write end */

    /* Create metadata record */
    container_record_t *rec = calloc(1, sizeof(*rec));
    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid = child_pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->nice_value = req->nice_value;
    rec->stop_requested = 0;
    rec->pipe_read_fd = pipefd[0];
    strncpy(rec->rootfs, req->rootfs, sizeof(rec->rootfs) - 1);
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    /* Insert into linked list */
    rec->next = ctx->containers;
    ctx->containers = rec;

    /* Start pipe reader thread (producer) */
    pipe_reader_arg_t *pra = malloc(sizeof(*pra));
    pra->ctx = ctx;
    strncpy(pra->container_id, req->container_id, CONTAINER_ID_LEN - 1);
    pra->pipe_fd = pipefd[0];
    rec->reader_active = 1;
    pthread_create(&rec->reader_thread, NULL, pipe_reader_thread, pra);
    pthread_detach(rec->reader_thread);

    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id,
                                  child_pid, req->soft_limit_bytes,
                                  req->hard_limit_bytes) == 0) {
            printf("[supervisor] Registered PID %d with kernel monitor\n", child_pid);
        } else {
            fprintf(stderr, "[supervisor] Warning: monitor register failed for PID %d\n",
                    child_pid);
        }
    }

    free(stack);
    free(cfg);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "OK: '%s' started PID %d (soft=%luMiB hard=%luMiB nice=%d)",
             req->container_id, child_pid,
             req->soft_limit_bytes >> 20,
             req->hard_limit_bytes >> 20,
             req->nice_value);
}

static void handle_stop(supervisor_ctx_t *ctx, const control_request_t *req,
                        control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container(ctx, req->container_id);

    if (!rec) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: Container '%s' not found", req->container_id);
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }
    if (rec->state != CONTAINER_RUNNING) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: '%s' not running (state=%s)",
                 req->container_id, state_to_string(rec->state));
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    /* ATTRIBUTION RULE: set stop_requested BEFORE signaling */
    rec->stop_requested = 1;
    pid_t pid = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    printf("[supervisor] Sending SIGTERM to '%s' (PID %d)\n", req->container_id, pid);
    kill(pid, SIGTERM);

    /* Grace period */
    usleep(3000000);

    pthread_mutex_lock(&ctx->metadata_lock);
    if (rec->state == CONTAINER_RUNNING) {
        printf("[supervisor] Force killing '%s' (PID %d)\n", req->container_id, pid);
        kill(pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        unregister_from_monitor(ctx->monitor_fd, req->container_id, pid);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "OK: Stop sent to '%s'", req->container_id);
}

static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    /* Build response string — fits in CONTROL_MESSAGE_LEN for small lists.
     * For larger output, we write directly to the client fd.
     * For simplicity here, we pack what we can. */
    int off = 0;
    off += snprintf(resp->message + off, sizeof(resp->message) - off,
                    "%-10s %-8s %-10s %-6s %-6s %-5s %-6s %s\n",
                    "ID", "PID", "STATE", "SOFT", "HARD", "NICE", "EXIT", "STARTED");

    container_record_t *rec = ctx->containers;
    while (rec && off < (int)sizeof(resp->message) - 120) {
        struct tm tm;
        localtime_r(&rec->started_at, &tm);
        char ts[20];
        strftime(ts, sizeof(ts), "%H:%M:%S", &tm);

        char exit_str[16];
        if (rec->state == CONTAINER_RUNNING || rec->state == CONTAINER_STARTING)
            snprintf(exit_str, sizeof(exit_str), "-");
        else if (rec->exit_signal > 0)
            snprintf(exit_str, sizeof(exit_str), "sig%d", rec->exit_signal);
        else
            snprintf(exit_str, sizeof(exit_str), "%d", rec->exit_code);

        off += snprintf(resp->message + off, sizeof(resp->message) - off,
                        "%-10s %-8d %-10s %-6lu %-6lu %-5d %-6s %s\n",
                        rec->id, rec->host_pid, state_to_string(rec->state),
                        rec->soft_limit_bytes >> 20,
                        rec->hard_limit_bytes >> 20,
                        rec->nice_value, exit_str, ts);
        rec = rec->next;
    }

    if (!ctx->containers)
        off += snprintf(resp->message + off, sizeof(resp->message) - off,
                        "(no containers)\n");

    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = 0;
}

static void handle_logs(supervisor_ctx_t *ctx, const control_request_t *req,
                        control_response_t *resp)
{
    (void)ctx;
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, req->container_id);

    FILE *f = fopen(path, "r");
    if (!f) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: No log for '%s': %s", req->container_id, strerror(errno));
        return;
    }

    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    long max_read = (long)sizeof(resp->message) - 64;
    long start = (sz > max_read) ? sz - max_read : 0;
    fseek(f, start, SEEK_SET);

    int off = 0;
    if (start > 0)
        off += snprintf(resp->message + off, sizeof(resp->message) - off,
                        "...(last %ld of %ld bytes)\n", sz - start, sz);

    size_t nread = fread(resp->message + off, 1, sizeof(resp->message) - off - 1, f);
    resp->message[off + nread] = '\0';
    fclose(f);
    resp->status = 0;
}

static void handle_run(supervisor_ctx_t *ctx, const control_request_t *req,
                       control_response_t *resp)
{
    /* Start the container */
    handle_start(ctx, req, resp);
    if (resp->status != 0) return;

    /* Wait for the container to exit */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container(ctx, req->container_id);
    pid_t cpid = rec ? rec->host_pid : -1;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (cpid > 0) {
        int wstatus;
        waitpid(cpid, &wstatus, 0);
        int exit_code = 0;
        if (WIFEXITED(wstatus))
            exit_code = WEXITSTATUS(wstatus);
        else if (WIFSIGNALED(wstatus))
            exit_code = 128 + WTERMSIG(wstatus);

        int len = strlen(resp->message);
        snprintf(resp->message + len, sizeof(resp->message) - len,
                 "\n'%s' exited code=%d", req->container_id, exit_code);
    }
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Supervisor cleanup
 * ═══════════════════════════════════════════════════════════════════════ */

static void supervisor_cleanup(supervisor_ctx_t *ctx)
{
    printf("\n[supervisor] Shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            rec->stop_requested = 1;
            printf("[supervisor] Stopping '%s' (PID %d)\n", rec->id, rec->host_pid);
            kill(rec->host_pid, SIGTERM);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    usleep(2000000);

    /* Force kill stragglers */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = ctx->containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            printf("[supervisor] Force killing '%s' (PID %d)\n", rec->id, rec->host_pid);
            kill(rec->host_pid, SIGKILL);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Reap all children */
    while (waitpid(-1, NULL, WNOHANG) > 0) ;

    /* Shutdown logging pipeline — signal, then join (flushes remaining) */
    bounded_buffer_begin_shutdown(&ctx->log_buffer);
    printf("[supervisor] Waiting for logger thread to flush...\n");
    pthread_join(ctx->logger_thread, NULL);

    /* Close server socket */
    if (ctx->server_fd >= 0) {
        close(ctx->server_fd);
        unlink(CONTROL_PATH);
    }
    if (ctx->monitor_fd >= 0)
        close(ctx->monitor_fd);

    /* Free all container records */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = ctx->containers;
    while (rec) {
        container_record_t *next = rec->next;
        free(rec);
        rec = next;
    }
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);

    bounded_buffer_destroy(&ctx->log_buffer);
    pthread_mutex_destroy(&ctx->metadata_lock);
    printf("[supervisor] Cleanup complete.\n");
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Supervisor main loop
 * ═══════════════════════════════════════════════════════════════════════ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    (void)rootfs;
    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    printf("╔══════════════════════════════════════════════╗\n");
    printf("║    Container Runtime Supervisor Starting     ║\n");
    printf("╚══════════════════════════════════════════════╝\n");
    printf("[supervisor] PID: %d\n", getpid());

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* 1) Open /dev/container_monitor */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] Warning: cannot open /dev/container_monitor: %s\n"
                        "             (memory monitoring disabled)\n", strerror(errno));
    else
        printf("[supervisor] Kernel monitor connected\n");

    /* 2) Create UNIX domain socket (Path B: control channel) */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    unlink(CONTROL_PATH);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(ctx.server_fd); return 1;
    }
    chmod(CONTROL_PATH, 0666);
    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen"); close(ctx.server_fd); return 1;
    }
    printf("[supervisor] Listening on %s\n", CONTROL_PATH);

    /* 3) Install signal handlers */
    struct sigaction sa_chld = { .sa_handler = sigchld_handler, .sa_flags = SA_RESTART | SA_NOCLDSTOP };
    sigemptyset(&sa_chld.sa_mask);
    sigaction(SIGCHLD, &sa_chld, NULL);

    struct sigaction sa_term = { .sa_handler = shutdown_handler };
    sigemptyset(&sa_term.sa_mask);
    sigaction(SIGINT, &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* 4) Spawn the logger thread */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger"); return 1;
    }
    printf("[supervisor] Logger thread started\n");
    printf("[supervisor] Ready. Use './engine <command>' in another terminal.\n\n");

    /* 5) Supervisor event loop */
    while (!ctx.should_stop) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

        int ret = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("select"); break;
        }
        if (ret == 0) continue; /* Timeout */

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept"); continue;
        }

        /* Read control request */
        control_request_t req;
        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != sizeof(req)) {
            close(client_fd);
            continue;
        }

        printf("[supervisor] Received command: %d id='%s'\n", req.kind, req.container_id);

        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        switch (req.kind) {
        case CMD_START:
            handle_start(&ctx, &req, &resp);
            break;
        case CMD_RUN:
            handle_run(&ctx, &req, &resp);
            break;
        case CMD_PS:
            handle_ps(&ctx, &resp);
            break;
        case CMD_LOGS:
            handle_logs(&ctx, &req, &resp);
            break;
        case CMD_STOP:
            handle_stop(&ctx, &req, &resp);
            break;
        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "ERROR: Unknown command");
            break;
        }

        if (write(client_fd, &resp, sizeof(resp)) < 0)
            perror("write to client");
        close(client_fd);
    }

    supervisor_cleanup(&ctx);
    g_ctx = NULL;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  CLI client — sends structured control_request_t to supervisor
 * ═══════════════════════════════════════════════════════════════════════ */

static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s\n"
                        "Start it with: sudo ./engine supervisor ./rootfs-base\n",
                CONTROL_PATH);
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) < 0) {
        perror("write"); close(sock); return 1;
    }

    control_response_t resp;
    ssize_t n = read(sock, &resp, sizeof(resp));
    if (n > 0)
        printf("%s\n", resp.message);

    close(sock);

    /* For 'run', return the container exit code */
    if (req->kind == CMD_RUN)
        return resp.status;

    return (resp.status == 0) ? 0 : 1;
}

/* ═══════════════════════════════════════════════════════════════════════
 *  CLI command builders (from boilerplate)
 * ═══════════════════════════════════════════════════════════════════════ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr, "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr, "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ═══════════════════════════════════════════════════════════════════════
 *  Main
 * ═══════════════════════════════════════════════════════════════════════ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run") == 0)   return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps") == 0)    return cmd_ps();
    if (strcmp(argv[1], "logs") == 0)  return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop") == 0)  return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
