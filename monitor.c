/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * Completed TODOs:
 *   1. Linked-list node struct
 *   2. Global list + mutex
 *   3. Periodic monitoring in timer callback
 *   4. Register (add entry)
 *   5. Unregister (remove entry)
 *   6. Cleanup on module unload
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1: Define your linked-list node struct.
 *
 * Tracks PID, container ID, soft/hard limits, whether the
 * soft-limit warning was already emitted, and list linkage.
 * ============================================================== */
struct monitored_entry {
    struct list_head list;
    pid_t            pid;
    char             container_id[MONITOR_NAME_LEN];
    unsigned long    soft_limit_bytes;
    unsigned long    hard_limit_bytes;
    int              soft_warned;
};

/* ==============================================================
 * TODO 2: Declare the global monitored list and a lock.
 *
 * We use a mutex because all code paths that access the list
 * (ioctl handlers and the timer callback) run in process context
 * where sleeping is permitted. A mutex provides simpler semantics
 * than a spinlock for this use case. The timer callback is
 * scheduled via mod_timer which runs in softirq context on some
 * kernels, but we use a workqueue-safe timer_setup and the
 * callback itself only does lightweight work, so mutex_trylock
 * is used in the timer to avoid sleeping in atomic context.
 *
 * README justification: mutex was chosen because ioctl handlers
 * run in process context (user syscall) where sleeping is safe.
 * The timer callback uses mutex_trylock to skip the check cycle
 * rather than risk sleeping in softirq context — this is a safe
 * tradeoff since the timer fires every second and missing one
 * cycle is acceptable.
 * ============================================================== */
static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(list_mutex);

/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    /* ==============================================================
     * TODO 3: Implement periodic monitoring.
     *
     * - iterate through tracked entries safely
     * - remove entries for exited processes
     * - emit soft-limit warning once per entry
     * - enforce hard limit and then remove the entry
     * - avoid use-after-free while deleting during iteration
     * ============================================================== */
    struct monitored_entry *entry, *tmp;

    (void)t;

    /*
     * Use mutex_trylock: if the mutex is held by an ioctl handler,
     * skip this cycle. The timer fires every second so missing one
     * check is harmless.
     */
    if (!mutex_trylock(&list_mutex))
        goto reschedule;

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        long rss = get_rss_bytes(entry->pid);

        /* Process no longer exists — remove stale entry */
        if (rss < 0) {
            printk(KERN_INFO
                   "[container_monitor] PID %d exited, removing entry container=%s\n",
                   entry->pid, entry->container_id);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /* Hard limit check (takes priority over soft) */
        if (entry->hard_limit_bytes > 0 &&
            (unsigned long)rss > entry->hard_limit_bytes) {
            kill_process(entry->container_id, entry->pid,
                         entry->hard_limit_bytes, rss);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /* Soft limit check (warn once) */
        if (entry->soft_limit_bytes > 0 &&
            (unsigned long)rss > entry->soft_limit_bytes &&
            !entry->soft_warned) {
            log_soft_limit_event(entry->container_id, entry->pid,
                                 entry->soft_limit_bytes, rss);
            entry->soft_warned = 1;
        }
    }

    mutex_unlock(&list_mutex);

reschedule:
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        /* ==============================================================
         * TODO 4: Add a monitored entry.
         *
         * - allocate and initialize one node from req
         * - validate allocation and limits
         * - insert into the shared list under the chosen lock
         * ============================================================== */
        struct monitored_entry *entry;

        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        if (req.pid <= 0)
            return -EINVAL;

        if (req.soft_limit_bytes > req.hard_limit_bytes && req.hard_limit_bytes > 0)
            return -EINVAL;

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid = req.pid;
        strncpy(entry->container_id, req.container_id,
                sizeof(entry->container_id) - 1);
        entry->container_id[sizeof(entry->container_id) - 1] = '\0';
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned = 0;
        INIT_LIST_HEAD(&entry->list);

        mutex_lock(&list_mutex);

        /* Check for duplicate PID — update limits if already registered */
        {
            struct monitored_entry *existing;
            list_for_each_entry(existing, &monitored_list, list) {
                if (existing->pid == req.pid) {
                    existing->soft_limit_bytes = req.soft_limit_bytes;
                    existing->hard_limit_bytes = req.hard_limit_bytes;
                    existing->soft_warned = 0;
                    mutex_unlock(&list_mutex);
                    kfree(entry);
                    printk(KERN_INFO
                           "[container_monitor] Updated existing entry for PID %d\n",
                           req.pid);
                    return 0;
                }
            }
        }

        list_add_tail(&entry->list, &monitored_list);
        mutex_unlock(&list_mutex);

        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /* ==============================================================
     * TODO 5: Remove a monitored entry on explicit unregister.
     *
     * - search by PID (primary key)
     * - remove the matching entry safely if found
     * - return -ENOENT if not found
     * ============================================================== */
    {
        struct monitored_entry *entry, *tmp;
        int found = 0;

        mutex_lock(&list_mutex);
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            if (entry->pid == req.pid) {
                list_del(&entry->list);
                kfree(entry);
                found = 1;
                break;
            }
        }
        mutex_unlock(&list_mutex);

        if (found) {
            printk(KERN_INFO
                   "[container_monitor] Unregistered PID %d container=%s\n",
                   req.pid, req.container_id);
            return 0;
        }
    }

    return -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    del_timer_sync(&monitor_timer);

    /* ==============================================================
     * TODO 6: Free all remaining monitored entries.
     * ============================================================== */
    {
        struct monitored_entry *entry, *tmp;

        mutex_lock(&list_mutex);
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            printk(KERN_INFO
                   "[container_monitor] Cleanup: removing PID %d container=%s\n",
                   entry->pid, entry->container_id);
            list_del(&entry->list);
            kfree(entry);
        }
        mutex_unlock(&list_mutex);
    }

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
