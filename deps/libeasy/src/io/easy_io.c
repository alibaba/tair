#include "easy_io.h"
#include "easy_baseth_pool.h"
#include "easy_connection.h"
#include "easy_message.h"
#include "easy_request.h"
#include "easy_file.h"
#include "easy_client.h"
#include "easy_socket.h"
#include <sched.h>

// 全局变量
char                    *easy_build_time = "VERSION: " PACKAGE_VERSION ", BUILD:" __DATE__ " " __TIME__;
easy_io_t               easy_io_var = {NULL};
easy_atomic_t           easy_io_list_lock = 0;
easy_list_t             easy_io_list_var = EASY_LIST_HEAD_INIT(easy_io_list_var);

static void *easy_io_on_thread_start(void *args);
static void easy_io_on_uthread_start(void *args);
static void easy_io_on_uthread_evstart(void *args);
static void easy_io_uthread_invoke(struct ev_loop *loop);
static void easy_io_thread_destroy(easy_io_thread_t *ioth);
static void easy_io_stat_process(struct ev_loop *loop, ev_timer *w, int revents);
static void easy_io_print_status(easy_io_t *eio);
static void easy_signal_handler(int sig);

/**
 * 初始化easy_io
 */
easy_io_t *easy_eio_create(easy_io_t *eio, int io_thread_count)
{
    easy_io_thread_t        *ioth;
    easy_thread_pool_t      *tp;
    easy_pool_t             *pool;
    int                     v;

    if (eio != NULL && eio->pool != NULL)
        return eio;

    if (io_thread_count <= 0 || io_thread_count > EASY_MAX_THREAD_CNT)
        io_thread_count = sysconf(_SC_NPROCESSORS_CONF);

    if ((pool = easy_pool_create(0)) == NULL)
        return NULL;

    // 分配空间
    if (eio == NULL && (eio = (easy_io_t *)easy_pool_alloc(pool, sizeof(easy_io_t))) == NULL) {
        easy_pool_destroy(pool);
        return NULL;
    }

    // 初始化
    memset(eio, 0, sizeof(easy_io_t));
    eio->pool = pool;
    eio->io_thread_count = io_thread_count;
    eio->start_time = ev_time();
    easy_list_init(&eio->thread_pool_list);
    ev_set_allocator(easy_pool_realloc);

    //create summary buffer
    eio->eio_summary = easy_summary_create();

    // 创建IO线程池
    tp = easy_baseth_pool_create(eio, io_thread_count, sizeof(easy_io_thread_t));

    if (tp == NULL)
        goto error_exit;

    eio->io_thread_pool = tp;
    eio->tcp_nodelay = 1;
    eio->tcp_defer_accept = 1;
    eio->do_signal = 1;
    eio->send_qlen = EASY_CONN_DOING_REQ_CNT;
    eio->queue_size = EASY_IOTH_DOING_REQ_CNT;
    eio->support_ipv6 = easy_socket_support_ipv6();

    // 初始化线程池
    easy_thread_pool_for_each(ioth, tp, 0) {
        easy_list_init(&ioth->connected_list);
        v = offsetof(easy_client_t, client_list_node);
        ioth->client_list = easy_hash_create(pool, EASY_MAX_CLIENT_CNT / io_thread_count, v);
        ioth->client_array = easy_array_create(sizeof(easy_client_t));

        // 起异步事件
        easy_list_init(&ioth->conn_list);
        easy_list_init(&ioth->session_list);
        easy_list_init(&ioth->request_list);

        ev_timer_init (&ioth->listen_watcher, easy_connection_on_listen, 0.0, 0.1);
        ioth->listen_watcher.data = ioth;
        ioth->iot = 1;

        // base thread init
        easy_baseth_init(ioth, tp, easy_io_on_thread_start, easy_connection_on_wakeup);
    }

    // 屏蔽掉SIGPIPE
    signal(SIGPIPE, SIG_IGN);


    // add to easy_io_list_var
    easy_spin_lock(&easy_io_list_lock);
    easy_list_add_tail(&eio->eio_list_node, &easy_io_list_var);
    easy_spin_unlock(&easy_io_list_lock);

    return eio;
error_exit:
    easy_eio_destroy(eio);
    return NULL;
}

/**
 * 把一个easy_io删除掉
 */
void easy_eio_destroy(easy_io_t *eio)
{
    easy_pool_t             *pool;
    easy_io_thread_t        *ioth;
    easy_thread_pool_t      *tp;
    easy_listen_t           *l;

    if (eio == NULL)
        return;

    // 从easy_io_list_var去掉
    easy_spin_lock(&easy_io_list_lock);

    eio->stoped = 1;

    if (eio->eio_list_node.prev) easy_list_del(&eio->eio_list_node);

    easy_spin_unlock(&easy_io_list_lock);

    // close listen
    for (l = eio->listen; l; l = l->next) {
        if (l->fd >= 0) close(l->fd);
    }

    // destroy io_thread
    if (eio->io_thread_pool) {
        easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
            easy_io_thread_destroy(ioth);
        }
    }

    // destroy baseth pool
    easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node) {
        easy_baseth_pool_destroy(tp);
    }

    easy_summary_destroy(eio->eio_summary);
    pool = eio->pool;
    memset(eio, 0, sizeof(easy_io_t));
    easy_pool_destroy(pool);

    easy_debug_log("easy_eio_destroy, eio=%p\n", eio);
}

/**
 * 开始easy_io, 第一个线程用于listen, 后面的线程用于处理
 */
int easy_eio_start(easy_io_t *eio)
{
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp;

    // 没初始化pool
    if (eio == NULL || eio->pool == NULL)
        return EASY_ERROR;

    if (eio->started)
        return EASY_ABORT;

    if (eio->tcp_nodelay) eio->tcp_cork = 0;

    if (eio->do_signal) {
        struct sigaction        sigact;
        memset(&sigact, 0, sizeof(struct sigaction));
        sigact.sa_handler = easy_signal_handler;
        sigemptyset(&sigact.sa_mask);
        sigaction(39, &sigact, NULL);
        sigact.sa_flags = SA_RESETHAND;
        sigaction(SIGINT, &sigact, NULL);
        sigaction(SIGTERM, &sigact, NULL);
    }

    // 起线程
    easy_spin_lock(&eio->lock);
    easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node) {
        easy_thread_pool_for_each(th, tp, 0) {
            pthread_create(&(th->tid), NULL, th->on_start, (void *)th);
        }
    }
    eio->started = 1;
    easy_spin_unlock(&eio->lock);

    return EASY_OK;
}

/**
 * 等待easy_io
 */
int easy_eio_wait(easy_io_t *eio)
{
    easy_baseth_t           *th;
    easy_thread_pool_t      *tp;

    // 等待thread
    easy_spin_lock(&eio->lock);
    easy_list_for_each_entry(tp, &eio->thread_pool_list, list_node) {
        easy_thread_pool_for_each(th, tp, 0) {
            if (th->tid && pthread_join(th->tid, NULL) == EDEADLK) {
                easy_fatal_log("easy_io_wait fatal, eio=%p, tid=%lx\n", eio, th->tid);
                abort();
            }

            th->tid = 0;
        }
    }
    easy_spin_unlock(&eio->lock);

    easy_debug_log("easy_io_wait exit, eio=%p\n", eio);

    return EASY_OK;
}

/**
 * 停止easy_io
 */
int easy_eio_stop(easy_io_t *eio)
{
    easy_thread_pool_t      *tp, *tp1;

    if (eio == NULL || eio->stoped)
        return EASY_ERROR;

    easy_debug_log("easy_eio_stop exit, eio=%p\n", eio);
    eio->stoped = 1;
    // 让thread停止
    easy_list_for_each_entry_safe(tp, tp1, &eio->thread_pool_list, list_node) {
        easy_baseth_pool_on_wakeup(tp);
    }
    easy_debug_log("easy_eio_stop exit, eio=%p\n", eio);

    return EASY_OK;
}

/**
 * 取线程的ev_loop,  要在easy_io_init后调用
 */
struct ev_loop *easy_eio_thread_loop(easy_io_t *eio, int index)
{
    easy_io_thread_t        *ioth;
    ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, index);
    return (ioth ? ioth->loop : NULL);
}

/**
 * 起处理速度定时器
 */
void easy_eio_stat_watcher_start(easy_io_t *eio, ev_timer *stat_watcher, double interval,
                                 easy_io_stat_t *iostat, easy_io_stat_process_pt *process)
{
    easy_io_thread_t        *ioth;

    memset(iostat, 0, sizeof(easy_io_stat_t));
    iostat->last_cnt = 0;
    iostat->last_time = eio->start_time;
    iostat->process = process;
    iostat->eio = eio;

    ioth = (easy_io_thread_t *)easy_thread_pool_index(eio->io_thread_pool, 0);
    ev_timer_init (stat_watcher, easy_io_stat_process, 0., interval);
    stat_watcher->data = iostat;
    ev_timer_start (ioth->loop, stat_watcher);
    easy_baseth_on_wakeup(ioth);
}

/**
 * 设置用户态线程开始
 */
void easy_eio_set_uthread_start(easy_io_t *eio, easy_io_uthread_start_pt *on_utstart, void *args)
{
    easy_io_thread_t        *ioth;

    eio->uthread_enable = 1;
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        ioth->on_utstart = on_utstart;
        ioth->ut_args = args;
    }
}
//////////////////////////////////////////////////////////////////////////////
/**
 * IO线程的回调程序
 */
static void *easy_io_on_thread_start(void *args)
{
    easy_listen_t           *l;
    easy_io_thread_t        *ioth;
    easy_io_t               *eio;
    easy_uthread_control_t  control;

    ioth = (easy_io_thread_t *) args;
    easy_baseth_self = (easy_baseth_t *) args;
    eio = ioth->eio;

    if (eio->block_thread_signal)
        pthread_sigmask(SIG_BLOCK, &eio->block_thread_sigset, NULL);

    // sched_setaffinity
    if (eio->affinity_enable) {
        static easy_atomic_t    cpuid = -1;
        int                     cpunum = sysconf(_SC_NPROCESSORS_CONF);
        cpu_set_t               mask;
        int                     idx = (easy_atomic_add_return(&cpuid, 1) & 0x7fffffff) % cpunum;
        CPU_ZERO(&mask);
        CPU_SET(idx, &mask);

        if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
            easy_error_log("sched_setaffinity error: %d (%s), cpuid=%d\n", errno, strerror(errno), cpuid);
        }
    }

    // 有listen
    if (eio->listen) {
        // 监听切换timer
        int                     ts = (eio->listen_all || eio->io_thread_count == 1);

        for (l = eio->listen; l; l = l->next) {
            if (l->reuseport || ts) {
                easy_connection_reuseport(eio, l, ioth->idx);
                ev_io_start(ioth->loop, &l->read_watcher[ioth->idx]);
            } else {
                ev_timer_start (ioth->loop, &ioth->listen_watcher);
            }
        }
    }

    // 允许使用用户线程
    if (eio->uthread_enable) {
        ev_set_invoke_pending_cb(ioth->loop, easy_io_uthread_invoke);
        easy_uthread_init(&control);
        easy_uthread_create(easy_io_on_uthread_evstart, ioth->loop, 256 * 1024);

        if (ioth->on_utstart) {
            easy_uthread_create(easy_io_on_uthread_start, ioth, EASY_UTHREAD_STACK);
            easy_baseth_on_wakeup(ioth);
        }

        easy_uthread_scheduler();
        easy_uthread_destroy();
    } else {
        if (ioth->on_utstart) ioth->on_utstart(ioth->ut_args);

        ev_run(ioth->loop, 0);
    }

    easy_baseth_self = NULL;

    easy_debug_log("pthread exit: %lx\n", pthread_self());

    return (void *)NULL;
}


/**
 * 用户态线程(uthread)的io函数
 */
static void easy_io_on_uthread_start(void *args)
{
    easy_io_thread_t        *ioth = (easy_io_thread_t *)args;

    if (ioth->on_utstart) (ioth->on_utstart)(ioth->ut_args);
}
static void easy_io_on_uthread_evstart(void *args)
{
    ev_run((struct ev_loop *)args, 0);
}

/**
 * 把io_thread_t释放掉
 */
static void easy_io_thread_destroy(easy_io_thread_t *ioth)
{
    easy_connection_t       *c, *c1;
    easy_session_t          *s, *s1;

    // session at ioth
    easy_spin_lock(&ioth->thread_lock);
    easy_list_for_each_entry_safe(s, s1, &ioth->session_list, session_list_node) {
        easy_list_del(&s->session_list_node);

        if (s->status && s->pool) easy_pool_destroy(s->pool);
    }
    // connection at ioth
    easy_list_for_each_entry_safe(c, c1, &ioth->conn_list, conn_list_node) {
        easy_connection_destroy(c);
    }
    // foreach connected_list
    easy_list_for_each_entry_safe(c, c1, &ioth->connected_list, conn_list_node) {
        easy_connection_destroy(c);
    }
    easy_spin_unlock(&ioth->thread_lock);

    easy_array_destroy(ioth->client_array);
}

/**
 * 统计处理函数
 */
static void easy_io_stat_process(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_io_stat_t          *iostat;
    ev_tstamp               last_time, t1, t2;
    int64_t                 last_cnt;
    easy_io_thread_t        *ioth;
    easy_io_t               *eio;

    iostat = (easy_io_stat_t *)w->data;
    eio = iostat->eio;

    // 统计当前值
    last_time = ev_now(loop);
    last_cnt = 0;
    int                     ql = 0;
    easy_connection_t       *c;
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        last_cnt += ioth->done_request_count;
        easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node) {
            ql += c->con_summary->doing_request_count;
        }
    }

    t1 = last_time - iostat->last_time;
    t2 = last_time - eio->start_time;

    // 保存起来
    iostat->last_speed = (last_cnt - iostat->last_cnt) / t1;
    iostat->total_speed = last_cnt / t2;

    iostat->last_cnt = last_cnt;
    iostat->last_time = last_time;

    if (iostat->process == NULL) {
        easy_info_log("cnt: %" PRId64 ", speed: %.2f, total_speed: %.2f, ql:%d\n",
                      iostat->last_cnt, iostat->last_speed, iostat->total_speed, ql);
    } else {
        (iostat->process)(iostat);
    }
}

static void easy_signal_handler(int sig)
{
    easy_io_t               *eio, *e1;

    if (easy_trylock(&easy_io_list_lock) == 0)
        return;

    if (sig == SIGINT || sig == SIGTERM) {
        easy_list_for_each_entry_safe(eio, e1, &easy_io_list_var, eio_list_node) {
            easy_eio_stop(eio);
        }
    } else if (sig == 39) {
        easy_list_for_each_entry_safe(eio, e1, &easy_io_list_var, eio_list_node) {
            easy_io_print_status(eio);
        }
    }

    easy_unlock(&easy_io_list_lock);
}

// uthread的处理函数
static void easy_io_uthread_invoke(struct ev_loop *loop)
{
    easy_baseth_t           *th = (easy_baseth_t *) ev_userdata (loop);

    // 是否退出
    if (th->eio->stoped) {
        ev_break(loop, EVBREAK_ALL);
        easy_uthread_stop();
        return;
    }

    ev_invoke_pending(loop);

    while(easy_uthread_yield() > 0);
}

/**
 * 打出connection信息
 */
static void easy_io_print_status(easy_io_t *eio)
{
    easy_connection_t       *c;
    easy_io_thread_t        *ioth;

    // foreach connected_list
    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        easy_info_log("thread:%d, doing: %d, done: %" PRIdFAST32 "\n", ioth->idx,
                      ioth->doing_request_count, ioth->done_request_count);
        easy_list_for_each_entry(c, &ioth->connected_list, conn_list_node) {
            easy_info_log("%d %s => doing: %d, done:%" PRIdFAST32 "\n", ioth->idx, easy_connection_str(c),
                          c->con_summary->doing_request_count, c->con_summary->done_request_count);
        }
    }
}
