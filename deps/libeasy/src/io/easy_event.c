#include <easy_io.h>
#include <easy_event.h>

static void easy_event_add(easy_baseth_t *th, easy_event_t *event);

/**
 * create task
 */
easy_task_t *easy_task_flags_create(easy_task_handler_pt *handler, void *data, int flags)
{
    easy_pool_t  *pool;
    easy_task_t *task;

    pool = easy_pool_create(512);

    if (pool == NULL) {
        return NULL;
    }

    task = (easy_task_t *)easy_pool_calloc(pool, sizeof(easy_task_t));

    if (task == NULL) {
       easy_pool_destroy(pool);
       return NULL;
    }

    task->pool = pool;
    task->handler = handler;
    task->data = data;
    task->th = easy_baseth_self;

    if (flags | EASY_TASK_FLAGS_LOCK) {
        task->lock = easy_lock_create();
    } else {
        task->lock = NULL;
    }

    return task;
}

easy_task_t *easy_task_create(easy_task_handler_pt *handler, void *data)
{
    return easy_task_flags_create(handler, data, EASY_TASK_FLAGS_NONE);
}

easy_task_t *easy_task_lock_create(easy_task_handler_pt *handler, void *data)
{
    return easy_task_flags_create(handler, data, EASY_TASK_FLAGS_LOCK);
}

void easy_task_set_handler(easy_task_t *task, easy_task_handler_pt *handler)
{
    task->handler = handler;
}

void easy_task_set_data(easy_task_t *task, void *data)
{
    task->data = data;
}

void easy_task_destroy(easy_task_t *task)
{
    if (task) {
        if (task->lock) {
            easy_lock_destroy(task->lock);
            task->lock = NULL;
        }

        if (task->pool) {
            easy_pool_destroy(task->pool);
        }
    }
}

void easy_event_process(easy_baseth_t *th)
{
    easy_event_t        *event, *event2;
    easy_list_t         event_list;
    easy_lock_t         *lock;

    assert(th == easy_baseth_self);

    easy_spin_lock(&th->thread_lock);
    easy_list_movelist(&th->event_list, &event_list);
    easy_spin_unlock(&th->thread_lock);
    ev_timer_stop(th->loop, &th->event_timer);

    // foreach
    easy_list_for_each_entry_safe(event, event2, &event_list, event_node) {
        easy_list_del(&event->event_node);

        lock = event->lock;
        if (lock == NULL || (lock && easy_trylock(&lock->lock))) {
            event->lock = NULL;
            if (lock == NULL) assert(event->alloc_pool);

            if (event->task->handler && event->cancel == 0) {
                event->task->event = NULL;
                (event->task->handler)(event->task);
            }

            if (lock) {
                easy_unlock(&lock->lock);
                easy_lock_dereference(lock);
                EASY_EVENT_FREE(event);
            }
        } else {
           easy_event_add(th, event);
        }
    }
}

void easy_event_thread_process(struct ev_loop *loop, ev_async *w, int revents)
{
    easy_baseth_t *baseth;

    baseth = (easy_baseth_t *) w->data;
    easy_event_process(baseth);
}

void easy_event_timer_process(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_baseth_t *baseth;

    baseth = (easy_baseth_t *) w->data;
    easy_event_process(baseth);
}

static void easy_event_add(easy_baseth_t *th, easy_event_t *event)
{
    easy_spin_lock(&th->thread_lock);
    easy_list_add_tail(&event->event_node, &th->event_list);
    easy_spin_unlock(&th->thread_lock);

    if (th == easy_baseth_self) {
        ev_timer_start(th->loop, &th->event_timer);
    } else {
        ev_async_send(th->loop, &th->event_watcher);
    }
}

void *easy_task_add(easy_baseth_t *th, easy_task_t *task)
{
    easy_event_t *event;

    if (task == NULL || th == NULL) {
        easy_error_log("easy_task_add: thread(%p) or task(%p) is null!\n",
                       th, task);
        return NULL;
    }

    if (task->lock) {
        event = EASY_EVENT_ALLOC();
        event->lock = easy_lock_reference(task->lock);
    } else {
        event = easy_pool_calloc(task->pool, sizeof(easy_event_t));
    }

    event->task = task;
    easy_list_init(&event->event_node);
    task->event = event;

    easy_event_add(th, event);

    return event;
}

int easy_task_cancel(easy_task_t *task)
{
    int ret = EASY_ABORT;

    if (task == NULL || task->lock == NULL) {
        return EASY_ERROR;
    }

    easy_spin_lock(&task->lock->lock);
    if (task->event) {
        task->event->cancel = 1;
        ret = EASY_OK;
    }
    easy_spin_unlock(&task->lock->lock);

    return ret;
}

