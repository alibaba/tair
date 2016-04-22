#ifndef EASY_EVENT_H
#define EASY_EVENT_H

#include <easy_io_struct.h>
#include "easy_lock.h"

typedef struct easy_event_t easy_event_t;
typedef struct easy_task_t easy_task_t;
typedef int (easy_task_handler_pt)(easy_task_t *task);

#define EASY_EVENT_DEFINE              \
    easy_task_t           *task;       \
    easy_list_t           event_node;  \
    easy_lock_t           *lock;       \
    uint8_t               cancel;      \
    uint8_t               alloc_pool;

#define EASY_TASK_DEFINE               \
    easy_pool_t           *pool;       \
    easy_task_handler_pt  *handler;    \
    void                  *data;       \
    easy_event_t          *event;      \
    easy_lock_t           *lock;       \
    easy_baseth_t         *th;

enum {
    EASY_TASK_FLAGS_NONE = 0,
    EASY_TASK_FLAGS_LOCK = 1,
};

struct easy_event_t {
    EASY_EVENT_DEFINE
};

struct easy_task_t {
    EASY_TASK_DEFINE
};

easy_task_t *easy_task_create(easy_task_handler_pt *handler, void *data);
easy_task_t *easy_task_lock_create(easy_task_handler_pt *handler, void *data);
void easy_task_set_handler(easy_task_t *task, easy_task_handler_pt *handler);
void easy_task_set_data(easy_task_t *task, void *data);
void easy_task_destroy(easy_task_t *task);
void *easy_task_add(easy_baseth_t *th, easy_task_t *task);
int easy_task_cancel(easy_task_t *task);

#define EASY_EVENT_ALLOC() calloc(1, sizeof(easy_event_t))
#define EASY_EVENT_FREE(event) free(event)
void easy_event_process(easy_baseth_t *th);
void easy_event_thread_process(struct ev_loop *loop, ev_async *w, int revents);
void easy_event_timer_process(struct ev_loop *loop, ev_timer *w, int revents);

#endif
