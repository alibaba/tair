#include "easy_baseth_pool.h"
#include "easy_io.h"
#include "easy_simple_handler.h"
#include "easy_event.h"
#include <easy_test.h>

typedef struct {
    easy_atomic32_t  num;
    easy_atomic32_t  seq;
    int              casenum;
    easy_baseth_t *th;
} easy_task_test_data_t;

#define IO_THREAD_COUNT 2
#define WORK_THREAD_COUNT 2
#define TEST_CANCEL_CNT 10000
enum {
    TEST_CASE1,
    TEST_CASE2,
    TEST_CASE3,
    TEST_EVENT_COUNT,
};
static easy_task_test_data_t easy_test_event_data[TEST_EVENT_COUNT];
static easy_task_t *easy_test_cancel_data[TEST_CANCEL_CNT];
static easy_thread_pool_t *easy_test_event_threads;

static int easy_task_test_case1_handler(easy_task_t *task);
static int easy_task_test_case2_handler(easy_task_t *task);
static int easy_task_test_case3_handler(easy_task_t *task);
#define EASY_TASK_CHECK_CASE(test_data, self, casen) do { \
         EXPECT_EQ(test_data->casenum, casen);   \
         test_data->num += easy_task_test_thread_num(self, test_data->seq); \
         test_data->seq ++;} while(0)
#define easy_test_debug(format, args...) do { \
        if (0) easy_error_log(format, ## args);} while (0)

// globe
easy_atomic_t easy_task_test_case_count = -1;
easy_atomic_t easy_task_test_case3_count = 0;

static int easy_task_test_thread_num(easy_baseth_t *th, int seq)
{
    return th->iot * 1000000 + th->idx * 100000 + seq;
}

/**
 * 1. thread A create event, thread B process and free task.
 */
void easy_event_test_case1(easy_baseth_t *th)
{
    easy_baseth_t          *eth;
    easy_task_t            *task;
    easy_task_test_data_t *test_data;
    easy_baseth_t         *self;

    test_data = &easy_test_event_data[TEST_CASE1];
    test_data->th = th;
    test_data->casenum = TEST_CASE1;

    self = easy_baseth_self;
    // event from thread 0 to thread 1.
    eth = easy_thread_pool_index(th->eio->io_thread_pool, 1);
    test_data->num = easy_task_test_thread_num(eth, test_data->seq);

    task = easy_task_create(easy_task_test_case1_handler, test_data);
    easy_test_debug("easy_task_test_case1_start: idx: %d, self idx: %d,"
                    " task: %p\n", test_data->th->idx, self->idx, task);
    easy_task_add(eth, task);
}

static int easy_task_test_case1_handler(easy_task_t *task)
{
    easy_baseth_t *self;
    easy_task_test_data_t *test_data;

    self = easy_baseth_self;
    test_data = task->data;
    easy_test_debug("easy_task_test_case1_handler: idx: %d, self idx: %d\n",
                    test_data->th->idx, self->idx);

    EXPECT_EQ(test_data->casenum, TEST_CASE1);
    EXPECT_EQ(test_data->num, easy_task_test_thread_num(self, test_data->seq));

    easy_atomic_inc(&easy_task_test_case_count);

    easy_task_destroy(task);
    return EASY_OK;
}

/**
 * 2. thread A create event, thread B process, then thread B add event
 *    to thread C, thread C process, and thread C add event to thread A
 *    run callback.
 */
void easy_event_test_case2(easy_baseth_t *th)
{
    easy_baseth_t          *eth;
    easy_task_t            *task;
    easy_task_test_data_t *test_data;
    easy_baseth_t         *self;

    test_data = &easy_test_event_data[TEST_CASE2];
    test_data->th = th;
    test_data->casenum = TEST_CASE2;
    test_data->num = 0;

    self = easy_baseth_self;
    // event from thread 0 to thread 1, iot: 1.
    eth = easy_thread_pool_index(th->eio->io_thread_pool, 1);

    task = easy_task_create(easy_task_test_case2_handler, test_data);
    easy_test_debug("easy_task_test_case2_start: idx: %d, self idx: %d, "
                    "task: %p\n", test_data->th->idx, self->idx, task);
    easy_task_add(eth, task);
}

static int easy_task_test_case2_callback(easy_task_t *task)

{
    easy_baseth_t *self;
    easy_task_test_data_t *test_data;

    self = easy_baseth_self;
    test_data = task->data;
    easy_test_debug("easy_task_test_case2_callback: idx: %d, self idx: %d, iot:"
                    " %d, task: %p\n", test_data->th->idx, self->idx, self->iot,
                    task);
    EASY_TASK_CHECK_CASE(test_data, self, TEST_CASE2);
    EXPECT_EQ(test_data->num, 3200006);

    easy_atomic_inc(&easy_task_test_case_count);
    easy_task_destroy(task);
    return EASY_OK;
}

static int easy_task_test_case2_handler2(easy_task_t *task)
{
    easy_baseth_t         *eth;
    easy_baseth_t         *self;
    easy_task_test_data_t *test_data;

    self = easy_baseth_self;
    test_data = task->data;
    easy_test_debug("easy_task_test_case2_handler2: idx: %d, self idx: %d, iot,"
                    " %d, task: %p\n", test_data->th->idx, self->idx, self->iot,
                    task);

    EASY_TASK_CHECK_CASE(test_data, self, TEST_CASE2);
    EXPECT_EQ(test_data->num, 2200003);

    easy_task_set_handler(task, easy_task_test_case2_callback);
    eth = easy_thread_pool_index(self->eio->io_thread_pool, 0);
    easy_task_add(eth, task);
    return EASY_OK;
}

static int easy_task_test_case2_handler1(easy_task_t *task)
{
    easy_baseth_t         *eth;
    easy_baseth_t         *self;
    easy_task_test_data_t *test_data;

    self = easy_baseth_self;
    test_data = task->data;
    easy_test_debug("easy_task_test_case2_handler1: idx: %d, self idx: %d, iot,"
                    " %d, task: %p\n", test_data->th->idx, self->idx, self->iot,
                    task);

    EASY_TASK_CHECK_CASE(test_data, self, TEST_CASE2);
    EXPECT_EQ(test_data->num, 2200001);

    easy_task_set_handler(task, easy_task_test_case2_handler2);
    eth = easy_thread_pool_index(easy_test_event_threads, 0);
    easy_task_add(eth, task);
    return EASY_OK;
}

static int easy_task_test_case2_handler(easy_task_t *task)
{
    easy_baseth_t         *self;
    easy_task_test_data_t *test_data;

    self = easy_baseth_self;
    test_data = task->data;
    easy_test_debug("easy_task_test_case2_handler: idx: %d, self idx: %d, iot, %d,"
                    " task: %p\n", test_data->th->idx, self->idx, self->iot, task);

    EASY_TASK_CHECK_CASE(test_data, self, TEST_CASE2);
    EXPECT_EQ(test_data->num, 1100000);

    easy_task_set_handler(task, easy_task_test_case2_handler1);
    easy_task_add(self, task);
    return EASY_OK;
}

/**
 * 3. test cancel
 */
void easy_event_test_case3(easy_baseth_t *th)
{
    easy_baseth_t          *eth;
    easy_task_t            *task;
    easy_task_test_data_t  *test_data, *tdata;
    int                    i, ret;

    // event from thread 0 to thread 1.
    eth = easy_thread_pool_index(th->eio->io_thread_pool, 1);
    tdata = &easy_test_event_data[TEST_CASE3];

    for (i = 1; i <= TEST_CANCEL_CNT; i ++) {
        task = easy_task_lock_create(easy_task_test_case3_handler, NULL);
        test_data = easy_pool_calloc(task->pool, sizeof(easy_task_test_data_t));
        test_data->th = th;
        test_data->casenum = TEST_CASE3;
        test_data->seq = i;
        easy_task_set_data(task, test_data);
        easy_test_cancel_data[i-1] = task;
        easy_task_add(eth, task);
    }

    for (i = TEST_CANCEL_CNT; i > 0; i --) {
        ret = easy_task_cancel(easy_test_cancel_data[i-1]);
        if (ret == EASY_OK) {
            // cancel success
            easy_atomic32_inc(&tdata->seq);
            easy_atomic_inc(&easy_task_test_case3_count);
        } else {
            test_data = easy_test_cancel_data[i-1]->data;
            EXPECT_EQ(test_data->num, i);
        }
    }

    easy_atomic_inc(&easy_task_test_case_count);
}

static int easy_task_test_case3_handler(easy_task_t *task)
{
    easy_task_test_data_t *test_data, *tdata;

    test_data = task->data;
    EXPECT_EQ(test_data->casenum, TEST_CASE3);
    test_data->num = test_data->seq;
    tdata = &easy_test_event_data[TEST_CASE3];
    // no cancel
    easy_atomic32_inc(&tdata->num);
    easy_atomic_inc(&easy_task_test_case3_count);

    usleep(1);

    return EASY_OK;
}

static void easy_test_timer_create_event(struct ev_loop *loop, ev_timer *w, int revents)
{
    easy_baseth_t *th;

    th = (easy_baseth_t *) ev_userdata (loop);
    easy_event_test_case1(th);
    easy_event_test_case2(th);
    easy_event_test_case3(th);
}

static void easy_test_timer_check_stop(struct ev_loop *loop, ev_timer *w, int revents)
{
    int           i;

    easy_task_test_data_t *tdata = &easy_test_event_data[TEST_CASE3];

    if (easy_task_test_case3_count >= TEST_CANCEL_CNT) {
        easy_atomic_inc(&easy_task_test_case_count);
        easy_task_test_case3_count = -1;
    }

    if (easy_task_test_case_count < 3) {
        easy_test_debug("easy_test_timer_check_stop: easy_task_test_case3_count:"
                       " %d, ""cancel: %d, no cancel: %d, easy_task_test_case_c"
                       "ount: %d\n", easy_task_test_case3_count, tdata->seq,
                       tdata->num, easy_task_test_case_count);
        return;
    }

    for (i = 0; i < TEST_CANCEL_CNT; i ++) {
        easy_task_destroy(easy_test_cancel_data[i]);
        easy_test_cancel_data[i] = NULL;
    }

    easy_error_log("easy_test_timer_check: case3 cancel success: %d, "
                     "no cancel: %d\n", tdata->seq, tdata->num);

    easy_io_stop();
}

static void easy_set_ioth_user_timer(easy_io_t *eio)
{
    easy_io_thread_t  *ioth;

    easy_thread_pool_for_each(ioth, eio->io_thread_pool, 0) {
        ioth->user_process = NULL;
        if (ioth->idx == 0) {
            ev_timer_init (&ioth->user_timer, easy_test_timer_create_event, 0, 0);
            ev_timer_start (ioth->loop, &ioth->user_timer);
        } else {
            ev_timer_init (&ioth->user_timer, easy_test_timer_check_stop, 1, 1);
            ev_timer_start (ioth->loop, &ioth->user_timer);
        }
        easy_list_init(&ioth->user_list);
    }
}

TEST(easy_event, add)
{
    easy_io_t               *eio;
    int                     ret;

    // create
    eio = easy_io_create(IO_THREAD_COUNT);
    EXPECT_TRUE(eio);
    easy_test_event_threads = easy_thread_pool_create(eio, WORK_THREAD_COUNT, NULL, NULL);

    easy_set_ioth_user_timer(eio);

    ret = easy_io_start();
    EXPECT_EQ(ret, EASY_OK);

    easy_io_wait();

    easy_io_destroy();
}
