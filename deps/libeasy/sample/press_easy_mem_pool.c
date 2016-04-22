#include <sys/time.h>
#include <pthread.h>
#include "easy_mem_pool.h"

#if __WORDSIZE == 64 && __GNUC__ >= 4
inline int64_t tv_to_microseconds(const struct timeval *tp)
{
    return (((int64_t) tp->tv_sec) * 1000000 + (int64_t) tp->tv_usec);
}

inline int64_t get_cur_microseconds_time(void)
{
    struct timeval          tp;
    gettimeofday(&tp, NULL);
    return tv_to_microseconds(&tp);
}

typedef void *(*realloc_pt)(void *ptr, size_t size);

realloc_pt              g_realloc = NULL;

typedef struct buf_t {
    struct buf_t            *next;
} buf_t;

void test_alloc_limit()
{
    easy_mempool_t          *pool = easy_mempool_create(0);

    int64_t                 i = 0;
    int64_t                 total = 0;
    int64_t                 limit = 1024 * 1024;
    buf_t                   *head = NULL;
    easy_mempool_set_memlimit(pool, limit);

    for (i = 0; total < limit; i++) {
        int32_t                 alloc_size = sizeof(buf_t) + i;
        buf_t                   *buf = (buf_t *)easy_mempool_alloc(pool, alloc_size);

        if (total + alloc_size > limit) {
            assert(NULL == buf);
        } else {
            assert(NULL != buf);
        }

        if (NULL == buf) {
            break;
        }

        total += alloc_size;
        buf->next = head;
        head = buf;
    }

    buf_t                   *iter = head;

    while (NULL != iter) {
        buf_t                   *next = iter->next;
        easy_mempool_free(pool, iter);
        iter = next;
    }

    easy_mempool_destroy(pool);
}

typedef struct buf_list_t {
    volatile buf_t          *head;
    buf_t                   *tail;
    int64_t                 total;
    pthread_spinlock_t      lock;
    easy_mempool_t          *pool;
} buf_list_t;

void *producer(void *data)
{
    void                    *tmp = easy_mempool_thread_realloc(NULL, 1);
    buf_list_t              *list = (buf_list_t *)data;
    int                     loop = 1;
    uint32_t                seed = 0;

    while (loop) {
        int32_t                 size = rand_r(&seed) % (32 * 1024) + sizeof(buf_t);
        //buf_t *buf = easy_mempool_alloc(list->pool, size);
        //buf_t *buf = easy_mempool_global_realloc(NULL, size);
        //buf_t *buf = easy_mempool_thread_realloc(NULL, size);
        //buf_t *buf = realloc(NULL, size);
        buf_t                   *buf = (buf_t *)g_realloc(NULL, size);

        if (NULL != buf) {
            //memset(buf, 0, sizeof(buf_t));
            pthread_spin_lock(&(list->lock));
            buf->next = NULL;

            if (NULL != list->tail) {
                list->tail->next = buf;
            }

            list->tail = buf;

            if (NULL == list->head) {
                list->head = list->tail;
            }

            list->total--;

            if (0 == list->total) {
                loop = 0;
            }

            pthread_spin_unlock(&(list->lock));
        } else {
            //usleep(1);
        }
    }

    easy_mempool_thread_realloc(tmp, 0);
    return NULL;
}

void *consummer(void *data)
{
    buf_list_t              *list = (buf_list_t *)data;
    int                     loop = 1;

    while (loop) {
        buf_t                   *buf = NULL;

        if (NULL == list->head
                && 0 != list->total) {
            continue;
        }

        pthread_spin_lock(&(list->lock));
        buf = (buf_t *)(list->head);

        if (NULL != buf) {
            list->head = buf->next;

            if (buf == list->tail) {
                list->tail = NULL;
            }
        } else {
            if (0 == list->total) {
                loop = 0;
            }
        }

        pthread_spin_unlock(&(list->lock));

        if (NULL != buf) {
            //easy_mempool_free(list->pool, buf);
            //easy_mempool_global_realloc(buf, 0);
            //easy_mempool_thread_realloc(buf, 0);
            //realloc(buf, 0);
            g_realloc(buf, 0);
        }
    }

    return NULL;
}

void test_thread_press(int64_t total, int64_t thread)
{
    buf_list_t              *list = (buf_list_t *)easy_malloc(sizeof(buf_list_t) * thread);
    pthread_t               *p_pd = (pthread_t *)easy_malloc(sizeof(pthread_t) * thread);
    pthread_t               *c_pd = (pthread_t *)easy_malloc(sizeof(pthread_t) * thread);
    easy_mempool_t          *pool = easy_mempool_create(0);
    int64_t                 i = 0;

    for (i = 0; i < thread; i++) {
        list[i].head = NULL;
        list[i].tail = NULL;
        list[i].total = total;
        pthread_spin_init(&(list[i].lock), PTHREAD_PROCESS_PRIVATE);
        list[i].pool = pool;
        pthread_create(&(p_pd[i]), NULL, producer, &list[i]);
    }

    for (i = 0; i < thread; i++) {
        pthread_create(&(c_pd[i]), NULL, consummer, &list[i]);
    }

    for (i = 0; i < thread; i++) {
        pthread_join(p_pd[i], NULL);
    }

    for (i = 0; i < thread; i++) {
        pthread_join(c_pd[i], NULL);
    }

    easy_mempool_destroy(pool);
    easy_free(c_pd);
    easy_free(p_pd);
}

int main(int argc, char **argv)
{
    if (3 > argc) {
        fprintf(stderr, "\n./press_easy_mem_pool [alloc_num_per_thread] [thread_num] [alloc_type]\n\n");
        fprintf(stderr, "alloc_type [mp_g_alloc|mp_t_alloc|sys_alloc]\n\n");
        exit(-1);
    }

    const char              *alloc_type = NULL;

    if (3 >= argc) {
        alloc_type = "mp_t_alloc";
    } else {
        alloc_type = argv[3];
    }

    if (0 == strcmp("mp_g_alloc", alloc_type)) {
        g_realloc = easy_mempool_global_realloc;
    } else if (0 == strcmp("mp_t_alloc", alloc_type)) {
        g_realloc = easy_mempool_thread_realloc;
    } else if (0 == strcmp("sys_alloc", alloc_type)) {
        g_realloc = realloc;
    } else {
        fprintf(stderr, "alloc_type must be [mp_g_alloc|mp_t_alloc|sys_alloc]\n");
        exit(-1);
    }

    test_alloc_limit();
    int64_t                 num_per_thread = atol(argv[1]);
    int64_t                 thread_num = atol(argv[2]) / 2;
    int64_t                 start = get_cur_microseconds_time();
    test_thread_press(num_per_thread, thread_num);
    int64_t                 timeu = get_cur_microseconds_time() - start;
    fprintf(stdout, "total alloc/free num:%ld timeu:%ld\n", num_per_thread * thread_num, timeu);
    fprintf(stdout, "      alloc_thread_num:%ld free_thread_num:%ld\n", thread_num, thread_num);
    fprintf(stdout, "total alloc/free num:%0.2lf/s\n\n", num_per_thread * thread_num * 1.0 / timeu * 1000000);
    return 0;
}
#else
int main(int argc, char **argv)
{
    fprintf(stderr, "__GNUC__: %d, __WORDSIZE: %d\n", __GNUC__, __WORDSIZE);
    return 0;
}
#endif

