#include "easy_pool.h"
#include <assert.h>
#include <stdio.h>

/**
 * 简单的内存池
 */

static void *easy_pool_alloc_block(easy_pool_t *pool, uint32_t size);
static void *easy_pool_alloc_large(easy_pool_t *pool, easy_pool_large_t *large, uint32_t size);
easy_pool_realloc_pt    easy_pool_realloc = easy_pool_default_realloc;

easy_pool_t *easy_pool_create(uint32_t size)
{
    easy_pool_t             *p;

    // 对齐
    size = easy_align(size + sizeof(easy_pool_t), EASY_POOL_ALIGNMENT);

    if ((p = (easy_pool_t *)easy_pool_realloc(NULL, size)) == NULL)
        return NULL;

    memset(p, 0, sizeof(easy_pool_t));
    p->last = (uint8_t *) p + sizeof(easy_pool_t);
    p->end = (uint8_t *) p + size;
    p->max = size - sizeof(easy_pool_t);
    p->current = p;

    return p;
}

// clear
void easy_pool_clear(easy_pool_t *pool)
{
    easy_pool_t             *p, *n;
    easy_pool_large_t       *l;

    // large
    for(l = pool->large; l; l = l->next) {
        easy_pool_realloc(l->data, 0);
    }

    // other page
    for(p = pool->next; p; p = n) {
        n = p->next;
        easy_pool_realloc(p, 0);
    }

    pool->large = NULL;
    pool->next = NULL;
    pool->current = pool;
    pool->failed = 0;
    pool->last = (uint8_t *) pool + sizeof(easy_pool_t);
}

void easy_pool_destroy(easy_pool_t *pool)
{
    easy_pool_clear(pool);
    assert(pool->ref == 0);
    easy_pool_realloc(pool, 0);
}

void *easy_pool_alloc_ex(easy_pool_t *pool, uint32_t size, int align)
{
    uint8_t                 *m;
    easy_pool_t             *p;
    int                     flags, dsize;

    // init
    dsize = 0;
    flags = pool->flags;

    if (size > pool->max) {
        dsize = size;
        size = sizeof(easy_pool_large_t);
    }

    if (unlikely(flags)) easy_spin_lock(&pool->tlock);

    p = pool->current;

    do {
        m = easy_align_ptr(p->last, align);

        if (m + size <= p->end) {
            p->last = m + size;
            break;
        }

        p = p->next;
    } while (p);

    // 重新分配一块出来
    if (p == NULL) {
        m = (uint8_t *)easy_pool_alloc_block(pool, size);
    }

    if (m && dsize) {
        m = (uint8_t *)easy_pool_alloc_large(pool, (easy_pool_large_t *)m, dsize);
    }

    if (unlikely(flags)) easy_spin_unlock(&pool->tlock);

    return m;
}

void *easy_pool_calloc(easy_pool_t *pool, uint32_t size)
{
    void                    *p;

    if ((p = easy_pool_alloc_ex(pool, size, sizeof(long))) != NULL)
        memset(p, 0, size);

    return p;
}

// set lock
void easy_pool_set_lock(easy_pool_t *pool)
{
    pool->flags = 1;
}

// set realloc
void easy_pool_set_allocator(easy_pool_realloc_pt alloc)
{
    easy_pool_realloc = (alloc ? alloc : easy_pool_default_realloc);
}

void *easy_pool_default_realloc (void *ptr, size_t size)
{
    if (size) {
        return realloc (ptr, size);
    } else if (ptr) {
        free (ptr);
    }

    return 0;
}
///////////////////////////////////////////////////////////////////////////////////////////////////
// default realloc

static void *easy_pool_alloc_block(easy_pool_t *pool, uint32_t size)
{
    uint8_t                 *m;
    uint32_t                psize;
    easy_pool_t             *p, *newpool, *current;

    psize = (uint32_t) (pool->end - (uint8_t *) pool);

    if ((m = (uint8_t *)easy_pool_realloc(NULL, psize)) == NULL)
        return NULL;

    newpool = (easy_pool_t *) m;
    newpool->end = m + psize;
    newpool->next = NULL;
    newpool->failed = 0;

    m += offsetof(easy_pool_t, current);
    m = easy_align_ptr(m, sizeof(unsigned long));
    newpool->last = m + size;
    current = pool->current;

    for (p = current; p->next; p = p->next) {
        if (p->failed++ > 4) {
            current = p->next;
        }
    }

    p->next = newpool;
    pool->current = current ? current : newpool;

    return m;
}

static void *easy_pool_alloc_large(easy_pool_t *pool, easy_pool_large_t *large, uint32_t size)
{
    if ((large->data = (uint8_t *)easy_pool_realloc(NULL, size)) == NULL)
        return NULL;

    large->next = pool->large;
    pool->large = large;
    return large->data;
}

/**
 * strdup
 */
char *easy_pool_strdup(easy_pool_t *pool, const char *str)
{
    int                     sz;
    char                    *ptr;

    if (str == NULL)
        return NULL;

    sz = strlen(str) + 1;

    if ((ptr = (char *)easy_pool_alloc(pool, sz)) == NULL)
        return NULL;

    memcpy(ptr, str, sz);
    return ptr;
}
