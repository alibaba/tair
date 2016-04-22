#include <stdarg.h>
#include "easy_buf.h"
#include "easy_string.h"

/**
 * 创建一个新的easy_buf_t
 */
easy_buf_t *easy_buf_create(easy_pool_t *pool, uint32_t size)
{
    easy_buf_t              *b;

    if ((b = (easy_buf_t *)easy_pool_calloc(pool, sizeof(easy_buf_t))) == NULL)
        return NULL;

    // 一个page大小
    if (size == 0)
        size = pool->end - pool->last;

    if ((b->pos = (char *)easy_pool_alloc(pool, size)) == NULL)
        return NULL;

    b->last = b->pos;
    b->end = b->last + size;
    b->cleanup = NULL;
    b->args = pool;
    easy_list_init(&b->node);

    return b;
}

/**
 * 把data包成easy_buf_t
 */
easy_buf_t *easy_buf_pack(easy_pool_t *pool, const void *data, uint32_t size)
{
    easy_buf_t              *b;

    if ((b = (easy_buf_t *)easy_pool_calloc(pool, sizeof(easy_buf_t))) == NULL)
        return NULL;

    easy_buf_set_data(pool, b, data, size);

    return b;
}

/**
 * 设置数据到b里
 */
void easy_buf_set_data(easy_pool_t *pool, easy_buf_t *b, const void *data, uint32_t size)
{
    b->pos = (char *)data;
    b->last = b->pos + size;
    b->end = b->last;
    b->cleanup = NULL;
    b->args = pool;
    b->flags = 0;
    easy_list_init(&b->node);
}

/**
 * 创建一个easy_file_buf_t, 用于sendfile等
 */
easy_file_buf_t *easy_file_buf_create(easy_pool_t *pool)
{
    easy_file_buf_t         *b;

    b = (easy_file_buf_t *)easy_pool_calloc(pool, sizeof(easy_file_buf_t));
    b->flags = EASY_BUF_FILE;
    b->cleanup = NULL;
    b->args = pool;
    easy_list_init(&b->node);

    return b;
}

void easy_file_buf_set_close(easy_file_buf_t *b)
{
    if ((b->flags & EASY_BUF_FILE))
        b->flags = EASY_BUF_CLOSE_FILE;
}

void easy_buf_set_cleanup(easy_buf_t *b, easy_buf_cleanup_pt *cleanup, void *args)
{
    b->cleanup = cleanup;
    b->args = args;
}

void easy_buf_destroy(easy_buf_t *b)
{
    easy_buf_cleanup_pt         *cleanup;
    easy_list_del(&b->node);

    if ((b->flags & EASY_BUF_CLOSE_FILE) == EASY_BUF_CLOSE_FILE)
        close(((easy_file_buf_t *)b)->fd);

    // cleanup
    if ((cleanup = b->cleanup)) {
        b->cleanup = NULL;
        (*cleanup)(b, b->args);
    }
}

/**
 * 空间不够,分配出一块来,保留之前的空间
 */
int easy_buf_check_read_space(easy_pool_t *pool, easy_buf_t *b, uint32_t size)
{
    int                     dsize;
    char                    *ptr;

    if ((b->end - b->last) >= (int)size)
        return EASY_OK;

    // 需要大小
    dsize = (b->last - b->pos);
    size = easy_max(dsize * 3 / 2, size + dsize);
    size = easy_align(size, EASY_POOL_PAGE_SIZE);

    // alloc
    if ((ptr = (char *)easy_pool_alloc(pool, size)) == NULL)
        return EASY_ERROR;

    // copy old buf to new buf
    if (dsize > 0)
        memcpy(ptr, b->pos, dsize);

    b->pos = ptr;
    b->last = b->pos + dsize;
    b->end = b->pos + size;

    return EASY_OK;
}

/**
 * 空间不够,分配出一块来,保留之前的空间
 */
easy_buf_t *easy_buf_check_write_space(easy_pool_t *pool, easy_list_t *bc, uint32_t size)
{
    easy_buf_t              *b = easy_list_get_last(bc, easy_buf_t, node);

    if (b != NULL && (b->end - b->last) >= (int)size)
        return b;

    // 重新生成一个buf,放入buf_chain_t中
    size = easy_align(size, EASY_POOL_PAGE_SIZE);

    if ((b = easy_buf_create(pool, size)) == NULL)
        return NULL;

    easy_list_add_tail(&b->node, bc);

    return b;
}

/**
 * 清除掉
 */
void easy_buf_chain_clear(easy_list_t *l)
{
    easy_buf_t              *b, *b1;

    easy_list_for_each_entry_safe(b, b1, l, node) {
        easy_buf_destroy(b);
    }
    easy_list_init(l);
}

/**
 * 加到后面
 */
void easy_buf_chain_offer(easy_list_t *l, easy_buf_t *b)
{
    if (!l->next) easy_list_init(l);

    easy_list_add_tail(&b->node, l);
}

/**
 * 把s复制到d上
 */
int easy_buf_string_copy(easy_pool_t *pool, easy_buf_string_t *d, const easy_buf_string_t *s)
{
    if (s->len > 0) {
        d->data = (char *)easy_pool_alloc(pool, s->len + 1);
        memcpy(d->data, s->data, s->len);
        d->data[s->len] = '\0';
        d->len = s->len;
    }

    return s->len;
}

int easy_buf_string_printf(easy_pool_t *pool, easy_buf_string_t *d, const char *fmt, ...)
{
    int                     len;
    char                    buffer[2048];

    va_list                 args;
    va_start(args, fmt);
    len = easy_vsnprintf(buffer, 2048, fmt, args);
    va_end(args);
    d->data = (char *)easy_pool_alloc(pool, len + 1);
    memcpy(d->data, buffer, len);
    d->data[len] = '\0';
    d->len = len;
    return len;
}

int easy_buf_list_len(easy_list_t *l)
{
    easy_buf_t              *b;
    int                     len = 0;

    easy_list_for_each_entry(b, l, node) {
        len += easy_buf_len(b);
    }

    return len;
}

