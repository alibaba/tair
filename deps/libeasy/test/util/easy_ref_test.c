#include "easy_ref.h"
#include "easy_list.h"
#include <easy_test.h>

/**
 * 测试 easy_ref
 */

typedef struct easy_ref_object_t {
    easy_ref_t refcnt;
    int        *destroy;
} easy_ref_object_t;

void easy_ref_object_release(easy_ref_t *ref)
{
    easy_ref_object_t *obj = easy_list_entry(ref, easy_ref_object_t, refcnt);
    obj->destroy = NULL;
}

easy_ref_object_t *easy_ref_object_create()
{
    easy_ref_object_t *obj = malloc(sizeof(easy_ref_object_t));
    obj->destroy = malloc(sizeof(int));
    easy_ref_init(&obj->refcnt);
    return obj;
}

void easy_ref_object_lock(easy_ref_object_t *obj)
{
    easy_ref_get(&obj->refcnt);
}

void easy_ref_object_unlock(easy_ref_object_t *obj)
{
    easy_ref_put(&obj->refcnt, easy_ref_object_release);
}

void easy_ref_object_destroy(easy_ref_object_t *obj)
{
    easy_ref_put(&obj->refcnt, easy_ref_object_release);
}

TEST(easy_ref, test)
{
    easy_ref_t ref;
    easy_ref_init(&ref);
    EXPECT_EQ(ref.refcount, 1);

    easy_ref_object_t *obj;
    obj = easy_ref_object_create();
    EXPECT_EQ(obj->refcnt.refcount, 1);

    easy_ref_object_lock(obj);
    EXPECT_EQ(obj->refcnt.refcount, 2);
    EXPECT_TRUE(obj->destroy);

    easy_ref_object_destroy(obj);
    EXPECT_EQ(obj->refcnt.refcount, 1);
    EXPECT_TRUE(obj->destroy);

    easy_ref_object_unlock(obj);
    EXPECT_EQ(obj->refcnt.refcount, 0);
    EXPECT_FALSE(obj->destroy);

    free(obj);
}
