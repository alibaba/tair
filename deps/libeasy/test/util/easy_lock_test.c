#include "easy_ref.h"
#include "easy_list.h"
#include "easy_lock.h"
#include <easy_test.h>

/**
 * 测试 easy_ref
 */

typedef struct easy_lock_object_test1_t {
    easy_lock_t *lock;
    int         val1;
} easy_lock_object_test1_t;

typedef struct easy_lock_object_test2_t {
    easy_lock_t *lock;
    int         val2;
} easy_lock_object_test2_t;

easy_lock_object_test1_t *easy_lock_object_test1_create(easy_lock_t *lock)
{
    easy_lock_object_test1_t *obj;

    obj = malloc(sizeof(easy_lock_object_test1_t));
    obj->lock = lock;
    easy_lock_inc(lock);

    return obj;
}

void easy_lock_object_test1_destroy(easy_lock_object_test1_t *obj)
{
    easy_lock_dec(obj->lock);
    free(obj);
}

TEST(easy_lock, test)
{
    easy_lock_object_test1_t *obj, *obj1;
    easy_lock_t              *lock;

    lock = easy_lock_create();

    obj = easy_lock_object_test1_create(lock);
    obj1 = easy_lock_object_test1_create(lock);
    EXPECT_EQ(lock->refcnt.refcount, 3);
    EXPECT_EQ(obj->lock, obj1->lock);
    EXPECT_EQ(lock, obj->lock);

    easy_lock_destroy(lock);
    EXPECT_EQ(lock->refcnt.refcount, 2);

    easy_lock_object_test1_destroy(obj);
    EXPECT_EQ(lock->refcnt.refcount, 1);

    easy_lock_object_test1_destroy(obj1);
}
