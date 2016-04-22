#include "easy_lock.h"
#include "easy_list.h"

easy_lock_t *easy_lock_create()
{
    easy_lock_t *lock;

    lock  = calloc(1, sizeof(easy_lock_t));
    easy_ref_init(&lock->refcnt);

    return lock;
}

void easy_lock_release(easy_ref_t *ref)
{
    easy_lock_t *lock;

    lock = easy_list_entry(ref, easy_lock_t, refcnt);

    free(lock);
}

easy_lock_t *easy_lock_inc(easy_lock_t *lock)
{
    if (lock == NULL)
        return NULL;

    assert(lock->refcnt.refcount);
    easy_ref_get(&lock->refcnt);
    return lock;
}

void easy_lock_dec(easy_lock_t *lock)
{
    if (lock)
        easy_ref_put(&lock->refcnt, easy_lock_release);
}

void easy_lock_destroy(easy_lock_t *lock)
{
    if (lock)
         easy_ref_put(&lock->refcnt, easy_lock_release);
}
