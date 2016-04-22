#ifndef _EASY_LOCK_H_
#define _EASY_LOCK_H_

#include "easy_ref.h"
#include "easy_atomic.h"

typedef struct easy_lock_t easy_lock_t;

struct easy_lock_t {
    easy_ref_t    refcnt;
    easy_atomic_t lock;
};

easy_lock_t *easy_lock_create();
void easy_lock_release(easy_ref_t *ref);
void easy_lock_destroy(easy_lock_t *lock);
easy_lock_t * easy_lock_inc(easy_lock_t *lock);
void easy_lock_dec(easy_lock_t *lock);

#define easy_lock_reference(lock) easy_lock_inc(lock)
#define easy_lock_dereference(lock) do { easy_lock_dec(lock); lock = NULL; } while(0)

#endif
