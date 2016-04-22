/*
 * kref.c - library routines for handling generic reference counted objects
 *
 * Copyright (C) 2004 Greg Kroah-Hartman <greg@kroah.com>
 * Copyright (C) 2004 IBM Corp.
 *
 * based on kobject.h which was:
 * Copyright (C) 2002-2003 Patrick Mochel <mochel@osdl.org>
 * Copyright (C) 2002-2003 Open Source Development Labs
 *
 * This file is released under the GPLv2.
 *
 */

#ifndef _KREF_H_
#define _KREF_H_

#include <easy_atomic.h>

#define barrier() __asm__ ("" ::: "memory")

struct kref {
	easy_atomic_t refcount;
};

void kref_set(struct kref *kref, int num);
void kref_init(struct kref *kref);
void kref_get(struct kref *kref);
int kref_put(struct kref *kref, void (*release) (struct kref *kref));
int kref_sub(struct kref *kref, int count,
	     void (*release) (struct kref *kref));

typedef struct kref easy_ref_t;
#define easy_ref_set(ref, num) kref_set((ref), (num))
#define easy_ref_init(ref) kref_init((ref))
#define easy_ref_get(ref) kref_get((ref))
#define easy_ref_put(ref, release) kref_put((ref), (release))
#define easy_ref_sub(ref, release) kref_sub((ref), count, (release))

#endif /* _KREF_H_ */
