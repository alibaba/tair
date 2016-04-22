/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#include <sys/time.h>
#include "Cond.h"

namespace tbutil
{
Cond::Cond()
{
    pthread_condattr_t attr;
    int rt = pthread_condattr_init(&attr);
#ifdef _NO_EXCEPTION
    assert( 0 == rt );
#else
    if( 0 != rt )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif
    
    rt =  pthread_cond_init(&_cond, &attr);
#ifdef _NO_EXCEPTION
    assert( 0 == rt );
#else
    if( 0 != rt )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif

    rt = pthread_condattr_destroy(&attr);
#ifdef _NO_EXCEPTION
    assert( 0 == rt );
#else
    if( 0 != rt )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif
}

Cond::~Cond()
{
    pthread_cond_destroy(&_cond);
}

void Cond::signal()
{
    const int rt = pthread_cond_signal(&_cond);
#ifdef _NO_EXCEPTION
    assert( 0 == rt );
#else
    if ( 0 != rt )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif
}

void Cond::broadcast()
{
    const int rt = pthread_cond_broadcast(&_cond);
#ifdef _NO_EXCEPTION
    assert( 0 == rt );
#else
    if( 0 != rt )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif
}
}//end namespace tbutil
