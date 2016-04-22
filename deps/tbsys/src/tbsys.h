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

#ifndef TBSYS_H
#define TBSYS_H

#include <assert.h>
#include <errno.h>

//add by duanbing 2009-06-24-11:06
#include <cassert>
#include <iostream>
#include <sstream>
#include <pthread.h>
#include <vector>
#include <string>

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif


namespace tbsys {
class CConfig;
class CFileUtil;
class CStringUtil;
class CNetUtil;
class CTimeUtil;
class CProcess;
class CLogger;
class CThread;
class CThreadMutex;
class CThreadCond; 
class Runnable;
class CDefaultRunnable;
class CFileQueue;
class IQueueHandler;
class CQueueThread;
class CFileQueueThread;
class WarningBuffer;
};//end namespace tbsys

//add by duanbing 2009-06-24-11:06
namespace tbutil
{
class noncopyable
{
protected:
 
    noncopyable() { }
    ~noncopyable() { }
private:
 
    noncopyable(const noncopyable&);
    const noncopyable& operator=(const noncopyable&);
};

#if defined(__BCPLUSPLUS__) || defined(_MSC_VER)
    typedef __int64 Int64;
    #define T_INT64(n) n##i64
#elif defined(TNET_64)
    typedef long Int64;
    #define T_INT64(n) n##L
#else
    typedef long long Int64;
    #define T_INT64(n) n##LL
#endif
 
typedef unsigned char Byte;
typedef short Short;
typedef int Int;
typedef Int64 Long;
typedef float Float;
typedef double Double;
 
typedef ::std::vector<bool> BoolSeq;
 
typedef ::std::vector< Byte> ByteSeq;
 
typedef ::std::vector< Short> ShortSeq;
 
typedef ::std::vector< Int> IntSeq;
 
typedef ::std::vector< Long> LongSeq;
 
typedef ::std::vector< Float> FloatSeq;
 
typedef ::std::vector< Double> DoubleSeq;
 
typedef ::std::vector< ::std::string> StringSeq;
 
inline int getSystemErrno()
{
    return errno;
}
}//end namespace tbutil

#include "atomic.h"
#include "config.h"
#include "fileutil.h"
#include "stringutil.h"
#include "tbnetutil.h"
#include "tbtimeutil.h"
#include "process.h"
#include "tblog.h"
#include "WarningBuffer.h"
#include "tbrwlock.h"

#include "runnable.h"
#include "iqueuehandler.h"
#include "defaultrunnable.h"
#include "thread.h"
#include "threadmutex.h"
#include "threadcond.h"

#include "queuethread.h"
#include "filequeue.h"
#include "filequeuethread.h"
#include "profiler.h"
#include "bytebuffer.h"

#endif

