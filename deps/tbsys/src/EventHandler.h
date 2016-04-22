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

#ifndef TBSYS_EVENTHANDLER_H
#define TBSYS_EVENTHANDLER_H

namespace tbutil
{
class ThreadPool;
/** 
 * @brief ThreadPoolWorkItem 线程任务队列Item基类,它拥有execute纯虚方法
 * 要实例化ThreadPoolWorkItem类，必须继承并实现execute方法
 */
class ThreadPoolWorkItem 
{
public:
      virtual ~ThreadPoolWorkItem(){}
      virtual void destroy( )=0;
      virtual void execute( const ThreadPool* ) = 0;
};
}
#endif
