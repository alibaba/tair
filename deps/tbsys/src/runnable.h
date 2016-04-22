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

#ifndef TBSYS_RUNNABLE_H_
#define TBSYS_RUNNABLE_H_

namespace tbsys {

/** 
 * @brief Runnable是一个抽象类，它拥有一个run纯虚方法
 * 主要用于Thread类  
 */
class Runnable {

public:
    /*
     * 析构
     */
    virtual ~Runnable() {
    }
    /**
     * 运行入口函数
     */
    virtual void run(CThread *thread, void *arg) = 0;
};

}

#endif /*RUNNABLE_H_*/
