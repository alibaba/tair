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

#ifndef TBSYSEX_THREAD_H
#define TBSYSEX_THREAD_H 
#include "Shared.h"
#include "Handle.h"
#include "Mutex.h"
#include "Cond.h"
#include "Time.h"

namespace tbutil
{
/** 
* @brief Thread类是一个抽象类，它有一个run纯虚方法，如果要实例化Thread类
* 用户必须继承并实现run方法
*/
class Thread : virtual public tbutil::Shared
{
public:

    Thread();
    virtual ~Thread();

    virtual void run() = 0;

    /** 
     * @brief 这个成员函数启动新创建的线程
     * 
     * @param stackSize: 栈的大小
     * 
     * @return 
     */
    int  start(size_t stackSize= 0);

    /** 
     * @brief 如果底层的线程还没有退出(也就是说，还没有离开它的run 方
     * 法)，这个方法返回真；否则返回假。如果你想要实现非阻塞式的join， isAlive 很有用
     * 
     * @return 
     */
    bool isAlive() const; 

    /** 
     * @brief 设置线程存活标志为false 
     */
    void _done(); 

    /** 
     * @brief 这个方法挂起发出调用的线程，直到join 所针对的线程终止为止
     * 
     * @return 
     */
    int join();  

    /** 
     * @brief 这个方法分离一个线程。一旦线程分离，你不能再与它汇合
     * 如果你针对已经分离的线程、或是已经汇合的线程调用detach，
     * 会产生不确定的行为
     * @return 
     */
    int detach();

    /** 
     * @brief 这个函数返回每个线程的唯一标识符
     * 
     * @return 
     */
    pthread_t id() const;

    /** 
     * @brief 这个方法使得它所针对的线程放弃CPU，让其他线程运行 
     */
    static void yield();
    /** 
     * @brief 这个方法挂起线程，时间长度由Time 参数指定
     * 
     * @param timeout : 挂起的超时时间
     */
    static void ssleep(const tbutil::Time& timeout);

protected:
    bool  _running;   //线程运行标志
    bool _started;    //线程是否处于开始状态
    bool _detachable; //是否全使线程处于分离状态
    pthread_t _thread;//线程ID
    tbutil::Mutex _mutex;     //线程运行标志锁
private:
    Thread(const Thread&);            
    Thread& operator=(const Thread&);   
};
typedef tbutil::Handle<Thread> ThreadPtr;
}//end namespace tbutil

#endif

