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

#ifndef TBSYS_TIMER_H
#define TBSYS_TIMER_H
#include <set>
#include <map>
#include "Shared.h"
#include "TbThread.h"
#include "Monitor.h"
#include "Time.h"
namespace tbutil 
{
class Timer;
typedef Handle<Timer> TimerPtr;

/** 
 * @brief TimerTask是定时器任务队列Item的基类,它是一抽象类
 * 它有一个runTimerTask纯虚方法,用户如果需要生成新的定时任务
 * 需要继承TimerTask并实现runTimerTask方法
 */
class TimerTask : virtual public Shared
{
public:

    virtual ~TimerTask() { }

    /** 
     * @brief 纯虚函数,具体逻辑函数
     */
    virtual void runTimerTask() = 0;
};
typedef Handle<TimerTask> TimerTaskPtr;

/** 
 * @brief Timer类是一个定时器类，内站一条线程和一个定时任务队列组成
 * 在将任务放入任务队列时，会对任务队列的任务按照被调度的时间先后顺序
 * 排序，在第一个被调度的定时任务时间到的时候,该任务就会被调度,有些定
 * 时任务在间隔一段时间后都会被调度一次可以调用scheduleRepeated函数,
 * 其它的都调用schedule
 */
class Timer :public virtual Shared ,private virtual tbutil::Thread
{
public:

    Timer();

    /** 
     * @brief 停止定时器
     */
    void destroy();

    /** 
     * @brief 新增加一个定时任务,此任务只会被调度一次，执行完成以后就会删除此任务
     * 
     * @param task : 新增任务
     * @param delay: 执行任务的间隔时间 
     * 说明: 真正执行任务的时间=新增任务当前时间 + 间隔时间
     * @return 
     */
    int schedule(const TimerTaskPtr& task, const Time& delay);

    /** 
     * @brief 新增加一个定时任务,此任务在间隔时间到时,都会调用一次
     * 
     * @param task : 新增任务
     * @param delay: 执行任务的间隔时间 
     * 说明: 真正执行任务的时间=新增任务当前时间 + 间隔时间
     * @return 
     */
    int scheduleRepeated(const TimerTaskPtr& task, const Time& delay);

    /** 
     * @brief 取消一个定时任务
     * 
     * @param task: 被取消的定时任务
     * 
     * @return 
     */
    bool cancel(const TimerTaskPtr&);

private:

    struct Token
    {
        Time scheduledTime;
        Time delay;
        TimerTaskPtr task;

        inline Token(const Time&, const Time&, const TimerTaskPtr&);
        inline bool operator<(const Token& r) const;
    };

    virtual void run();

    Monitor<Mutex> _monitor;
    bool _destroyed;
    std::set<Token> _tokens;
    
    class TimerTaskCompare : public std::binary_function<TimerTaskPtr, TimerTaskPtr, bool>
    {
    public:
        
        bool operator()(const TimerTaskPtr& lhs, const TimerTaskPtr& rhs) const
        {
            return lhs.get() < rhs.get();
        }
    };
    std::map<TimerTaskPtr, Time, TimerTaskCompare> _tasks;
    Time _wakeUpTime;
};
typedef Handle<Timer> TimerPtr;

inline 
Timer::Token::Token(const Time& st, const Time& d, const TimerTaskPtr& t) :
    scheduledTime(st), delay(d), task(t)
{
}

inline bool
Timer::Token::operator<(const Timer::Token& r) const
{
    if(scheduledTime < r.scheduledTime)
    {
        return true;
    }
    else if(scheduledTime > r.scheduledTime)
    {
        return false;
    }
    
    return task.get() < r.task.get();
}

}

#endif

