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

#include "Timer.h"
#include "Exception.h"

using namespace std;
namespace tbutil
{
Timer::Timer() :
    Thread(),
    _destroyed(false)
{
    start();
}

void Timer::destroy()
{
    {
        Monitor<Mutex>::Lock sync(_monitor);
        if(_destroyed)
        {
            return;
        }
        _destroyed = true;
        _monitor.notifyAll();
        _tasks.clear();
        _tokens.clear();
    }

    if(!_detachable)
    {
        join();
    }
}

int Timer::schedule(const TimerTaskPtr& task, const Time& delay)
{
    Monitor<Mutex>::Lock sync(_monitor);
    if(_destroyed)
    {
#ifdef _NO_EXCEPTION
        TBSYS_LOG(ERROR,"%s","timer destroyed...");       
        return -1; 
#else
        throw IllegalArgumentException(__FILE__, __LINE__, "timer destroyed");
#endif
    }

    Time time = Time::now(Time::Monotonic) + delay;
    bool inserted = _tasks.insert(make_pair(task, time)).second;
    if(!inserted)
    {
#ifdef _NO_EXCEPTION
        TBSYS_LOG(ERROR,"%s","task is already schedulded...");       
        return -1; 
#else
        throw IllegalArgumentException(__FILE__, __LINE__, "task is already schedulded");
#endif
    }
    _tokens.insert(Token(time, Time(), task));

    if(_wakeUpTime == Time() || time < _wakeUpTime)
    {
        _monitor.notify();
    }
    return 0;
}

int Timer::scheduleRepeated(const TimerTaskPtr& task, const Time& delay)
{
    Monitor<Mutex>::Lock sync(_monitor);
    if(_destroyed)
    {
#ifdef _NO_EXCEPTION
        TBSYS_LOG(ERROR,"%s","timer destroyed...");
        return -1;
#else
        throw IllegalArgumentException(__FILE__, __LINE__, "timer destroyed");
#endif
    }

    const Token token(Time::now(Time::Monotonic) + delay, delay, task);
    bool inserted = _tasks.insert(make_pair(task, token.scheduledTime)).second;
    if(!inserted)
    {
#ifdef _NO_EXCEPTION
        TBSYS_LOG(ERROR,"%s","task is already schedulded...");
        return -1;
#else
        throw IllegalArgumentException(__FILE__, __LINE__, "task is already schedulded");
#endif
    }
    _tokens.insert(token); 
   
    if(_wakeUpTime == Time() || token.scheduledTime < _wakeUpTime)
    {
        _monitor.notify();
    }
    return 0;
}

bool Timer::cancel(const TimerTaskPtr& task)
{
    Monitor<Mutex>::Lock sync(_monitor);
    if(_destroyed)
    {
        return false;
    }

    map<TimerTaskPtr, Time, TimerTaskCompare>::iterator p = _tasks.find(task);
    if(p == _tasks.end())
    {
        return false;
    }

    _tokens.erase(Token(p->second, Time(), p->first));
    _tasks.erase(p);

    return true;
}

void
Timer::run()
{
    Token token(Time(), Time(), 0);
    while(true)
    {
        {
            Monitor<Mutex>::Lock sync(_monitor);

            if(!_destroyed)
            {
                if(token.delay != Time())
                {
                    map<TimerTaskPtr, Time, TimerTaskCompare>::iterator p = _tasks.find(token.task);
                    if(p != _tasks.end())
                    {
                        token.scheduledTime = Time::now(Time::Monotonic) + token.delay;
                        p->second = token.scheduledTime;
                        _tokens.insert(token);
                    }
                }
                token = Token(Time(), Time(), 0);

                if(_tokens.empty())
                {
                    _wakeUpTime = Time();
                    _monitor.wait();
                }
            }
            
            if(_destroyed)
            {
                break;
            }
           
            while(!_tokens.empty() && !_destroyed)
            {
                const Time now = Time::now(Time::Monotonic);
                const Token& first = *(_tokens.begin());
                if(first.scheduledTime <= now)
                {
                    token = first;
                    _tokens.erase(_tokens.begin());
                    if(token.delay == Time())
                    {
                        _tasks.erase(token.task);
                    }
                    break;
                }
                
                _wakeUpTime = first.scheduledTime;
                _monitor.timedWait(first.scheduledTime - now);
            }

            if(_destroyed)
            {
                break;
            }
        }     

        if(token.task)
        {
            try
            {
                token.task->runTimerTask();
            }
            catch(const std::exception& e)
            {
                cerr << "Timer::run(): uncaught exception:\n" << e.what() << endl;
            } 
            catch(...)
            {
                cerr << "Timer::run(): uncaught exception" << endl;
            }
        }
    }
}
}// end namespace tbutil
