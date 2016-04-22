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

#include <ThreadPool.h>
#include <EventHandler.h>
#include <Timer.h>

#include "testCommon.h"
#include <vector>

using namespace tbutil;
using namespace std;

static int64_t gCount;
Monitor<Mutex> _monitor;

static ThreadPool * pool = NULL;
tbutil::Time gNow;
tbutil::Time gStart;

class TestWorkItems : public ThreadPoolWorkItem 
{
public:
    TestWorkItems()
    {

    }
    virtual void execute( const ThreadPool* )
    {
        Monitor<Mutex>::Lock lock(_monitor);
        ++gCount;
        tbutil::Time now2 = tbutil::Time::now();
        if ( (now2 - gNow ) >= tbutil::Time::seconds(10) )
        {
            gNow = now2;
            cout<<"gCount: "<<gCount<<" , "<<"Time: " <<(now2 -gStart).toSeconds()<<endl;
        }
    }

    virtual void destroy( )
    {

    }
    
};

class ScheduleTask: public TimerTask
{
public:

    ScheduleTask()
    {
    }

    virtual void 
    runTimerTask()
    {
        for ( register int i = 0; i < 0x100000; ++i )
        //for ( register int i = 0; i < 0x01; ++i )
        {
            assert( pool != NULL );
            pool->execute( new TestWorkItems() );          
        }
    }
};
typedef Handle<ScheduleTask> ScheduleTaskPtr;

int main(int argc, char* argv[])
{
    gStart =tbutil::Time::now();
    pool = new ThreadPool(2,4,3);
    TimerPtr timer = new tbutil::Timer();

    cout << "testing threadpool... " << flush;
    {
        {
            ScheduleTaskPtr task = new ScheduleTask();
            timer->scheduleRepeated(task, tbutil::Time::seconds(10));
            ScheduleTaskPtr task1 = new ScheduleTask();
            /*timer->scheduleRepeated(task1, tbutil::Time::seconds(1));
            ScheduleTaskPtr task2 = new ScheduleTask();
            timer->scheduleRepeated(task2, tbutil::Time::seconds(100));
            ScheduleTaskPtr task3 = new ScheduleTask();
            timer->scheduleRepeated(task3, tbutil::Time::seconds(130));
            ScheduleTaskPtr task4 = new ScheduleTask();
            timer->scheduleRepeated(task4, tbutil::Time::seconds(180));
            ScheduleTaskPtr task5 = new ScheduleTask();
            timer->scheduleRepeated(task5, tbutil::Time::seconds(220));
            ScheduleTaskPtr task6 = new ScheduleTask();
            timer->scheduleRepeated(task6, tbutil::Time::seconds(270));*/
        }
    }

    sleep(  0xfffff );
    timer->destroy();
    pool->destroy();
    pool->joinWithAllThreads();
    delete pool;
    cout << "ok" << endl;
    return EXIT_SUCCESS;
}

