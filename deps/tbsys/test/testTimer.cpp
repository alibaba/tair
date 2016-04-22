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

#include <Timer.h>
#include "testCommon.h"
#include <vector>

using namespace tbutil;
using namespace tbutil;
using namespace std;

class TestTask : public TimerTask, Monitor<Mutex>
{
public:

    TestTask() : _count(0)
    {
    }

    TestTask(const tbutil::Time& scheduledTime) : 
         _scheduledTime(scheduledTime),
        _count(0)
    {
    }
    
    virtual void 
    runTimerTask()
    {
        Lock sync(*this);
        ++_count;
        _run = tbutil::Time::now(tbutil::Time::Monotonic);
        //cerr << "run: " << _scheduledTime.toMilliSeconds() << " " << _run.toMilliSeconds() << endl;
        notifyAll();
    }

    virtual bool
    operator<(const TestTask& r) const
    {
        return _scheduledTime < r._scheduledTime;
    }

    virtual bool
    hasRun() const
    {
        Lock sync(*this);
        return _run != tbutil::Time();
    }

    int 
    getCount() const
    {
        Lock sync(*this);
        return _count;
    }
    
    virtual tbutil::Time
    getRunTime() const
    {
        Lock sync(*this);
        return _run;
    }

    tbutil::Time
    getScheduledTime() const
    {
        return _scheduledTime;
    }

    virtual void
    waitForRun()
    {
        Lock sync(*this);
        while(_run == tbutil::Time())
        {
            if(!timedWait(tbutil::Time::seconds(10)))
            {
                test(false); // Timeout.
            }
        }
    }

    void 
    clear()
    {
        _run = tbutil::Time();
        _count = 0;
    }

private:

    tbutil::Time _run;
    tbutil::Time _scheduledTime;
    int _count;
};
typedef tbutil::Handle<TestTask> TestTaskPtr;


class DestroyTask : public TimerTask, Monitor<Mutex>
{
public:

    DestroyTask(const tbutil::TimerPtr& timer) :
         _timer(timer),
         _run(false)
    {
    }

    virtual void 
    runTimerTask()
    {
        Lock sync(*this);
        _timer->destroy();
        _run = true;
        notify();
    }

    virtual void
    waitForRun()
    {
        Lock sync(*this);
        while(!_run)
        {
            if(!timedWait(tbutil::Time::seconds(10)))
            {
                test(false); // Timeout.
            }
        }
    }

private:

    TimerPtr _timer;
    bool _run;
};
typedef Handle<DestroyTask> DestroyTaskPtr;

int main(int argc, char* argv[])
{
    tbutil::Time t = tbutil::Time::now();
    cout<<" time string : "<<t.toDateTime()<<endl<<" time : "<<t.toDuration()<<endl;
    cout << "testing timer... " << flush;
    {
        TimerPtr timer = new tbutil::Timer();
        {
            TestTaskPtr task = new TestTask();
            timer->schedule(task, tbutil::Time());
            task->waitForRun();
            task->clear();
	    while(true)
	    {
		try
		{
                    timer->schedule(task, tbutil::Time::milliSeconds(-10));
		    timer->schedule(task, tbutil::Time());
		}
		catch(const tbutil::IllegalArgumentException&)
		{
		    break;
		}
                task->waitForRun();
                task->clear();
	    }
        }

        {
            TestTaskPtr task = new TestTask();
            test(!timer->cancel(task));
            timer->schedule(task, tbutil::Time::seconds(1));
            test(!task->hasRun() && timer->cancel(task) && !task->hasRun());
            test(!timer->cancel(task));
            Thread::ssleep(tbutil::Time::milliSeconds(1100));
            test(!task->hasRun());
        }

        {
            vector<TestTaskPtr> tasks;
            tbutil::Time start = Time::now(Time::Monotonic) + Time::milliSeconds(500);
            for(int i = 0; i < 20; ++i)
            {
                tasks.push_back(new TestTask(Time::milliSeconds(500 + i * 5)));
            }

            random_shuffle(tasks.begin(), tasks.end());
	    vector<TestTaskPtr>::const_iterator p;
            for(p = tasks.begin(); p != tasks.end(); ++p)
            {
                timer->schedule(*p, (*p)->getScheduledTime());
            }

            for(p = tasks.begin(); p != tasks.end(); ++p)
            {
                (*p)->waitForRun();
            }            

            test(Time::now(Time::Monotonic) > start);

            sort(tasks.begin(), tasks.end());
            for(p = tasks.begin(); p + 1 != tasks.end(); ++p)
            {
                if((*p)->getRunTime() > (*(p + 1))->getRunTime())
                {
                    test(false);
                }
            }
        }

        {
            TestTaskPtr task = new TestTask();
            timer->scheduleRepeated(task, tbutil::Time::milliSeconds(20));
            Thread::ssleep(Time::milliSeconds(500));
            test(task->hasRun());
	    test(task->getCount() > 1);
	    test(task->getCount() < 26);
            test(timer->cancel(task));
            int count = task->getCount();
            Thread::ssleep(Time::milliSeconds(100));
            test(count == task->getCount() || count + 1 == task->getCount());
        }

        timer->destroy();
        timer = 0;
    }
    cout << "ok" << endl;

    cout << "testing timer destroy... " << endl;
    {
        {
            TimerPtr timer = new Timer();
            TestTaskPtr testTask = new TestTask();
            //timer->schedule(testTask, Time());
            //timer->destroy();
            try
            {
                timer->schedule(testTask, Time());
            }
            catch(const IllegalArgumentException& e )
            {
                cout<< e <<endl;
            }
        }
    }
    cout << "ok" << endl;

    return EXIT_SUCCESS;
}

