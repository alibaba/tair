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

#include <Exception.h>
#include <Time.h>
#include <stdio.h>
#include <string.h>
#include <ThreadPool.h>
#include <EventHandler.h>
#include <Handle.h>
#include <Time.h>
#include "testBase.h"
#include "testCommon.h"
#include <list>
#include "testCommon.h"

using namespace std;
using namespace tbutil;
static const string testName1("thread alive");

class CondVar : public Monitor<RecMutex>
{
public:
    CondVar():
     _done( false)
    {

    }

    void waitForSignal()
    {
         Monitor<RecMutex>::Lock sync(*this);
         while( !_done )
         {
             wait();
         }
    }
    void signal()
    {
         Monitor<RecMutex>::Lock sync(*this);
         _done = true;
         notify();
    }
private:
    bool _done;
};

class AliveTestThread : public Thread
{
public:
    AliveTestThread(CondVar& childCreated, CondVar& parentReady ):
    _childCreated(childCreated),
    _parentReady(parentReady)
    {

    }
    virtual void run()
    {
        try
        {
           _childCreated.signal();
           _parentReady.waitForSignal();
        }
        catch( Exception& e )
        {

        }
    }
private:
    CondVar& _childCreated;
    CondVar& _parentReady;
};

typedef Handle<AliveTestThread> AliveTestThreadPtr;

class AliveTest : public testBase
{
public:
     AliveTest():
     testBase(testName1)
     {

     }

     virtual void run()
     {
         CondVar childCreated;
         CondVar parentReady;
         AliveTestThreadPtr t = new AliveTestThread(childCreated, parentReady);
         t->start();
         childCreated.waitForSignal();
         test(t->isAlive());
         parentReady.signal();
         t->join();
         test(!t->isAlive());
     }
};

class MonitorMutexTestThread: public Thread
{
public:
   MonitorMutexTestThread( Monitor<Mutex>& m ):
       _monitor( m ),
       _tryLock( false )
   {

   }

   virtual void run()
   {
       Monitor<Mutex>::TryLock trylock(_monitor);
       test(!trylock.acquired() );
       {
           Mutex::Lock lock(_tryLockMutex);
           _tryLock = true;
       }
       _tryLockCond.signal();
       Monitor<Mutex>::Lock lock(_monitor);
   }

   void waitTryLock()
   {
       Mutex::Lock lock(_tryLockMutex);
       while( !_tryLock)
       {
           _tryLockCond.wait(lock);
       }
   }
   
   Monitor<Mutex>& _monitor;
   bool _tryLock;

   Cond _tryLockCond;
   Mutex _tryLockMutex;
};

typedef Handle<MonitorMutexTestThread> MonitorMutexTestThreadPtr;

class MonitorMutexTestThread2 : public Thread
{
public:
    MonitorMutexTestThread2(Monitor<Mutex>&m):
        _finished( false),
        _monitor(m)
    {

    }
    virtual void run()
    {
        Monitor<Mutex>::Lock lock(_monitor );
        _monitor.wait();
        _finished = true;
    }
    
    bool _finished;
    Monitor<Mutex>& _monitor;
};

typedef Handle<MonitorMutexTestThread2> MonitorMutexTestThread2Ptr;


static const string monitorName("Monitor<Mutex>");

class MonitorMutxTest : public testBase
{
public:
    MonitorMutxTest() :
        testBase(monitorName)
    {

    }

    void run();
};

void MonitorMutxTest::run()
{
    Monitor<Mutex> monitor;
    MonitorMutexTestThreadPtr t1;
    MonitorMutexTestThread2Ptr t2;
    MonitorMutexTestThread2Ptr t3;

    {
        Monitor<Mutex>::Lock lock(monitor);
        try
        {
            Monitor<Mutex>::TryLock tlock(monitor);
            test(!tlock.acquired());
        }
        catch( const ThreadLockedException& e )
        {
            cout<<"thread locked Execption: "<<e<<endl;
        }
        t1 = new MonitorMutexTestThread(monitor);
        t1->start();
        t1->waitTryLock();
    }
    t1->join();

    // test notify()
    t2 = new MonitorMutexTestThread2(monitor);
    t2->start();
    t3 = new MonitorMutexTestThread2(monitor);
    t3->start();
    
    Thread::ssleep(Time::seconds(1));

    {
        Monitor<Mutex>::Lock lock(monitor);
        monitor.notify();
    }

    Thread::ssleep(Time::seconds(1));
    
    test((t2->_finished && !t3->_finished )
          || (t3->_finished && !t2->_finished));
     
    {
        Monitor<Mutex>::Lock lock(monitor);
        monitor.notify();
    }

    t2->join();
    t3->join();

    Thread::ssleep(Time::seconds(1));


    //test notifyAll()
    t2 = new MonitorMutexTestThread2(monitor);
    t2->start();
    t3 = new MonitorMutexTestThread2(monitor);
    t3->start();
    
    Thread::ssleep(Time::seconds(1));

    {
        Monitor<Mutex>::Lock lock(monitor);
        monitor.notifyAll();
    }

    Thread::ssleep(Time::seconds(1));
     
    t2->join();
    t3->join();

    // test timeWait()
    {
         Monitor<Mutex>::Lock lock(monitor);
         try
         {
             monitor.timedWait(Time::milliSeconds(-1));
             test(false);
         }
         catch( const std::exception& ex )
         {
              
         }
         const bool bRet = monitor.timedWait(Time::milliSeconds(500));
         cout<<"bRet: "<<bRet<<endl;
         test(bRet);
    }
}

class RecMutexTestThread: public Thread
{
public:
    RecMutexTestThread( RecMutex& m ):
        _mutex( m ),
        _tryLock( false )
    {

    }
    
    virtual void run()
    {
        RecMutex::TryLock tlock(_mutex);
        test(!tlock.acquired());
        {
            Mutex::Lock lock(_tryLockMutex);
            _tryLock = true;
        }
        _tryLockCond.signal();

        RecMutex::Lock lock(_mutex);
    }

    void waitTryLock()
    {
        Mutex::Lock lock(_tryLockMutex);
        while(!_tryLock)
        {
            _tryLockCond.wait(lock);
        }
    }
    RecMutex& _mutex;
    bool _tryLock;
    Cond _tryLockCond;
    Mutex _tryLockMutex;
};

typedef Handle<RecMutexTestThread> RecMutexTestThreadPtr;

static const std::string recMutexName("RecMutex");
class RecMutexTest : public testBase
{
public:
    RecMutexTest():
        testBase(recMutexName)
    {

    }
    virtual void run();
};

void RecMutexTest::run()
{
    RecMutex mutex;
    RecMutexTestThreadPtr t1;
    {
         RecMutex::Lock lock(mutex);
         cout<<"lock 1"<<endl;
         RecMutex::Lock lock2(mutex);
         cout<<"lock 2"<<endl;
         RecMutex::TryLock lock3(mutex);
         cout<<"lock 3"<<endl;
         test(lock3.acquired());

         t1 = new RecMutexTestThread(mutex);
         t1->start();
         t1->waitTryLock();
    }
    t1->join();
}

#include <list>
 
using namespace tbutil;
std::list<testBasePtr> allTests;
 
int main()
{
   cout<<(2<<31-1)<<endl;
   allTests.push_back( new AliveTest() );
   allTests.push_back( new MonitorMutxTest() );
   allTests.push_back( new RecMutexTest() );
   std::list<testBasePtr>::const_iterator p = allTests.begin();
   for(; p != allTests.end(); ++p)
   {
            (*p)->start();
   }
   return 0;
}
