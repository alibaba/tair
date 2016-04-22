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

#include "ThreadPool.h"
#include "Functional.h"
#include "Exception.h"
#include "Memory.hpp"

using namespace std;
namespace tbutil
{
ThreadPool::ThreadPool(int size , int sizeMax, int sizeWarn,int listSizeMax,int stackSize) :
    _destroyed(false),
    _listSize( 0 ),
    _procSize( 0 ),
    _listSizeMax( listSizeMax),
    _size(size),
    _sizeMax(sizeMax),
    _sizeWarn(sizeWarn),
    _stackSize(0),
    _running(0),
    _inUse(0),
    _load(1.0),
    _promote(true),
    _waitingNumber(0)
{
    if ( size < 1 )
         size = 1;

    if ( sizeMax < size )
          sizeMax = size;

    if ( sizeWarn > sizeMax )
         sizeWarn = sizeMax;

    if ( stackSize < 0 )
         stackSize = 16 * 1024 * 1024;
    
    const_cast<int&>(_size) = size;
    const_cast<int&>(_sizeMax) = sizeMax;
    const_cast<int&>(_sizeWarn) = sizeWarn;
    const_cast<size_t&>(_stackSize) = static_cast<size_t>(stackSize);

    try
    {
        for(int i = 0 ; i < _size ; ++i)
        {
            ThreadPtr thread = new EventHandlerThread(this);
            thread->start(_stackSize);
            _threads.push_back(thread);
            ++_running;
        }
    }
    catch(const Exception& ex)
    {
        destroy();
        joinWithAllThreads();
    }
}

ThreadPool::~ThreadPool()
{
    assert(_destroyed);
    _monitor.lock();
    TBSYS_LOG(DEBUG,"_workItem.size: %d,_listSize: %d,_procSize: %d", _workItems.size(),_listSize,_procSize);
    while( !_workItems.empty() ) 
    {
        ThreadPoolWorkItem* workItem = _workItems.front();
        if ( workItem != NULL )
        {
            workItem->destroy();
            tbsys::gDelete(workItem);
        }       
        _workItems.pop_front();
    }
    _monitor.unlock();
}

void ThreadPool::destroy()
{
    this->lock();
    assert(!_destroyed);
    _destroyed = true;
    this->notifyAll();
    this->unlock();
    _monitor.lock();
    _monitor.notifyAll();
    _monitor.unlock();
}

int ThreadPool::execute(ThreadPoolWorkItem* workItem)
{
    if ( _destroyed)
    {
        //TBSYS_LOG(ERROR,"%s","ThreadPoolDestroyedException");
        return -1;
    }
    if (_listSize > _listSizeMax )
    {
        //TBSYS_LOG(DEBUG,"%s","ThreadPoolQueueFullException");
        return -1;
    }

    _monitor.lock();
    _workItems.push_back(workItem);
    ++_listSize;
    _monitor.notify();
    _monitor.unlock();
    return 0;
}

void ThreadPool::promoteFollower( pthread_t thid )
{
    if(_sizeMax > 1)
    {
        this->lock();
        assert(!_promote);
        _promote = true;
        this->notify();

        if(!_destroyed)
        {
            assert(_inUse >= 0);
            ++_inUse;
            
            if(_inUse == _sizeWarn)
            {
                /*cout << "thread pool `" << "' is running low on threads\n"
                    << "Size=" << _size << ", " << "SizeMax=" << _sizeMax << ", " 
		    << "SizeWarn=" << _sizeWarn<<", " <<"_inUse=" <<_inUse << ", "
		    << "_running=" << _running <<". " <<"threads.size="<< _threads.size() << ", " 
		    << "_workItems.size="<< _workItems.size()<<endl;*/
            }
            
            assert(_inUse <= _running);
            if(_inUse < _sizeMax && _inUse == _running)
            {
                try
                {
                    ThreadPtr thread = new EventHandlerThread(this);
                    thread->start(_stackSize);
                    _threads.push_back(thread);
                    ++_running;
                }
                catch(const Exception& ex)
                {
		     throw ThreadCreateException(__FILE__,__LINE__);
                }
            }
        }
        this->unlock();
    }
}

void ThreadPool::joinWithAllThreads()
{
    assert(_destroyed);
    for(vector<ThreadPtr>::iterator p = _threads.begin(); p != _threads.end(); ++p)
    {
        (*p)->join();
    }
}


bool ThreadPool::isMaxCapacity() const
{
    return _listSize > _listSizeMax ? true:false;
}

bool ThreadPool::run( pthread_t thid)
{
    ThreadPool* self = this;
    while(true)
    {
        #ifdef DEBUG
        cout<<"["<<thid<<"]debug-1: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
        #endif
        if( _sizeMax > 1 )
	{
            this->lock();
	    while( !_promote )
	    {
                #ifdef DEBUG
                cout<<"["<<thid<<"]debug-2: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
                #endif
                const bool bRet = this->wait();
                #ifdef DEBUG
                cout<<"["<<thid<<"]debug-3: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
                #endif
                if ( !bRet )
                {
                    --_inUse;
                    --_running;
                    this->unlock();
                    return false;
                }
                #ifdef DEBUG
                cout<<"["<<thid<<"]debug-4: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
                #endif
	    }
	    _promote = false;
            this->unlock();
	}

        #ifdef DEBUG
        cout<<"["<<thid<<"]debug-5: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
        #endif
        _monitor.lock();
	while( (_workItems.empty() && !_destroyed) )
	{
            #ifdef DEBUG
            cout<<"["<<thid<<"]debug-6: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
            #endif
	    const bool bRet = _monitor.wait();
            #ifdef DEBUG
            cout<<"["<<thid<<"]debug-7: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
            #endif
            if ( !bRet )
            {
                --_inUse;
                --_running;
                _monitor.unlock();
                return false;
            }
            #ifdef DEBUG
            cout<<"["<<thid<<"]debug-8: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
            #endif
	}

        #ifdef DEBUG
        cout<<"["<<thid<<"]debug-9: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
        #endif
        if ( _destroyed && _workItems.empty()) 
        {
            _monitor.unlock();
            return true;
        }

        ThreadPoolWorkItem* workItem = NULL;
        workItem = _workItems.front();
        _workItems.pop_front();
        ++_procSize;
        --_listSize;
        _monitor.unlock();

        #ifdef DEBUG
        cout<<"["<<thid<<"]debug-10: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
        #endif
        promoteFollower(thid);
        #ifdef DEBUG
        cout<<"["<<thid<<"]debug-11: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
        #endif
        if( workItem != NULL && !_destroyed )
        {
            try
            {
                workItem->execute(self);
            }
            catch(const Exception& ex)
            {
	        cout << "exception in" << "while calling execute():"<<ex.what()<<endl;
            }
            tbsys::gDelete( workItem );
        }
        else
        {
            if ( workItem != NULL )
            {
                workItem->destroy();
                tbsys::gDelete( workItem );
            }
        }
        #ifdef DEBUG
        cout<<"["<<thid<<"]debug-12: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
        #endif
        if(_sizeMax > 1)
        {
            this->lock();
            if(!_destroyed)
            {
                const int sz = static_cast<int>(_threads.size());
                assert(_running <= sz);
                if(_running < sz)
                {
                    vector<ThreadPtr>::iterator start =
                       partition(_threads.begin(), _threads.end(), constMemFun(&Thread::isAlive));

                    for(vector<ThreadPtr>::iterator p = start; p != _threads.end(); ++p)
                    {
                        (*p)->join();
                        --_running;
                    }

                    _threads.erase(start, _threads.end());
                }
                #ifdef DEBUG
                cout<<"["<<thid<<"]debug-13: size:"<<_workItems.size()<<",_promote: "<<_promote<<endl ;
                #endif
                /*double inUse = static_cast<double>(_inUse);
                if(_load < inUse)
                {
                    _load = inUse;
                }
                else
                {
                    const double loadFactor = 0.05; // TODO: Configurable?
                    const double oneMinusLoadFactor = 1 - loadFactor;
                    _load = _load * oneMinusLoadFactor + inUse * loadFactor;
                }
                
                if(_running > _size)
                {
                    cout<<"_running: "<<_running<<"_size :"<<_size<<endl;
                    int load = static_cast<int>(_load + 0.5);

                    if(load + 1 < _running)
                    {
                        assert(_inUse > 0);
                        --_inUse;
                        
                        assert(_running > 0);
                        --_running;
                        this->unlock();    
                        return false;
                    }
                }*/
                assert(_inUse > 0);
                --_inUse;
            }
            this->unlock();
        }//end _sizeMax > 1
    }//end while
}

ThreadPool::EventHandlerThread::EventHandlerThread(const ThreadPool* pool) 
{
    _pool = (ThreadPool*)pool;
}

void ThreadPool::EventHandlerThread::run()
{
    bool promote;

    try
    {
        promote = _pool->run( id() );
    }
    catch(const std::exception& ex)
    {
        promote = true;
	cout<<ex.what() <<endl;
    }
    catch(...)
    {
        cout << "unknown exception in ThreadPool::EventHandlerThread::run() "<<endl; 
        promote = true;
    }

    #ifdef DEBUG
    cout<<"["<<id()<<"]debug-14:_promote: "<<_pool->_promote<<endl ;
    #endif
    if( promote && _pool->_sizeMax > 1)
    {
        {
            _pool->lock();
            assert(!_pool->_promote);
            _pool->_promote = true;
            _pool->notify();
            _pool->unlock();
        }
    }
}
}//end namespace tbutil
