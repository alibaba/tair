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

#include <signal.h>
#include "CtrlCHandler.h"
#include "StaticMutex.h"

using namespace std;

namespace tbutil
{
static CtrlCHandlerCallback _callback = 0;
static const CtrlCHandler* _handler = 0;

CtrlCHandlerException::CtrlCHandlerException(const char* file, int line) :
    Exception(file, line)
{
}

static const char* ctrlCHandlerName = "tbutil::CtrlCHandlerException";

string CtrlCHandlerException::_name() const
{
    return ctrlCHandlerName;
}

Exception* CtrlCHandlerException::_clone() const
{
    return new CtrlCHandlerException(*this);
}

void CtrlCHandlerException::_throw() const
{
    throw *this;
}

void CtrlCHandler::setCallback(CtrlCHandlerCallback callback)
{
    StaticMutex::Lock lock(globalMutex);
    _callback = callback;
}

CtrlCHandlerCallback 
CtrlCHandler::getCallback() const
{
    StaticMutex::Lock lock(globalMutex);
    return _callback;
}

extern "C" 
{

static void* sigwaitThread(void*)
{
    sigset_t ctrlCLikeSignals;
    sigemptyset(&ctrlCLikeSignals);
    sigaddset(&ctrlCLikeSignals, SIGHUP);
    sigaddset(&ctrlCLikeSignals, SIGINT);
    sigaddset(&ctrlCLikeSignals, SIGTERM);
    sigaddset(&ctrlCLikeSignals, SIGUSR1);

    for(;;)
    {
        int signal = 0;
        int rc = sigwait(&ctrlCLikeSignals, &signal);
        if(rc == EINTR)
        {
            continue;
        }
        assert(rc == 0);
        
        rc = pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, 0);
        assert(rc == 0);
        
        CtrlCHandlerCallback callback = _handler->getCallback();

        if(callback != 0)
        {
            callback(signal);
        }

        rc = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, 0);
        assert(rc == 0);
    }
    return 0;
}
}

static pthread_t _tid;

CtrlCHandler::CtrlCHandler(CtrlCHandlerCallback callback)
{
    StaticMutex::Lock lock(globalMutex);
    if(_handler != 0)
    {
#ifdef _NO_EXCEPTION
        assert( 0 == _handler );
        if ( 0 != _handler )
        {
            TBSYS_LOG(ERROR,"%s","CtrlCHandlerException");
        } 
#else
        throw CtrlCHandlerException(__FILE__, __LINE__);
#endif
    }
    else
    {
        _callback = callback;
        _handler = this;
        lock.release();
        
        sigset_t ctrlCLikeSignals;
        sigemptyset(&ctrlCLikeSignals);
        sigaddset(&ctrlCLikeSignals, SIGHUP);
        sigaddset(&ctrlCLikeSignals, SIGINT);
        sigaddset(&ctrlCLikeSignals, SIGTERM);
        sigaddset(&ctrlCLikeSignals, SIGUSR1);
        int rc = pthread_sigmask(SIG_BLOCK, &ctrlCLikeSignals, 0);
        assert(rc == 0);

        // Joinable thread
        rc = pthread_create(&_tid, 0, sigwaitThread, 0);
        assert(rc == 0);
    }
}

CtrlCHandler::~CtrlCHandler()
{
    int rc = pthread_cancel(_tid);
    assert(rc == 0);
    void* status = 0;
    rc = pthread_join(_tid, &status);
    assert(rc == 0);
    {
        StaticMutex::Lock lock(globalMutex);
        _handler = 0;
    }
}
}
