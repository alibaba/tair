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
#include <Service.h>

#include "testCommon.h"
#include <vector>

using namespace tbutil;
using namespace std;

class testService : public Service
{
public:
     testService(){}
     ~testService(){}
public:
    virtual int run(int argc, char*argv[], const std::string& config, std::string& errMsg)
    {
        //errMsg += "[ERROR]: this is test\r\n";
        return 0;
    }
    virtual int interruptCallback( int signal )
    {
        switch(signal)
        {
            case SIGUSR1:
                 printf("SIGUSR1\n");
                 break;
            case SIGPIPE:
                 printf("SIGPIPE\n");
                 break;
            case SIGTERM:
                 printf("SIGTERM\n");
                 break;
            case SIGINT:
                 printf("SIGINT\n");
                 break;
            case SIGSEGV:
                 printf("SIGSEGV\n");
                 break;
            case SIGABRT:
                 printf("SIGABRT\n");
                 break;
            case 40:
                 printf("40\n");
            case SIGHUP:
                 printf("SIGHUP\n");
                 break;
        }
        return 0;
    }
    virtual bool destroy()
    {
         return true;
    }
};

int main(int argc, char* argv[])
{
    testService app;
    app.main(argc, argv);
    return 0;
}

