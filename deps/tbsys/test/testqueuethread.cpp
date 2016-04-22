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

#include <tbsys.h>

using namespace tbsys;

int mWriteCount = 1000;
atomic_t mReadCount;
CQueueThread *mQueueThread = NULL;

class CMyHandler : public IQueueHandler
{
public:
bool handleQueue (void *data, int len, int threadIndex, void *arg)
{
//    printf("TEST==> read_thread: %lu(%d) %d (%s)\n", pthread_self(), threadIndex, len, (char*)data);
//    fflush(stdout);
    if (mWriteCount == atomic_add_return(1, &mReadCount) && mQueueThread) {
        mQueueThread->stop();
        mQueueThread = NULL;
    }
    return 0; 
}
};

int main(int argc, char *argv[])
{
    if (argc>1) {
        mWriteCount = atoi(argv[1]);
    }
    CMyHandler handler;
    CQueueThread queueThread(3, &handler, NULL);
    mQueueThread = &queueThread;
    queueThread.start();
    char data[1024];
    for(int i=1; i<=mWriteCount; i++) {
        int len = sprintf(data, "data_%05d", i);
        queueThread.writeData(data, len+1);
//        printf("TEST==> writeData: %d, (%s)\n", i, data);
//        fflush(stdout);
        //if (rand() % 111 == 0) {
            //usleep(100000);
        //}
    }
    queueThread.wait(); 

    printf("mReadCount: %d\n", atomic_read(&mReadCount));
    printf("OK\n");

    return 0;
}

