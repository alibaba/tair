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

#ifndef TBSYS_FILE_QUEUE_THREAD_H
#define TBSYS_FILE_QUEUE_THREAD_H

//using namespace std;

namespace tbsys {
	/** 
	 * @brief File Queue 运行线程池
	 */
    class CFileQueueThread : public CDefaultRunnable {
        public:
            /** 构造函数 */
            CFileQueueThread(CFileQueue *queue, int threadCount, IQueueHandler *handler, void *args);
            /** 析构函数 */
            ~CFileQueueThread(void);
            /** 写入数据 */
            int writeData(void *data, int len);
            /** 停止 */
            void stop();
            /** 运行入口函数 */
            void run(CThread *thread, void *args);
            
        private:
            /** 线程锁*/
            CThreadCond _mutex;
            /** 处理函数*/
            IQueueHandler *_handler;
            /** 函数参数*/
            void *_args;
            /** file queue*/
            CFileQueue *_queue;
    };
}

#endif

//////////////////END
