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

#ifndef TBSYS_QUEUE_THREAD_H
#define TBSYS_QUEUE_THREAD_H

#include <queue>

//using namespace std;

namespace tbsys {
        
	/** 
	 * @brief Queue 运行线程池
	 */
    class CQueueThread : public CDefaultRunnable {
        public:
            /* 构造函数 */
            CQueueThread(int threadCount, IQueueHandler *handler, void *args);
            /* 析构函数 */
            ~CQueueThread(void);
            /* 写入数据 */
            int writeData(void *data, int len);
            /* 停止 */
            void stop();
            /* 运行入口函数 */
            void run(CThread *thread, void *args);
                   
        private:
            typedef struct data_pair {
                char *data;
                int len;
            } data_pair;
            // queue
            std::queue<data_pair*> _queue;            
            
        protected:
            // 线程锁
            CThreadCond _mutex;
            // 处理函数
            IQueueHandler *_handler;
            // 函数参数
            void *_args;
    };
}

#endif

//////////////////END
