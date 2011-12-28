#ifndef __BLOCK_QUEUE_H_
#define __BLOCK_QUEUE_H_

#include <stdint.h>
#include <deque>
#include <queue>
#include "syncproc.hpp"

const size_t MAX_QUEUE_SIZE = 300000;

template <class __T>
class CBlockQueue 
{
public:
	CBlockQueue(size_t queMax=MAX_QUEUE_SIZE)
    {
	m_queMax=queMax;
        pthread_mutex_init(&m_mutex, NULL);
    }
	void SetMaxQueueSize(size_t queMax)
	{
        CMutexLock lock(m_mutex);
		m_queMax = queMax;
	}
	virtual  ~CBlockQueue()
    {
        pthread_mutex_destroy(&m_mutex);
    }
	bool Empty()
    {
        //CMutexLock lock(m_mutex);
        return m_queue.empty();
    }
	uint32_t Size()
    {
        CMutexLock lock(m_mutex);
        return (uint32_t)m_queue.size();
    }
	bool Put(const __T& value)
    {
        CMutexLock lock(m_mutex);
        //if(m_queue.size() > MAX_QUEUE_SIZE)
        if(m_queue.size() > m_queMax)
        {
            return false;
        }
        m_queue.push_back(value);
        m_semaphore.Produce();
        return true;
    }
	bool Get(__T& value)
    {
        m_semaphore.Consume();
        CMutexLock lock(m_mutex);
        if(m_queue.empty())
            return false;
        value = m_queue.front();
        m_queue.pop_front();
        //m_semaphore.Produce();
        return true;
    }
    bool Peek(__T& value)
    {
        CMutexLock lock(m_mutex);
        if(m_queue.empty())
            return false;
        value = m_queue.front();
        return true;
    }
    bool Pop()
    {
        CMutexLock lock(m_mutex);
        if(m_queue.empty())
            return false;
        m_queue.pop_front();
        m_semaphore.Consume();
        return true;
    }

    bool TryGet(__T& value)
    {
        if(!m_semaphore.Try())
            return false;
        CMutexLock lock(m_mutex);
        if(m_queue.empty())
            return false;
        value = m_queue.front();
        m_queue.pop_front();
        return true;
    }
	//微秒部分，尽量不要使用超过500000微秒的值，不然会导致CPU或者LOAD上升。化整到秒的部分是没有问题的，比如2000000即2秒。
	bool TryGetTime(__T& value, int micSec)
    {
        if(!m_semaphore.TryTime(micSec))
            return false;
        CMutexLock lock(m_mutex);
        if(m_queue.empty())
            return false;
        value = m_queue.front();
        m_queue.pop_front();
        return true;
    }

    void Clear()
    {
        CMutexLock lock(m_mutex);
        m_queue.clear();
    }

private:
    pthread_mutex_t m_mutex;
	std::deque<__T> m_queue;
	CSemaphore m_semaphore;
	size_t  m_queMax;
};

#endif

