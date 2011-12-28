/**
 * Synchronization class for ALI-WANGWANG project
 * include class CReadLock, CWriteLock, CMutexLock, CSemaphore
 * @version 1.0
 */

#ifndef __SYNC_PROC_H_
#define __SYNC_PROC_H_

#ifdef _WIN32
#include <windows.h>
#else
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#endif

class CReadLock
{
public:
    CReadLock(pthread_rwlock_t& rwlock) : m_plock(&rwlock)
    {
        pthread_rwlock_rdlock(m_plock);
    }
    ~CReadLock()
    {
        pthread_rwlock_unlock(m_plock);
        m_plock = NULL;
    }
private:
    CReadLock() : m_plock(NULL) { }
private:
    pthread_rwlock_t* m_plock;
};

class CWriteLock
{
public:
    CWriteLock(pthread_rwlock_t& rwlock) : m_plock(&rwlock)
    {
        pthread_rwlock_wrlock(m_plock);
    }
    ~CWriteLock()
    {
        pthread_rwlock_unlock(m_plock);
        m_plock = NULL;
    }
private:
    CWriteLock() : m_plock(NULL) { }
private:
    pthread_rwlock_t* m_plock;
};

typedef enum
{
    RLOCK,
    WLOCK
} RWLOCK_FLAG;

class CRwLock
{
public:
    CRwLock(pthread_rwlock_t& rwlock, RWLOCK_FLAG lockFlag) : m_plock(&rwlock)
    {
        if(lockFlag == RLOCK)
            pthread_rwlock_rdlock(m_plock);
        else
            pthread_rwlock_wrlock(m_plock);
    }
    ~CRwLock()
    {
        pthread_rwlock_unlock(m_plock);
        m_plock = NULL;
    }
    void ChangeLock(RWLOCK_FLAG lockFlag)
    {
        pthread_rwlock_unlock(m_plock);
        if(lockFlag == RLOCK)
            pthread_rwlock_rdlock(m_plock);
        else
            pthread_rwlock_wrlock(m_plock);
    }
private:
    CRwLock() : m_plock(NULL) { }
private:
    pthread_rwlock_t* m_plock;
};

class CMutexLock
{
public:
	CMutexLock(pthread_mutex_t& mutex): m_pmutex(&mutex)
	{
		pthread_mutex_lock(m_pmutex);
	}
	~CMutexLock()
	{
		pthread_mutex_unlock(m_pmutex);
		m_pmutex = NULL;
	}
	void Lock()
    {
		pthread_mutex_lock(m_pmutex);
	}
	void UnLock()
    {
		pthread_mutex_unlock(m_pmutex);
	}
	bool IsLocked()
    {
		return (pthread_mutex_trylock(m_pmutex) == 0) ? true : false;
	}
private:
    CMutexLock() : m_pmutex(NULL) { }
private:
	pthread_mutex_t* m_pmutex;
};

class CSemaphore
{
public:
    CSemaphore()
    {
        sem_init(&m_sem, 0, 0);
    }
    ~CSemaphore()
    {
        sem_destroy(&m_sem);
    }
    void Produce()
    {
        sem_post(&m_sem);
    }
    void Consume()
    {
		while (sem_wait(&m_sem) != 0)
		{
			sched_yield();
		}
    }
    bool Try()
    {
        int value = 0;
        int ret = sem_getvalue(&m_sem, &value);
        if(ret < 0 || value <= 0)
            return false;
        return true;
    }
	//微秒部分，尽量不要使用超过500000微秒的值，不然会导致CPU或者LOAD上升。化整到秒的部分是没有问题的，比如2000000即2秒.
    bool TryTime(int micSec)
    {
	    struct timespec ts;
		clock_gettime(CLOCK_REALTIME,&ts);
		if(micSec >= 1000000)
			ts.tv_sec += micSec/1000000;
		ts.tv_nsec += micSec%1000000*1000;
		if(ts.tv_nsec >= 1000000000)
		{
			++ts.tv_sec;
			ts.tv_nsec -= 1000000000;
		}

		int ret = sem_timedwait(&m_sem,&ts);
        if(ret < 0)
            return false;
        return true;
    }


    int GetCount()
    {
        int value = 0;
        int ret = sem_getvalue(&m_sem, &value);
        if(ret < 0)
            return -1;
        else
            return value;
    }
private:
    sem_t  m_sem;
};

static struct sembuf lock={0,-1,SEM_UNDO};
static struct sembuf unlock={0,1,SEM_UNDO|IPC_NOWAIT};

class CSemOper
{
public:
    CSemOper()
    {
    }
    CSemOper(int semid)
    {
        m_semid = semid;
    }
    ~CSemOper()
    {
    }
    void SetSemid(int semid)
    {
        m_semid = semid;
    }
    void Produce()
    {
        semop(m_semid, &unlock, 1);
    }
    void Consume()
    {
	while (semop(m_semid, &lock, 1) != 0)
	{
		sched_yield();
	}
    }
    int GetCount()
    {
        return semctl(m_semid, 0, GETVAL, 0);
    }
private:
    int  m_semid;
};

class CSemLock
{
public:
    CSemLock(int semid)
    {
        m_semid = semid;
        semop(m_semid, &lock, 1);
    }
    ~CSemLock()
    {
        semop(m_semid, &unlock, 1);
    }
private:
    int  m_semid;
};

class CCondLock
{
public:
    CCondLock()
    {
        pthread_mutex_init(&m_mutex, NULL);
        pthread_cond_init(&m_cond, NULL);
        m_count = 0;
    }
    ~CCondLock()
    {
        pthread_cond_destroy(&m_cond);
        pthread_mutex_destroy(&m_mutex);
    }
    void Produce()
    {
        pthread_mutex_lock(&m_mutex);
        pthread_cond_signal(&m_cond);
        m_count++;
        pthread_mutex_unlock(&m_mutex);
    }
    void Consume()
    {
        pthread_mutex_lock(&m_mutex);
        while(m_count == 0)
        {
            pthread_cond_wait(&m_cond, &m_mutex);
        }
        m_count--;
        pthread_mutex_unlock(&m_mutex);
    }
    void Consume(int tmms)
    {
	    struct timeval now;
        struct timespec timeout;
        struct timezone tz;
        gettimeofday(&now, &tz);
        int sec = tmms/ 1000;
        int nano = (tmms- sec*1000)*1000000;
        timeout.tv_sec = now.tv_sec + tmms/1000;
        timeout.tv_nsec = now.tv_usec * 1000 + nano;

        pthread_mutex_lock(&m_mutex);
        while(m_count == 0)
        {
		    pthread_cond_timedwait(&m_cond, &m_mutex, &timeout);
        }
        if(m_count > 0) m_count--;
	    pthread_mutex_unlock(&m_mutex);
    }

	int ConsumeTimeWait(int tmms)
	{
		struct timeval now;
		struct timespec timeout;
		struct timezone tz;
		gettimeofday(&now, &tz);
		int sec = tmms/1000;
		int nano = (tmms%1000)*1000000;
		timeout.tv_sec = now.tv_sec + sec;
		timeout.tv_nsec = now.tv_usec*1000 + nano;

		if(timeout.tv_nsec >= 1000000000)
		{
			timeout.tv_sec += 1;
			timeout.tv_nsec -= 1000000000;
		}

		pthread_mutex_lock(&m_mutex);
		if(m_count > 0)
		{
			m_count--;
			pthread_mutex_unlock(&m_mutex);
			return 0;
		}

		int ret = pthread_cond_timedwait(&m_cond, &m_mutex, &timeout);
		if((0 == ret) && (m_count > 0))
			m_count--;
		else
			ret = -1;
		pthread_mutex_unlock(&m_mutex);
		return ret;
	}
private:
    pthread_mutex_t m_mutex;
    pthread_cond_t  m_cond;
    size_t          m_count;
};

class CMutexObj
{
public:
	CMutexObj(){
		pthread_mutex_init(&m_mutex, NULL);
	}
	~CMutexObj(){
		if(IsLocked())
			UnLock();	
	}
	void Lock(){
		pthread_mutex_lock(&m_mutex);
	}
	void UnLock(){
		pthread_mutex_unlock(&m_mutex);
	}	
	bool IsLocked()
    {
		return (pthread_mutex_trylock(&m_mutex) == 0) ? true : false;
	}
	
private:	
	pthread_mutex_t m_mutex;
};
class CMutexObjLock{
public:
	CMutexObjLock(CMutexObj& mutex):m_pmutexObj(&mutex)
	{
		Lock();
	}
	~CMutexObjLock(){
		UnLock();
	}
	void Lock(){
		m_pmutexObj->Lock();
	}
	void UnLock(){
		m_pmutexObj->UnLock();
	}

private:
	CMutexObj* m_pmutexObj;
};
#endif
