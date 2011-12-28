#ifndef __BLOCKQUEUEEX_H
#define __BLOCKQUEUEEX_H

#include "blockqueue.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

using namespace std;

template <class T>
class BlockQueueEx 
{
public:
	BlockQueueEx(); 
	~BlockQueueEx(); 
	bool isEmpty();
	unsigned long size();
	bool put(const T& value); 
	T get();
	T get(int timeout);//ms
private:
	CBlockQueue<T> inQueue;
#ifdef _WIN32
	HANDLE bqs;
#else
	pthread_mutex_t write_mutex;
	pthread_cond_t more;
#endif 
};

template <class T>
BlockQueueEx<T>::BlockQueueEx()
{
#ifdef _WIN32
	bqs = CreateSemaphore( 
		NULL,   // default security attributes
		0,   // initial count
		1000,   // maximum count
		NULL);  // unnamed semaphore
	
	if (bqs == NULL) 
	{
		DWORD dwerror= GetLastError();
		printf("CreateSemaphore error: %d\n", GetLastError());
		return ;
	}
#else
	pthread_mutex_init(&write_mutex, NULL);
	pthread_cond_init(&more, NULL);
#endif
}

template <class T>
BlockQueueEx<T>::~BlockQueueEx()
{
#ifdef _WIN32
	CloseHandle(bqs);
#else
	pthread_mutex_destroy(&write_mutex);
#endif
}

template <class T>
bool BlockQueueEx<T>::isEmpty()
{
	return inQueue.Empty();
}

template <class T>
unsigned long BlockQueueEx<T>::size()
{
    return (unsigned long)inQueue.Size();

}

template <class T>
bool BlockQueueEx<T>::put(const T& value)
{
	
#ifdef _WIN32
	inQueue.push(value);
	ReleaseSemaphore(bqs,1,NULL);
#else
  bool bret=inQueue.Put(value);
	pthread_mutex_lock(&write_mutex);
	//inQueue.push(value); //put here is the bug
	pthread_cond_signal(&more);
	pthread_mutex_unlock(&write_mutex);
#endif
	if (inQueue.Size() > 100000)
	{
		//usleep(500000);
	}
  return bret;
}

template <class T>
T BlockQueueEx<T>::get()
{
	T retT;
#ifdef _WIN32
	
	DWORD	dwWaitResult = WaitForSingleObject( 
        bqs,   // handle to semaphore
        INFINITE);          // zero

	if(WAIT_OBJECT_0 !=dwWaitResult )
		return NULL;
	
	if(!inQueue.Get(retT)) retT = NULL;
	return retT;
#else
	pthread_mutex_lock(&write_mutex);
	//while( count <= 0)
	while(inQueue.Empty())
	{
		pthread_cond_wait(&more, &write_mutex);
	}
	if(!inQueue.Get(retT)) retT = NULL;
	pthread_mutex_unlock(&write_mutex);
	return retT;
#endif

	
}

template <class T>
T BlockQueueEx<T>::get(int tmms)
{
	T retT;
#ifdef _WIN32
	
	DWORD	dwWaitResult = WaitForSingleObject( 
        bqs,   // handle to semaphore
        tmms);          // zero

	if(WAIT_OBJECT_0 !=dwWaitResult )
		return NULL;
	if(!inQueue.Get(retT)) retT = NULL;
	return retT;
#else
	struct timeval now;
    struct timespec timeout;
    struct timezone tz;
    int retcode=0;
    gettimeofday(&now,&tz);
    int sec = tmms/ 1000;
    int nano = (tmms- sec*1000)*1000000;
    timeout.tv_sec = now.tv_sec + tmms/1000;
    //timeout.tv_nsec = now.tv_usec * 1000 + tmms%1000;
    timeout.tv_nsec = now.tv_usec * 1000 + nano;

    pthread_mutex_lock(&write_mutex);
	//while( count <= 0 && retcode != ETIMEDOUT)
	while(inQueue.Empty() && retcode != ETIMEDOUT)
	{
		retcode = pthread_cond_timedwait(&more, &write_mutex, &timeout);
	}
	if(ETIMEDOUT == retcode)
	{
		pthread_mutex_unlock(&write_mutex);
		return NULL;	
	}
	pthread_mutex_unlock(&write_mutex);
	if(!inQueue.Get(retT)) retT = NULL;
	return retT;
#endif

	
}
#endif //__BLOCKQUEUE_H
