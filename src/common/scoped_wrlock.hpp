#ifndef __CSCOPEDRWLOC_H
#define __CSCOPEDRWLOC_H

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

namespace tair {
  namespace common{
class CSlotLocks
{
	public:
		explicit CSlotLocks(int slot_number)
		{
			this->bucket_number = slot_number;
			b_locks = NULL;
			init();
		}

		~CSlotLocks()
		{

        for(int i = 0; i < bucket_number; i++)
        {
          pthread_rwlock_destroy(b_locks + i);
        }

        if(b_locks != NULL && bucket_number > 0)
        {
          delete[]b_locks;
        }
      }
		pthread_rwlock_t *getlock(int index)
		{
			return b_locks + index;
		}
	private:
		void init()
		{
			b_locks = new pthread_rwlock_t[bucket_number];
			for(int i = 0; i < bucket_number; i++) {
				if(pthread_rwlock_init((pthread_rwlock_t *) b_locks + i, NULL) != 0) {
					// destory inited locks
					for(int j = i; j >= 0; j--) {
						pthread_rwlock_destroy(b_locks + j);
					}
					exit(1);
				}
			}
		}
	private:
		pthread_rwlock_t * b_locks;
		int bucket_number;
};

class CScopedRwLock
{
	public:
		CScopedRwLock(pthread_rwlock_t* rwlock, bool _write) : m_plock(rwlock)
	{
		if(!_write)
			pthread_rwlock_rdlock(m_plock);
		else
			pthread_rwlock_wrlock(m_plock);
	}
		~CScopedRwLock()
		{
			pthread_rwlock_unlock(m_plock);
			m_plock = NULL;
		}
		void ChangeLock(bool _write)
		{   
			pthread_rwlock_unlock(m_plock);
			if(!_write)
				pthread_rwlock_rdlock(m_plock);
			else
				pthread_rwlock_wrlock(m_plock);
		} 
	private:
		CScopedRwLock() : m_plock(NULL) { }
	private:
		pthread_rwlock_t* m_plock;
};
}
}
#endif
