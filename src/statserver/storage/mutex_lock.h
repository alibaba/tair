#ifndef MUTEX_LOCK_H
#define MUTEX_LOCK_H

#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>

#include "log.hpp"

namespace flstorage
{
class RWLock
{
 public:
  RWLock()
  {
    if (pthread_rwlock_init(&lock_, NULL) != 0)
    {
      abort();
    }
  }

  void WLock()
  {
    pthread_rwlock_wrlock(&lock_);
  }

  void RLock()
  {
    pthread_rwlock_rdlock(&lock_);
  }

  void UnLock()
  {
    pthread_rwlock_unlock(&lock_);
  }

  ~RWLock() 
  {
    pthread_rwlock_destroy(&lock_);
  }

 private:
  pthread_rwlock_t lock_;
};

class Mutex
{
 public:
  Mutex()
  {
    if (pthread_mutex_init(&lock_, NULL) != 0)
    {
      abort();
    }
    if (pthread_cond_init(&cond_, NULL) != 0)
    {
      abort();
    }
  }

  void Lock()
  {
    pthread_mutex_lock(&lock_);
  }

  void UnLock()
  {
    pthread_mutex_unlock(&lock_);
  }

  void Wait(time_t second)
  {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += second;
    int rc = pthread_cond_timedwait(&cond_, &lock_, &ts); 
    if (rc == ETIMEDOUT) 
    {
      log_debug("time pass: %s", strerror(rc));
    } 
    else if (rc != 0) 
    {
      log_error("cond wait error: %s", strerror(rc));
    }
  }

  void Signal()
  {
    pthread_cond_signal(&cond_); 
  }

  ~Mutex() 
  {
    pthread_mutex_destroy(&lock_);
    pthread_cond_destroy(&cond_);
  }

 private:
  pthread_mutex_t lock_;
  pthread_cond_t cond_;
};



}

#endif

