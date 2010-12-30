/*************************************************************************************************
 * Threading devices
 *                                                               Copyright (C) 2009-2010 FAL Labs
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#include "kcthread.h"
#include "myconf.h"

namespace kyotocabinet {                 // common namespace


/**
 * Thread internal.
 */
struct ThreadCore {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  ::HANDLE th;                           ///< handle
#else
  ::pthread_t th;                        ///< identifier
  bool alive;                            ///< alive flag
#endif
};


/**
 * CondVar internal.
 */
struct CondVarCore {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  ::CRITICAL_SECTION mutex;              ///< mutex
  uint32_t wait;                         ///< wait count
  uint32_t wake;                         ///< wake count
  ::HANDLE sev;                          ///< signal event handle
  ::HANDLE fev;                          ///< finish event handle
#else
  ::pthread_cond_t cond;                 ///< condition
#endif
};


/**
 * Call the running thread.
 * @param arg the thread.
 * @return always NULL.
 */
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
static ::DWORD threadrun(::LPVOID arg);
#else
static void* threadrun(void* arg);
#endif


/**
 * Default constructor.
 */
Thread::Thread() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ThreadCore* core = new ThreadCore;
  core->th = NULL;
  opq_ = (void*)core;
#else
  _assert_(true);
  ThreadCore* core = new ThreadCore;
  core->alive = false;
  opq_ = (void*)core;
#endif
}


/**
 * Destructor.
 */
Thread::~Thread() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (core->th) join();
  delete core;
#else
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (core->alive) join();
  delete core;
#endif
}


/**
 * Start the thread.
 */
void Thread::start() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (core->th) throw std::invalid_argument("already started");
  ::DWORD id;
  core->th = ::CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)threadrun, this, 0, &id);
  if (!core->th) throw std::runtime_error("CreateThread");
#else
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (core->alive) throw std::invalid_argument("already started");
  if (::pthread_create(&core->th, NULL, threadrun, this) != 0)
    throw std::runtime_error("pthread_create");
  core->alive = true;
#endif
}


/**
 * Wait for the thread to finish.
 */
void Thread::join() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (!core->th) throw std::invalid_argument("not alive");
  if (::WaitForSingleObject(core->th, INFINITE) == WAIT_FAILED)
    throw std::runtime_error("WaitForSingleObject");
  ::CloseHandle(core->th);
  core->th = NULL;
#else
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (!core->alive) throw std::invalid_argument("not alive");
  core->alive = false;
  if (::pthread_join(core->th, NULL) != 0) throw std::runtime_error("pthread_join");
#endif
}


/**
 * Put the thread in the detached state.
 */
void Thread::detach() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
#else
  _assert_(true);
  ThreadCore* core = (ThreadCore*)opq_;
  if (!core->alive) throw std::invalid_argument("not alive");
  core->alive = false;
  if (::pthread_detach(core->th) != 0) throw std::runtime_error("pthread_detach");
#endif
}


/**
 * Terminate the running thread.
 */
void Thread::exit() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::ExitThread(0);
#else
  _assert_(true);
  ::pthread_exit(NULL);
#endif
}


/**
 * Yield the processor from the current thread.
 */
void Thread::yield() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::Sleep(0);
#else
  _assert_(true);
  ::sched_yield();
#endif
}


/**
 * Suspend execution of the current thread.
 */
bool Thread::sleep(double sec) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(sec >= 0.0);
  if (sec <= 0.0) {
    yield();
    return true;
  }
  if (sec > INT32_MAX) sec = INT32_MAX;
  ::Sleep(sec * 1000);
  return true;
#else
  _assert_(sec >= 0.0);
  if (sec <= 0.0) {
    yield();
    return true;
  }
  if (sec > INT32_MAX) sec = INT32_MAX;
  double integ, fract;
  fract = std::modf(sec, &integ);
  struct ::timespec req, rem;
  req.tv_sec = (time_t)integ;
  req.tv_nsec = (long)(fract * 999999000);
  while (::nanosleep(&req, &rem) != 0) {
    if (errno != EINTR) return false;
    req = rem;
  }
  return true;
#endif
}


/**
 * Get the hash value of the current thread.
 */
int64_t Thread::hash() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  return ::GetCurrentThreadId();
#else
  _assert_(true);
  pthread_t tid = pthread_self();
  int64_t num;
  if (sizeof(tid) == sizeof(num)) {
    std::memcpy(&num, &tid, sizeof(num));
  } else if (sizeof(tid) == sizeof(int32_t)) {
    uint32_t inum;
    std::memcpy(&inum, &tid, sizeof(inum));
    num = inum;
  } else {
    num = hashmurmur(&tid, sizeof(tid));
  }
  return num & INT64_MAX;
#endif
}


/**
 * Call the running thread.
 */
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
static ::DWORD threadrun(::LPVOID arg) {
  _assert_(true);
  Thread* thread = (Thread*)arg;
  thread->run();
  return NULL;
}
#else
static void* threadrun(void* arg) {
  _assert_(true);
  Thread* thread = (Thread*)arg;
  thread->run();
  return NULL;
}
#endif


/**
 * Default constructor.
 */
Mutex::Mutex() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::CRITICAL_SECTION* mutex = new ::CRITICAL_SECTION;
  ::InitializeCriticalSection(mutex);
  opq_ = (void*)mutex;
#else
  _assert_(true);
  ::pthread_mutex_t* mutex = new ::pthread_mutex_t;
  if (::pthread_mutex_init(mutex, NULL) != 0) throw std::runtime_error("pthread_mutex_init");
  opq_ = (void*)mutex;
#endif
}


/**
 * Constructor with the specifications.
 */
Mutex::Mutex(Type type) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::CRITICAL_SECTION* mutex = new ::CRITICAL_SECTION;
  ::InitializeCriticalSection(mutex);
  opq_ = (void*)mutex;
#else
  _assert_(true);
  ::pthread_mutexattr_t attr;
  if (::pthread_mutexattr_init(&attr) != 0) throw std::runtime_error("pthread_mutexattr_init");
  switch (type) {
    case FAST: {
      break;
    }
    case ERRORCHECK: {
      if (::pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK) != 0)
        throw std::runtime_error("pthread_mutexattr_settype");
      break;
    }
    case RECURSIVE: {
      if (::pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) != 0)
        throw std::runtime_error("pthread_mutexattr_settype");
      break;
    }
  }
  ::pthread_mutex_t* mutex = new ::pthread_mutex_t;
  if (::pthread_mutex_init(mutex, &attr) != 0) throw std::runtime_error("pthread_mutex_init");
  ::pthread_mutexattr_destroy(&attr);
  opq_ = (void*)mutex;
#endif
}


/**
 * Destructor.
 */
Mutex::~Mutex() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::CRITICAL_SECTION* mutex = (::CRITICAL_SECTION*)opq_;
  ::DeleteCriticalSection(mutex);
  delete mutex;
#else
  _assert_(true);
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  ::pthread_mutex_destroy(mutex);
  delete mutex;
#endif
}


/**
 * Get the lock.
 */
void Mutex::lock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::CRITICAL_SECTION* mutex = (::CRITICAL_SECTION*)opq_;
  ::EnterCriticalSection(mutex);
#else
  _assert_(true);
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  if (::pthread_mutex_lock(mutex) != 0) throw std::runtime_error("pthread_mutex_lock");
#endif
}


/**
 * Try to get the lock.
 */
bool Mutex::lock_try() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::CRITICAL_SECTION* mutex = (::CRITICAL_SECTION*)opq_;
  if (!::TryEnterCriticalSection(mutex)) return false;
  return true;
#else
  _assert_(true);
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  int32_t ecode = ::pthread_mutex_trylock(mutex);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_mutex_trylock");
  return false;
#endif
}


/**
 * Try to get the lock.
 */
bool Mutex::lock_try(double sec) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_) || defined(_SYS_MACOSX_)
  _assert_(sec >= 0.0);
  if (lock_try()) return true;
  double end = time() + sec;
  Thread::yield();
  while (!lock_try()) {
    if (time() > end) return false;
    Thread::yield();
  }
  return true;
#else
  _assert_(sec >= 0.0);
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  struct ::timeval tv;
  struct ::timespec ts;
  if (::gettimeofday(&tv, NULL) == 0) {
    double integ;
    double fract = std::modf(sec, &integ);
    ts.tv_sec = tv.tv_sec + (time_t)integ;
    ts.tv_nsec = (long)(tv.tv_usec * 1000.0 + fract * 999999000);
    if (ts.tv_nsec >= 1000000000) {
      ts.tv_nsec -= 1000000000;
      ts.tv_sec++;
    }
  } else {
    ts.tv_sec = std::time(NULL) + 1;
    ts.tv_nsec = 0;
  }
  int32_t ecode = ::pthread_mutex_timedlock(mutex, &ts);
  if (ecode == 0) return true;
  if (ecode != ETIMEDOUT) throw std::runtime_error("pthread_mutex_timedlock");
  return false;
#endif
}


/**
 * Release the lock.
 */
void Mutex::unlock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::CRITICAL_SECTION* mutex = (::CRITICAL_SECTION*)opq_;
  ::LeaveCriticalSection(mutex);
#else
  _assert_(true);
  ::pthread_mutex_t* mutex = (::pthread_mutex_t*)opq_;
  if (::pthread_mutex_unlock(mutex) != 0) throw std::runtime_error("pthread_mutex_unlock");
#endif
}


/**
 * Default constructor.
 */
SpinLock::SpinLock() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
#elif _KC_GCCATOMIC
  _assert_(true);
#else
  _assert_(true);
  ::pthread_spinlock_t* spin = new ::pthread_spinlock_t;
  if (::pthread_spin_init(spin, PTHREAD_PROCESS_PRIVATE) != 0)
    throw std::runtime_error("pthread_spin_init");
  opq_ = (void*)spin;
#endif
}


/**
 * Destructor.
 */
SpinLock::~SpinLock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
#elif _KC_GCCATOMIC
  _assert_(true);
#else
  _assert_(true);
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  ::pthread_spin_destroy(spin);
  delete spin;
#endif
}


/**
 * Get the lock.
 */
void SpinLock::lock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  while (::InterlockedCompareExchange((LONG*)&opq_, 1, 0) != 0) {
    ::Sleep(0);
  }
#elif _KC_GCCATOMIC
  _assert_(true);
  while (!__sync_bool_compare_and_swap(&opq_, 0, 1)) {
    ::sched_yield();
  }
#else
  _assert_(true);
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  if (::pthread_spin_lock(spin) != 0) throw std::runtime_error("pthread_spin_lock");
#endif
}


/**
 * Try to get the lock.
 */
bool SpinLock::lock_try() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  return ::InterlockedCompareExchange((LONG*)&opq_, 1, 0) == 0;
#elif _KC_GCCATOMIC
  _assert_(true);
  return __sync_bool_compare_and_swap(&opq_, 0, 1);
#else
  _assert_(true);
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  int32_t ecode = ::pthread_spin_trylock(spin);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_spin_trylock");
  return false;
#endif
}


/**
 * Release the lock.
 */
void SpinLock::unlock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::InterlockedExchange((LONG*)&opq_, 0);
#elif _KC_GCCATOMIC
  _assert_(true);
  (void)__sync_lock_test_and_set(&opq_, 0);
#else
  _assert_(true);
  ::pthread_spinlock_t* spin = (::pthread_spinlock_t*)opq_;
  if (::pthread_spin_unlock(spin) != 0) throw std::runtime_error("pthread_spin_unlock");
#endif
}


/**
 * Default constructor.
 */
RWLock::RWLock() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = new SpinRWLock();
  opq_ = (void*)rwlock;
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = new ::pthread_rwlock_t;
  if (::pthread_rwlock_init(rwlock, NULL) != 0) throw std::runtime_error("pthread_rwlock_init");
  opq_ = (void*)rwlock;
#endif
}


/**
 * Destructor.
 */
RWLock::~RWLock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = (SpinRWLock*)opq_;
  delete rwlock;
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  ::pthread_rwlock_destroy(rwlock);
  delete rwlock;
#endif
}


/**
 * Get the writer lock.
 */
void RWLock::lock_writer() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = (SpinRWLock*)opq_;
  rwlock->lock_writer();
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  if (::pthread_rwlock_wrlock(rwlock) != 0) throw std::runtime_error("pthread_rwlock_lock");
#endif
}


/**
 * Try to get the writer lock.
 */
bool RWLock::lock_writer_try() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = (SpinRWLock*)opq_;
  return rwlock->lock_writer_try();
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  int32_t ecode = ::pthread_rwlock_trywrlock(rwlock);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_rwlock_trylock");
  return false;
#endif
}


/**
 * Get a reader lock.
 */
void RWLock::lock_reader() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = (SpinRWLock*)opq_;
  rwlock->lock_reader();
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  if (::pthread_rwlock_rdlock(rwlock) != 0) throw std::runtime_error("pthread_rwlock_lock");
#endif
}


/**
 * Try to get a reader lock.
 */
bool RWLock::lock_reader_try() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = (SpinRWLock*)opq_;
  return rwlock->lock_reader_try();
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  int32_t ecode = ::pthread_rwlock_tryrdlock(rwlock);
  if (ecode == 0) return true;
  if (ecode != EBUSY) throw std::runtime_error("pthread_rwlock_trylock");
  return false;
#endif
}


/**
 * Release the lock.
 */
void RWLock::unlock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  SpinRWLock* rwlock = (SpinRWLock*)opq_;
  rwlock->unlock();
#else
  _assert_(true);
  ::pthread_rwlock_t* rwlock = (::pthread_rwlock_t*)opq_;
  if (::pthread_rwlock_unlock(rwlock) != 0) throw std::runtime_error("pthread_rwlock_unlock");
#endif
}


/**
 * SpinRWLock internal.
 */
struct SpinRWLockCore {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  LONG sem;                              ///< semaphore
  int32_t wc;                            ///< count of writers
  int32_t rc;                            ///< count of readers
#elif _KC_GCCATOMIC
  int32_t sem;                           ///< semaphore
  int32_t wc;                            ///< count of writers
  int32_t rc;                            ///< count of readers
#else
  ::pthread_spinlock_t sem;              ///< semaphore
  int32_t wc;                            ///< count of writers
  int32_t rc;                            ///< count of readers
#endif
};


/**
 * Lock the semephore of SpinRWLock.
 * @param core the internal fields.
 */
static void spinrwlocklock(SpinRWLockCore* core);


/**
 * Unlock the semephore of SpinRWLock.
 * @param core the internal fields.
 */
static void spinrwlockunlock(SpinRWLockCore* core);


/**
 * Default constructor.
 */
SpinRWLock::SpinRWLock() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_) || _KC_GCCATOMIC
  _assert_(true);
  SpinRWLockCore* core = new SpinRWLockCore;
  core->sem = 0;
  core->wc = 0;
  core->rc = 0;
  opq_ = (void*)core;
#else
  _assert_(true);
  SpinRWLockCore* core = new SpinRWLockCore;
  if (::pthread_spin_init(&core->sem, PTHREAD_PROCESS_PRIVATE) != 0)
    throw std::runtime_error("pthread_spin_init");
  core->wc = 0;
  core->rc = 0;
  opq_ = (void*)core;
#endif
}


/**
 * Destructor.
 */
SpinRWLock::~SpinRWLock() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_) || _KC_GCCATOMIC
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  delete core;
#else
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  ::pthread_spin_destroy(&core->sem);
  delete core;
#endif
}


/**
 * Get the writer lock.
 */
void SpinRWLock::lock_writer() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  while (core->wc > 0 || core->rc > 0) {
    spinrwlockunlock(core);
    Thread::yield();
    spinrwlocklock(core);
  }
  core->wc++;
  spinrwlockunlock(core);
}


/**
 * Try to get the writer lock.
 */
bool SpinRWLock::lock_writer_try() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->wc > 0 || core->rc > 0) {
    spinrwlockunlock(core);
    return false;
  }
  spinrwlockunlock(core);
  return true;
}


/**
 * Get a reader lock.
 */
void SpinRWLock::lock_reader() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  while (core->wc > 0) {
    spinrwlockunlock(core);
    Thread::yield();
    spinrwlocklock(core);
  }
  core->rc++;
  spinrwlockunlock(core);
}


/**
 * Try to get a reader lock.
 */
bool SpinRWLock::lock_reader_try() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->wc > 0) {
    spinrwlockunlock(core);
    return false;
  }
  core->rc++;
  spinrwlockunlock(core);
  return true;
}


/**
 * Release the lock.
 */
void SpinRWLock::unlock() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->wc > 0) {
    core->wc--;
  } else {
    core->rc--;
  }
  spinrwlockunlock(core);
}


/**
 * Promote a reader lock to the writer lock.
 */
bool SpinRWLock::promote() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  if (core->rc > 1) {
    spinrwlockunlock(core);
    return false;
  }
  core->rc--;
  core->wc++;
  spinrwlockunlock(core);
  return true;
}


/**
 * Demote the writer lock to a reader lock.
 */
void SpinRWLock::demote() {
  _assert_(true);
  SpinRWLockCore* core = (SpinRWLockCore*)opq_;
  spinrwlocklock(core);
  core->wc--;
  core->rc++;
  spinrwlockunlock(core);
}


/**
 * Lock the semephore of SpinRWLock.
 */
static void spinrwlocklock(SpinRWLockCore* core) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(core);
  while (::InterlockedCompareExchange(&core->sem, 1, 0) != 0) {
    ::Sleep(0);
  }
#elif _KC_GCCATOMIC
  _assert_(core);
  while (!__sync_bool_compare_and_swap(&core->sem, 0, 1)) {
    ::sched_yield();
  }
#else
  _assert_(core);
  if (::pthread_spin_lock(&core->sem) != 0) throw std::runtime_error("pthread_spin_lock");
#endif
}


/**
 * Unlock the semephore of SpinRWLock.
 */
static void spinrwlockunlock(SpinRWLockCore* core) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(core);
  ::InterlockedExchange(&core->sem, 0);
#elif _KC_GCCATOMIC
  _assert_(core);
  (void)__sync_lock_test_and_set(&core->sem, 0);
#else
  _assert_(core);
  if (::pthread_spin_unlock(&core->sem) != 0) throw std::runtime_error("pthread_spin_unlock");
#endif
}


/**
 * Default constructor.
 */
CondVar::CondVar() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  CondVarCore* core = new CondVarCore;
  ::InitializeCriticalSection(&core->mutex);
  core->wait = 0;
  core->wake = 0;
  core->sev = ::CreateEvent(NULL, true, false, NULL);
  if (!core->sev) throw std::runtime_error("CreateEvent");
  core->fev = ::CreateEvent(NULL, false, false, NULL);
  if (!core->fev) throw std::runtime_error("CreateEvent");
  opq_ = (void*)core;
#else
  _assert_(true);
  CondVarCore* core = new CondVarCore;
  if (::pthread_cond_init(&core->cond, NULL) != 0) throw std::runtime_error("pthread_cond_init");
  opq_ = (void*)core;
#endif
}


/**
 * Destructor.
 */
CondVar::~CondVar() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  CondVarCore* core = (CondVarCore*)opq_;
  ::CloseHandle(core->fev);
  ::CloseHandle(core->sev);
  ::DeleteCriticalSection(&core->mutex);
#else
  _assert_(true);
  CondVarCore* core = (CondVarCore*)opq_;
  ::pthread_cond_destroy(&core->cond);
  delete core;
#endif
}


/**
 * Wait for the signal.
 */
void CondVar::wait(Mutex* mutex) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(mutex);
  CondVarCore* core = (CondVarCore*)opq_;
  ::CRITICAL_SECTION* mymutex = (::CRITICAL_SECTION*)mutex->opq_;
  core->wait++;
  ::LeaveCriticalSection(mymutex);
  while (true) {
    ::WaitForSingleObject(core->sev, INFINITE);
    ::EnterCriticalSection(&core->mutex);
    if (core->wake > 0) {
      core->wait--;
      core->wake--;
      if (core->wake < 1) {
        ::ResetEvent(core->sev);
        ::SetEvent(core->fev);
      }
      ::LeaveCriticalSection(&core->mutex);
      break;
    }
    ::LeaveCriticalSection(&core->mutex);
  }
  ::EnterCriticalSection(mymutex);
#else
  _assert_(mutex);
  CondVarCore* core = (CondVarCore*)opq_;
  ::pthread_mutex_t* mymutex = (::pthread_mutex_t*)mutex->opq_;
  if (::pthread_cond_wait(&core->cond, mymutex) != 0)
    throw std::runtime_error("pthread_cond_wait");
#endif
}


/**
 * Wait for the signal.
 */
bool CondVar::wait(Mutex* mutex, double sec) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(mutex && sec >= 0);
  if (sec <= 0) return false;
  CondVarCore* core = (CondVarCore*)opq_;
  ::CRITICAL_SECTION* mymutex = (::CRITICAL_SECTION*)mutex->opq_;
  core->wait++;
  ::LeaveCriticalSection(mymutex);
  while (true) {
    if (::WaitForSingleObject(core->sev, sec * 1000) == WAIT_TIMEOUT) {
      ::EnterCriticalSection(mymutex);
      core->wait--;
      return false;
    }
    ::EnterCriticalSection(&core->mutex);
    if (core->wake > 0) {
      core->wait--;
      core->wake--;
      if (core->wake < 1) {
        ::ResetEvent(core->sev);
        ::SetEvent(core->fev);
      }
      ::LeaveCriticalSection(&core->mutex);
      break;
    }
    ::LeaveCriticalSection(&core->mutex);
  }
  ::EnterCriticalSection(mymutex);
  return true;
#else
  _assert_(mutex && sec >= 0);
  if (sec <= 0) return false;
  CondVarCore* core = (CondVarCore*)opq_;
  ::pthread_mutex_t* mymutex = (::pthread_mutex_t*)mutex->opq_;
  struct ::timeval tv;
  struct ::timespec ts;
  if (::gettimeofday(&tv, NULL) == 0) {
    double integ;
    double fract = std::modf(sec, &integ);
    ts.tv_sec = tv.tv_sec + (time_t)integ;
    ts.tv_nsec = (long)(tv.tv_usec * 1000.0 + fract * 999999000);
    if (ts.tv_nsec >= 1000000000) {
      ts.tv_nsec -= 1000000000;
      ts.tv_sec++;
    }
  } else {
    ts.tv_sec = std::time(NULL) + 1;
    ts.tv_nsec = 0;
  }
  int32_t ecode = ::pthread_cond_timedwait(&core->cond, mymutex, &ts);
  if (ecode == 0) return true;
  if (ecode != ETIMEDOUT && ecode != EINTR) throw std::runtime_error("pthread_cond_timedwait");
  return false;
#endif
}


/**
 * Send the wake-up signal to another waiting thread.
 */
void CondVar::signal() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  CondVarCore* core = (CondVarCore*)opq_;
  if (core->wait > 0) {
    core->wake = 1;
    ::SetEvent(core->sev);
    ::WaitForSingleObject(core->fev, INFINITE);
  }
#else
  _assert_(true);
  CondVarCore* core = (CondVarCore*)opq_;
  if (::pthread_cond_signal(&core->cond) != 0)
    throw std::runtime_error("pthread_cond_signal");
#endif
}


/**
 * Send the wake-up signal to all waiting threads.
 */
void CondVar::broadcast() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  CondVarCore* core = (CondVarCore*)opq_;
  if (core->wait > 0) {
    core->wake = core->wait;
    ::SetEvent(core->sev);
    ::WaitForSingleObject(core->fev, INFINITE);
  }
#else
  _assert_(true);
  CondVarCore* core = (CondVarCore*)opq_;
  if (::pthread_cond_broadcast(&core->cond) != 0)
    throw std::runtime_error("pthread_cond_broadcast");
#endif
}


/**
 * Default constructor.
 */
TSDKey::TSDKey() : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::DWORD key = ::TlsAlloc();
  if (key == 0xFFFFFFFF) throw std::runtime_error("TlsAlloc");
  opq_ = (void*)key;
#else
  _assert_(true);
  ::pthread_key_t* key = new ::pthread_key_t;
  if (::pthread_key_create(key, NULL) != 0) throw std::runtime_error("pthread_key_create");
  opq_ = (void*)key;
#endif
}


/**
 * Constructor with the specifications.
 */
TSDKey::TSDKey(void (*dstr)(void*)) : opq_(NULL) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::DWORD key = ::TlsAlloc();
  if (key == 0xFFFFFFFF) throw std::runtime_error("TlsAlloc");
  opq_ = (void*)key;
#else
  _assert_(true);
  ::pthread_key_t* key = new ::pthread_key_t;
  if (::pthread_key_create(key, dstr) != 0) throw std::runtime_error("pthread_key_create");
  opq_ = (void*)key;
#endif
}


/**
 * Destructor.
 */
TSDKey::~TSDKey() {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::DWORD key = (::DWORD)opq_;
  ::TlsFree(key);
#else
  _assert_(true);
  ::pthread_key_t* key = (::pthread_key_t*)opq_;
  ::pthread_key_delete(*key);
  delete key;
#endif
}


/**
 * Set the value.
 */
void TSDKey::set(void* ptr) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::DWORD key = (::DWORD)opq_;
  if (!::TlsSetValue(key, ptr)) std::runtime_error("TlsSetValue");
#else
  _assert_(true);
  ::pthread_key_t* key = (::pthread_key_t*)opq_;
  if (::pthread_setspecific(*key, ptr) != 0) throw std::runtime_error("pthread_setspecific");
#endif
}


/**
 * Get the value.
 */
void* TSDKey::get() const {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(true);
  ::DWORD key = (::DWORD)opq_;
  return ::TlsGetValue(key);
#else
  _assert_(true);
  ::pthread_key_t* key = (::pthread_key_t*)opq_;
  return ::pthread_getspecific(*key);
#endif
}


/**
 * Set the new value.
 */
int64_t AtomicInt64::set(int64_t val) {
#if (defined(_SYS_MSVC_) || defined(_SYS_MINGW_)) && defined(_SYS_WIN64_)
  _assert_(true);
  return ::InterlockedExchange((uint64_t*)&value_, val);
#elif _KC_GCCATOMIC
  _assert_(true);
  int64_t oval = __sync_lock_test_and_set(&value_, val);
  __sync_synchronize();
  return oval;
#else
  _assert_(true);
  lock_.lock();
  int64_t oval = value_;
  value_ = val;
  lock_.unlock();
  return oval;
#endif
}


/**
 * Add a value.
 */
int64_t AtomicInt64::add(int64_t val) {
#if (defined(_SYS_MSVC_) || defined(_SYS_MINGW_)) && defined(_SYS_WIN64_)
  _assert_(true);
  return ::InterlockedExchangeAdd((uint64_t*)&value_, val);
#elif _KC_GCCATOMIC
  _assert_(true);
  int64_t oval = __sync_fetch_and_add(&value_, val);
  __sync_synchronize();
  return oval;
#else
  _assert_(true);
  lock_.lock();
  int64_t oval = value_;
  value_ += val;
  lock_.unlock();
  return oval;
#endif
}


/**
 * Perform compare-and-swap.
 */
bool AtomicInt64::cas(int64_t oval, int64_t nval) {
#if (defined(_SYS_MSVC_) || defined(_SYS_MINGW_)) && defined(_SYS_WIN64_)
  _assert_(true);
  return ::InterlockedCompareExchange((uint64_t*)&value_, nval, oval) == nval;
#elif _KC_GCCATOMIC
  _assert_(true);
  bool rv = __sync_bool_compare_and_swap(&value_, oval, nval);
  __sync_synchronize();
  return rv;
#else
  _assert_(true);
  bool rv = false;
  lock_.lock();
  if (value_ == oval) {
    value_ = nval;
    rv = true;
  }
  lock_.unlock();
  return rv;
#endif
}


/**
 * Get the current value.
 */
int64_t AtomicInt64::get() const {
#if (defined(_SYS_MSVC_) || defined(_SYS_MINGW_)) && defined(_SYS_WIN64_)
  _assert_(true);
  return ::InterlockedExchangeAdd((uint64_t*)&value_, 0);
#elif _KC_GCCATOMIC
  _assert_(true);
  return __sync_fetch_and_add((int64_t*)&value_, 0);
#else
  _assert_(true);
  lock_.lock();
  int64_t oval = value_;
  lock_.unlock();
  return oval;
#endif
}


}                                        // common namespace

// END OF FILE
