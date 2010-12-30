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


#ifndef _KCTHREAD_H                      // duplication check
#define _KCTHREAD_H

#include <kccommon.h>
#include <kcutil.h>

namespace kyotocabinet {                 // common namespace


/**
 * Threading device.
 */
class Thread {
public:
  /**
   * Default constructor.
   */
  explicit Thread();
  /**
   * Destructor.
   */
  virtual ~Thread();
  /**
   * Perform the concrete process.
   */
  virtual void run() = 0;
  /**
   * Start the thread.
   */
  void start();
  /**
   * Wait for the thread to finish.
   */
  void join();
  /**
   * Put the thread in the detached state.
   */
  void detach();
  /**
   * Terminate the running thread.
   */
  static void exit();
  /**
   * Yield the processor from the current thread.
   */
  static void yield();
  /**
   * Suspend execution of the current thread.
   * @param sec the interval of the suspension in seconds.
   * @return true on success, or false on failure.
   */
  static bool sleep(double sec);
  /**
   * Get the hash value of the current thread.
   * @return the hash value of the current thread.
   */
  static int64_t hash();
private:
  /** Dummy constructor to forbid the use. */
  Thread(const Thread&);
  /** Dummy Operator to forbid the use. */
  Thread& operator =(const Thread&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Basic mutual exclusion device.
 */
class Mutex {
  friend class CondVar;
public:
  /**
   * Type of the behavior for double locking.
   */
  enum Type {
    FAST,                ///< no operation
    ERRORCHECK,          ///< check error
    RECURSIVE            ///< allow recursive locking
  };
  /**
   * Default constructor.
   */
  explicit Mutex();
  /**
   * Constructor.
   * @param type the behavior for double locking.
   */
  explicit Mutex(Type type);
  /**
   * Destructor.
   */
  ~Mutex();
  /**
   * Get the lock.
   */
  void lock();
  /**
   * Try to get the lock.
   * @return true on success, or false on failure.
   */
  bool lock_try();
  /**
   * Try to get the lock.
   * @param sec the interval of the suspension in seconds.
   * @return true on success, or false on failure.
   */
  bool lock_try(double sec);
  /**
   * Release the lock.
   */
  void unlock();
private:
  /** Dummy constructor to forbid the use. */
  Mutex(const Mutex&);
  /** Dummy Operator to forbid the use. */
  Mutex& operator =(const Mutex&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Scoped mutex device.
 */
class ScopedMutex {
public:
  /**
   * Constructor.
   * @param mutex a mutex to lock the block.
   */
  explicit ScopedMutex(Mutex* mutex) : mutex_(mutex) {
    _assert_(mutex);
    mutex_->lock();
  }
  /**
   * Destructor.
   */
  ~ScopedMutex() {
    _assert_(true);
    mutex_->unlock();
  }
private:
  /** Dummy constructor to forbid the use. */
  ScopedMutex(const ScopedMutex&);
  /** Dummy Operator to forbid the use. */
  ScopedMutex& operator =(const ScopedMutex&);
  /** The inner device. */
  Mutex* mutex_;
};


/**
 * Slotted mutex device.
 * @param SLOTNUM the number of slots.
 */
template <int32_t SLOTNUM>
class SlottedMutex {
public:
  /**
   * Constructor.
   */
  explicit SlottedMutex() : locks_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~SlottedMutex() {
    _assert_(true);
  }
  /**
   * Get the lock of a slot.
   * @param idx the index of a slot.
   */
  void lock(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].lock();
  }
  /**
   * Release the lock of a slot.
   * @param idx the index of a slot.
   */
  void unlock(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].unlock();
  }
  /**
   * Get the locks of all slots.
   */
  void lock_all() {
    _assert_(true);
    for (int32_t i = 0; i < SLOTNUM; i++) {
      locks_[i].lock();
    }
  }
  /**
   * Release the locks of all slots.
   */
  void unlock_all() {
    _assert_(true);
    for (int32_t i = SLOTNUM - 1; i >= 0; i--) {
      locks_[i].unlock();
    }
  }
private:
  /** The inner devices. */
  Mutex locks_[SLOTNUM];
};


/**
 * Lightweight mutual exclusion device.
 */
class SpinLock {
public:
  /**
   * Default constructor.
   */
  explicit SpinLock();
  /**
   * Destructor.
   */
  ~SpinLock();
  /**
   * Get the lock.
   */
  void lock();
  /**
   * Try to get the lock.
   * @return true on success, or false on failure.
   */
  bool lock_try();
  /**
   * Release the lock.
   */
  void unlock();
private:
  /** Dummy constructor to forbid the use. */
  SpinLock(const SpinLock&);
  /** Dummy Operator to forbid the use. */
  SpinLock& operator =(const SpinLock&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Scoped spin lock device.
 */
class ScopedSpinLock {
public:
  /**
   * Constructor.
   * @param spinlock a spin lock to lock the block.
   */
  explicit ScopedSpinLock(SpinLock* spinlock) : spinlock_(spinlock) {
    _assert_(spinlock);
    spinlock_->lock();
  }
  /**
   * Destructor.
   */
  ~ScopedSpinLock() {
    _assert_(true);
    spinlock_->unlock();
  }
private:
  /** Dummy constructor to forbid the use. */
  ScopedSpinLock(const ScopedSpinLock&);
  /** Dummy Operator to forbid the use. */
  ScopedSpinLock& operator =(const ScopedSpinLock&);
  /** The inner device. */
  SpinLock* spinlock_;
};


/**
 * Slotted spin lock devices.
 * @param SLOTNUM the number of slots.
 */
template <int32_t SLOTNUM>
class SlottedSpinLock {
public:
  /**
   * Constructor.
   */
  explicit SlottedSpinLock() : locks_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~SlottedSpinLock() {
    _assert_(true);
  }
  /**
   * Get the lock of a slot.
   * @param idx the index of a slot.
   */
  void lock(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].lock();
  }
  /**
   * Release the lock of a slot.
   * @param idx the index of a slot.
   */
  void unlock(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].unlock();
  }
  /**
   * Get the locks of all slots.
   */
  void lock_all() {
    _assert_(true);
    for (int32_t i = 0; i < SLOTNUM; i++) {
      locks_[i].lock();
    }
  }
  /**
   * Release the locks of all slots.
   */
  void unlock_all() {
    _assert_(true);
    for (int32_t i = SLOTNUM - 1; i >= 0; i--) {
      locks_[i].unlock();
    }
  }
private:
  /** The inner devices. */
  SpinLock locks_[SLOTNUM];
};


/**
 * Reader-writer locking device.
 */
class RWLock {
public:
  /**
   * Default constructor.
   */
  explicit RWLock();
  /**
   * Destructor.
   */
  ~RWLock();
  /**
   * Get the writer lock.
   */
  void lock_writer();
  /**
   * Try to get the writer lock.
   * @return true on success, or false on failure.
   */
  bool lock_writer_try();
  /**
   * Get a reader lock.
   */
  void lock_reader();
  /**
   * Try to get a reader lock.
   * @return true on success, or false on failure.
   */
  bool lock_reader_try();
  /**
   * Release the lock.
   */
  void unlock();
private:
  /** Dummy constructor to forbid the use. */
  RWLock(const RWLock&);
  /** Dummy Operator to forbid the use. */
  RWLock& operator =(const RWLock&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Scoped reader-writer locking device.
 */
class ScopedRWLock {
public:
  /**
   * Constructor.
   * @param rwlock a rwlock to lock the block.
   * @param writer true for writer lock, or false for reader lock.
   */
  explicit ScopedRWLock(RWLock* rwlock, bool writer) : rwlock_(rwlock) {
    _assert_(rwlock);
    if (writer) {
      rwlock_->lock_writer();
    } else {
      rwlock_->lock_reader();
    }
  }
  /**
   * Destructor.
   */
  ~ScopedRWLock() {
    _assert_(true);
    rwlock_->unlock();
  }
private:
  /** Dummy constructor to forbid the use. */
  ScopedRWLock(const ScopedRWLock&);
  /** Dummy Operator to forbid the use. */
  ScopedRWLock& operator =(const ScopedRWLock&);
  /** The inner device. */
  RWLock* rwlock_;
};


/**
 * Slotted reader-writer lock devices.
 * @param SLOTNUM the number of slots.
 */
template <int32_t SLOTNUM>
class SlottedRWLock {
public:
  /**
   * Constructor.
   */
  explicit SlottedRWLock() : locks_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~SlottedRWLock() {
    _assert_(true);
  }
  /**
   * Get the writer lock of a slot.
   * @param idx the index of a slot.
   */
  void lock_writer(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].lock_writer();
  }
  /**
   * Get the reader lock of a slot.
   * @param idx the index of a slot.
   */
  void lock_reader(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].lock_reader();
  }
  /**
   * Release the lock of a slot.
   * @param idx the index of a slot.
   */
  void unlock(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].unlock();
  }
  /**
   * Get the writer locks of all slots.
   */
  void lock_writer_all() {
    _assert_(true);
    for (int32_t i = 0; i < SLOTNUM; i++) {
      locks_[i].lock_writer();
    }
  }
  /**
   * Get the reader locks of all slots.
   */
  void lock_reader_all() {
    _assert_(true);
    for (int32_t i = 0; i < SLOTNUM; i++) {
      locks_[i].lock_reader();
    }
  }
  /**
   * Release the locks of all slots.
   */
  void unlock_all() {
    _assert_(true);
    for (int32_t i = SLOTNUM - 1; i >= 0; i--) {
      locks_[i].unlock();
    }
  }
private:
  /** The inner devices. */
  RWLock locks_[SLOTNUM];
};


/**
 * Lightweight reader-writer locking device.
 */
class SpinRWLock {
public:
  /**
   * Default constructor.
   */
  explicit SpinRWLock();
  /**
   * Destructor.
   */
  ~SpinRWLock();
  /**
   * Get the writer lock.
   */
  void lock_writer();
  /**
   * Try to get the writer lock.
   * @return true on success, or false on failure.
   */
  bool lock_writer_try();
  /**
   * Get a reader lock.
   */
  void lock_reader();
  /**
   * Try to get a reader lock.
   * @return true on success, or false on failure.
   */
  bool lock_reader_try();
  /**
   * Release the lock.
   */
  void unlock();
  /**
   * Promote a reader lock to the writer lock.
   * @return true on success, or false on failure.
   */
  bool promote();
  /**
   * Demote the writer lock to a reader lock.
   */
  void demote();
private:
  /** Dummy constructor to forbid the use. */
  SpinRWLock(const SpinRWLock&);
  /** Dummy Operator to forbid the use. */
  SpinRWLock& operator =(const SpinRWLock&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Scoped reader-writer locking device.
 */
class ScopedSpinRWLock {
public:
  /**
   * Constructor.
   * @param srwlock a spin rwlock to lock the block.
   * @param writer true for writer lock, or false for reader lock.
   */
  explicit ScopedSpinRWLock(SpinRWLock* srwlock, bool writer) : srwlock_(srwlock) {
    _assert_(srwlock);
    if (writer) {
      srwlock_->lock_writer();
    } else {
      srwlock_->lock_reader();
    }
  }
  /**
   * Destructor.
   */
  ~ScopedSpinRWLock() {
    _assert_(true);
    srwlock_->unlock();
  }
private:
  /** Dummy constructor to forbid the use. */
  ScopedSpinRWLock(const ScopedSpinRWLock&);
  /** Dummy Operator to forbid the use. */
  ScopedSpinRWLock& operator =(const ScopedSpinRWLock&);
  /** The inner device. */
  SpinRWLock* srwlock_;
};


/**
 * Slotted lightweight reader-writer lock devices.
 * @param SLOTNUM the number of slots.
 */
template <int32_t SLOTNUM>
class SlottedSpinRWLock {
public:
  /**
   * Constructor.
   */
  explicit SlottedSpinRWLock() : locks_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~SlottedSpinRWLock() {
    _assert_(true);
  }
  /**
   * Get the writer lock of a slot.
   * @param idx the index of a slot.
   */
  void lock_writer(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].lock_writer();
  }
  /**
   * Get the reader lock of a slot.
   * @param idx the index of a slot.
   */
  void lock_reader(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].lock_reader();
  }
  /**
   * Release the lock of a slot.
   * @param idx the index of a slot.
   */
  void unlock(int32_t idx) {
    _assert_(idx >= 0);
    locks_[idx].unlock();
  }
  /**
   * Get the writer locks of all slots.
   */
  void lock_writer_all() {
    _assert_(true);
    for (int32_t i = 0; i < SLOTNUM; i++) {
      locks_[i].lock_writer();
    }
  }
  /**
   * Get the reader locks of all slots.
   */
  void lock_reader_all() {
    _assert_(true);
    for (int32_t i = 0; i < SLOTNUM; i++) {
      locks_[i].lock_reader();
    }
  }
  /**
   * Release the locks of all slots.
   */
  void unlock_all() {
    _assert_(true);
    for (int32_t i = SLOTNUM - 1; i >= 0; i--) {
      locks_[i].unlock();
    }
  }
private:
  /** The inner devices. */
  SpinRWLock locks_[SLOTNUM];
};


/**
 * Condition variable.
 */
class CondVar {
public:
  /**
   * Default constructor.
   */
  explicit CondVar();
  /**
   * Destructor.
   */
  ~CondVar();
  /**
   * Wait for the signal.
   * @param mutex a locked mutex.
   */
  void wait(Mutex* mutex);
  /**
   * Wait for the signal.
   * @param mutex a locked mutex.
   * @param sec the interval of the suspension in seconds.
   * @return true on catched signal, or false on timeout.
   */
  bool wait(Mutex* mutex, double sec);
  /**
   * Send the wake-up signal to another waiting thread.
   * @note The mutex used for the wait method should be locked by the caller.
   */
  void signal();
  /**
   * Send the wake-up signal to all waiting threads.
   * @note The mutex used for the wait method should be locked by the caller.
   */
  void broadcast();
private:
  /** Dummy constructor to forbid the use. */
  CondVar(const CondVar&);
  /** Dummy Operator to forbid the use. */
  CondVar& operator =(const CondVar&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Key of thread specific data.
 */
class TSDKey {
public:
  /**
   * Default constructor.
   */
  explicit TSDKey();
  /**
   * Constructor.
   * @param dstr the destructor for the value.
   */
  explicit TSDKey(void (*dstr)(void*));
  /**
   * Destructor.
   */
  ~TSDKey();
  /**
   * Set the value.
   * @param ptr an arbitrary pointer.
   */
  void set(void* ptr);
  /**
   * Get the value.
   * @return the value.
   */
  void* get() const ;
private:
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Smart pointer to thread specific data.
 */
template <class TYPE>
class TSD {
public:
  /**
   * Default constructor.
   */
  explicit TSD() : key_(delete_value) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~TSD() {
    _assert_(true);
    TYPE* obj = (TYPE*)key_.get();
    if (obj) {
      delete obj;
      key_.set(NULL);
    }
  }
  /**
   * Dereference operator.
   * @return the reference to the inner object.
   */
  TYPE& operator *() {
    _assert_(true);
    TYPE* obj = (TYPE*)key_.get();
    if (!obj) {
      obj = new TYPE;
      key_.set(obj);
    }
    return *obj;
  }
  /**
   * Member reference operator.
   * @return the pointer to the inner object.
   */
  TYPE* operator ->() {
    _assert_(true);
    TYPE* obj = (TYPE*)key_.get();
    if (!obj) {
      obj = new TYPE;
      key_.set(obj);
    }
    return obj;
  }
  /**
   * Cast operator to the original type.
   * @return the copy of the inner object.
   */
  operator TYPE() const {
    _assert_(true);
    TYPE* obj = (TYPE*)key_.get();
    if (!obj) return TYPE();
    return *obj;
  }
private:
  /**
   * Delete the inner object.
   * @param obj the inner object.
   */
  static void delete_value(void* obj) {
    _assert_(true);
    delete (TYPE*)obj;
  }
  /** Dummy constructor to forbid the use. */
  TSD(const TSD&);
  /** Dummy Operator to forbid the use. */
  TSD& operator =(const TSD&);
  /** Key of thread specific data. */
  TSDKey key_;
};


/**
 * Integer with atomic operations.
 */
class AtomicInt64 {
public:
  /**
   * Default constructor.
   */
  explicit AtomicInt64() : value_(0), lock_() {
    _assert_(true);
  }
  /**
   * Copy constructor.
   * @param src the source object.
   */
  AtomicInt64(const AtomicInt64& src) : value_(src.get()), lock_() {
    _assert_(true);
  };
  /**
   * Constructor.
   * @param num the initial value.
   */
  AtomicInt64(int64_t num) : value_(num), lock_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~AtomicInt64() {
    _assert_(true);
  }
  /**
   * Set the new value.
   * @param val the new value.
   * @return the old value.
   */
  int64_t set(int64_t val);
  /**
   * Add a value.
   * @param val the additional value.
   * @return the old value.
   */
  int64_t add(int64_t val);
  /**
   * Perform compare-and-swap.
   * @param oval the old value.
   * @param nval the new value.
   * @return true on success, or false on failure.
   */
  bool cas(int64_t oval, int64_t nval);
  /**
   * Get the current value.
   * @return the current value.
   */
  int64_t get() const;
  /**
   * Assignment operator from the self type.
   * @param right the right operand.
   * @return the reference to itself.
   */
  AtomicInt64& operator =(const AtomicInt64& right) {
    _assert_(true);
    if (&right == this) return *this;
    set(right.get());
    return *this;
  }
  /**
   * Assignment operator from integer.
   * @param right the right operand.
   * @return the reference to itself.
   */
  AtomicInt64& operator =(const int64_t& right) {
    _assert_(true);
    set(right);
    return *this;
  }
  /**
   * Cast operator to integer.
   * @return the current value.
   */
  operator int64_t() const {
    _assert_(true);
    return get();
  }
  /**
   * Summation assignment operator by integer.
   * @param right the right operand.
   * @return the reference to itself.
   */
  AtomicInt64& operator +=(int64_t right) {
    _assert_(true);
    add(right);
    return *this;
  }
  /**
   * Subtraction assignment operator by integer.
   * @param right the right operand.
   * @return the reference to itself.
   */
  AtomicInt64& operator -=(int64_t right) {
    _assert_(true);
    add(-right);
    return *this;
  }
  /**
   * Secure the least value
   * @param val the least value
   * @return the current value.
   */
  int64_t secure_least(int64_t val) {
    _assert_(true);
    while (true) {
      int64_t cur = get();
      if (cur >= val) return cur;
      if (cas(cur, val)) break;
    }
    return val;
  }
private:
  /** The value. */
  volatile int64_t value_;
  /** The alternative lock. */
  mutable SpinLock lock_;
};


/**
 * Task queue device.
 */
class TaskQueue {
public:
  class Task;
private:
  class WorkerThread;
  /** An alias of list of tasks. */
  typedef std::list<Task*> TaskList;
public:
  /**
   * Interface of a task.
   */
  class Task {
    friend class TaskQueue;
  public:
    /**
     * Default constructor.
     */
    explicit Task() : id_(0), thid_(0), aborted_(false) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    virtual ~Task() {
      _assert_(true);
    }
    /**
     * Get the ID number of the task.
     * @return the ID number of the task, which is incremented from 1.
     */
    uint64_t id() {
      _assert_(true);
      return id_;
    }
    /**
     * Get the ID number of the worker thread.
     * @return the ID number of the worker thread.  It is from 0 to less than the number of
     * worker threads.
     */
    uint32_t thread_id() {
      _assert_(true);
      return thid_;
    }
    /**
     * Check whether the thread is to be aborted.
     * @return true if the thread is to be aborted, or false if not.
     */
    bool aborted() {
      _assert_(true);
      return aborted_;
    }
  private:
    /** The task ID number. */
    uint64_t id_;
    /** The thread ID number. */
    uint64_t thid_;
    /** The flag to be aborted. */
    bool aborted_;
  };
  /**
   * Default Constructor.
   */
  TaskQueue() : thary_(NULL), thnum_(0), tasks_(), count_(0), mutex_(), cond_(), seed_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  virtual ~TaskQueue() {
    _assert_(true);
  }
  /**
   * Process a task.
   * @param task a task object.
   */
  virtual void do_task(Task* task) = 0;
  /**
   * Start the task queue.
   * @param thnum the number of worker threads.
   */
  void start(size_t thnum) {
    _assert_(thnum > 0 && thnum <= MEMMAXSIZ);
    thary_ = new WorkerThread[thnum];
    for (size_t i = 0; i < thnum; i++) {
      thary_[i].id_ = i;
      thary_[i].queue_ = this;
      thary_[i].start();
    }
    thnum_ = thnum;
  }
  /**
   * Finish the task queue.
   * @note This function blocks until all tasks in the queue are popped.
   */
  void finish() {
    _assert_(true);
    mutex_.lock();
    TaskList::iterator it = tasks_.begin();
    TaskList::iterator itend = tasks_.end();
    while (it != itend) {
      Task* task = *it;
      task->aborted_ = true;
      it++;
    }
    cond_.broadcast();
    mutex_.unlock();
    Thread::yield();
    for (double wsec = 1.0 / CLOCKTICK; true; wsec *= 2) {
      mutex_.lock();
      if (tasks_.empty()) {
        mutex_.unlock();
        break;
      }
      mutex_.unlock();
      if (wsec > 1.0) wsec = 1.0;
      Thread::sleep(wsec);
    }
    mutex_.lock();
    for (size_t i = 0; i < thnum_; i++) {
      thary_[i].aborted_ = true;
    }
    cond_.broadcast();
    mutex_.unlock();
    for (size_t i = 0; i < thnum_; i++) {
      thary_[i].join();
    }
    delete[] thary_;
  }
  /**
   * Add a task.
   * @param task a task object.
   * @return the number of tasks in the queue.
   */
  int64_t add_task(Task* task) {
    _assert_(task);
    mutex_.lock();
    task->id_ = ++seed_;
    tasks_.push_back(task);
    int64_t count = ++count_;
    cond_.signal();
    mutex_.unlock();
    return count;
  }
  /**
   * Get the number of tasks in the queue.
   * @return the number of tasks in the queue.
   */
  int64_t count() {
    _assert_(true);
    mutex_.lock();
    int64_t count = count_;
    mutex_.unlock();
    return count;
  }
private:
  /**
   * Implementation of the worker thread.
   */
  class WorkerThread : public Thread {
    friend class TaskQueue;
  public:
    explicit WorkerThread() : id_(0), queue_(NULL), aborted_(false) {
      _assert_(true);
    }
  private:
    void run() {
      _assert_(true);
      bool empty = false;
      while (true) {
        queue_->mutex_.lock();
        if (aborted_) {
          queue_->mutex_.unlock();
          break;
        }
        if (empty) queue_->cond_.wait(&queue_->mutex_, 1.0);
        Task * task = NULL;
        if (queue_->tasks_.empty()) {
          empty = true;
        } else {
          task = queue_->tasks_.front();
          task->thid_ = id_;
          queue_->tasks_.pop_front();
          queue_->count_--;
          empty = false;
        }
        queue_->mutex_.unlock();
        if (task) queue_->do_task(task);
      }
    }
    uint32_t id_;
    TaskQueue* queue_;
    Task* task_;
    bool aborted_;
  };
  /** Dummy constructor to forbid the use. */
  TaskQueue(const TaskQueue&);
  /** Dummy Operator to forbid the use. */
  TaskQueue& operator =(const TaskQueue&);
  /** The array of worker threads. */
  WorkerThread* thary_;
  /** The number of worker threads. */
  size_t thnum_;
  /** The list of tasks. */
  TaskList tasks_;
  /** The number of the tasks. */
  int64_t count_;
  /** The mutex for the task list. */
  Mutex mutex_;
  /** The condition variable for the task list. */
  CondVar cond_;
  /** The seed of ID numbers. */
  uint64_t seed_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
