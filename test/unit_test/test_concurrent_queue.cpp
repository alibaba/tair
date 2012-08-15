#include <gtest/gtest.h>
#include <iostream>
#include <map>

#include "hash/concurrent_queue.hpp"
#include "atomic.h"



using namespace flstorage;
using namespace std;


class TestConcurrentQueue : public testing::Test
{
 public:
  TestConcurrentQueue()
  {
  }

 protected:
  ConcurrentQueue<int64_t> *queue;
  
  virtual void SetUp()
  {
    queue = new ConcurrentQueue<int64_t>();
    push_seqs.clear();
    pop_seqs.clear();
    atomic_set(&push_thread_seq, 0);
    atomic_set(&push_thread_count, 0);

    for (int i = 0; i < 128; ++i) 
    {
      atomic_set(&push_seqs[i], 0);
      atomic_set(&pop_seqs[i], 0);
    }
  }

  virtual void TearDown()
  {
    delete queue;
  }

  std::vector<pthread_t> pids;
  
  void StartThread(int n, void *(*func)(void*))
  {
    for (int i = 0; i < n; ++i)
    {
      pthread_t pid;
      pthread_create(&pid, NULL, func, this); 
      pids.push_back(pid);
    }
  }

  void JoinThread()
  {
    for (size_t i = 0; i < pids.size(); ++i)
    {
      pthread_join(pids[i], NULL);
    }
    pids.clear();
  }

  const static int32_t MAX = 100000;

  static void *ThreadPush(void *arg)
  {
    TestConcurrentQueue *tqueue = (TestConcurrentQueue *)(arg);
    tqueue->push_thread(atomic_inc_return(&tqueue->push_thread_seq));
    return NULL;
  }

  void push_thread(int32_t seq) 
  {
    for (int32_t i = 0; i < MAX; ++i)
    {
      int64_t value = seq;
      value <<= 32;
      value += i;
      atomic_set(&push_seqs[seq], i);
      queue->enqueue(value); 
    }
  }

  static void *ThreadPopSingle(void *arg)
  {
    TestConcurrentQueue *tqueue = (TestConcurrentQueue *)(arg);
    tqueue->pop_single_thread();
    return NULL;
  }

  void pop_single_thread()
  {
    do
    {
      int64_t data = 0;
      if (queue->dequeue(&data))
      {
        int32_t seq = static_cast<int32_t>(data >> 32);
        int32_t val = static_cast<int32_t>(data & 0xFFFFFFFF);
        ASSERT_LE(val, atomic_read(&push_seqs[seq]));
        ASSERT_EQ(val, atomic_read(&pop_seqs[seq]));
        atomic_inc(&pop_seqs[seq]); 
        if (val == MAX - 1)
          atomic_dec(&push_thread_count);
      }
    } while (atomic_read(&push_thread_count) > 0);
  }

  static void *ThreadPopMulti(void *arg)
  {
    TestConcurrentQueue *tqueue = (TestConcurrentQueue *)(arg);
    tqueue->pop_multi_thread();
    return NULL;
  }


  void pop_multi_thread()
  {
    do
    {
      int64_t data = 0;
      if (queue->dequeue(&data))
      {
        int32_t seq = static_cast<int32_t>(data >> 32);
        int32_t val = static_cast<int32_t>(data & 0xFFFFFFFF);
        ASSERT_LE(val, atomic_read(&push_seqs[seq]));
        if (val == MAX - 1)
          atomic_dec(&push_thread_count);
      } 
    } while (atomic_read(&push_thread_count) > 0);
  }

  atomic_t push_thread_count;
  atomic_t push_thread_seq;
  map<int32_t, atomic_t> push_seqs; 
  map<int32_t, atomic_t> pop_seqs; 
};


TEST_F(TestConcurrentQueue, test_simple_single_thread)
{
  int64_t seq = 0;
  int64_t pop = 0;
  for (int i = 0; i < 100; ++i)
  {
    for (int j = 0; j< i; ++j)
    {
      queue->enqueue(seq);
      ++seq;
    }
    for (int j = 0; j < i; ++j)
    {
      int64_t t = -1;
      queue->dequeue(&t);
      ASSERT_EQ(pop, t);
      ++pop;
    }
  }
}

TEST_F(TestConcurrentQueue, test_simple_popnull)
{
  int64_t seq = 0;
  int64_t pop = 0;
  for (int i = 0; i < 100; ++i)
  {
    for (int j = 0; j< i; ++j)
    {
      queue->enqueue(seq);
      ++seq;
    }
    int j = 0;
    for (j = 0; j < i/2; ++j)
    {
      queue->dequeue(NULL);
      ++pop;
    }
    for (; j < i; ++j)
    {
      int64_t t = -1;
      queue->dequeue(&t);
      ASSERT_EQ(pop, t);
      ++pop;
    }
  }
}

TEST_F(TestConcurrentQueue, test_singlepush_singlepop)
{
  atomic_set(&push_thread_count, 1);
  StartThread(1, ThreadPush);
  StartThread(1, ThreadPopSingle);
  JoinThread();
  ASSERT_EQ(0, atomic_read(&push_thread_count));
}

TEST_F(TestConcurrentQueue, test_multipush_singlepop)
{
  atomic_set(&push_thread_count, 32);
  StartThread(32, ThreadPush);
  StartThread(1, ThreadPopSingle);
  JoinThread();
  ASSERT_EQ(0, atomic_read(&push_thread_count));
}

TEST_F(TestConcurrentQueue, test_multipush_multipop)
{  
  atomic_set(&push_thread_count, 16);
  StartThread(16, ThreadPush);
  StartThread(16, ThreadPopMulti);
  JoinThread();
  ASSERT_EQ(0, atomic_read(&push_thread_count));
}

TEST_F(TestConcurrentQueue, test_singleush_multipop)
{
  atomic_set(&push_thread_count, 1);
  StartThread(1, ThreadPush);
  StartThread(1, ThreadPopMulti);
  JoinThread();
  ASSERT_EQ(0, atomic_read(&push_thread_count));
}

