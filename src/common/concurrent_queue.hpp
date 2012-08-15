/**
 * see Michael & Scott, Simple, Fast, and Practical Non-Blocking
 * concurrent queue algorithm
 *
 */
#include "atomic.h"

namespace flstorage
{

#define ATOMIC_CAS(val, cmpv, newv) atomic_cas_128bit((val), (cmpv), (newv))

template<class DataT>
struct QueueNode;


template<class DataT>
struct QueuePointer
{
  QueueNode<DataT>     *ptr;
  int64_t              seq;

  bool operator==(volatile const QueuePointer<DataT>& rhs) 
  {
    return ptr == rhs.ptr && seq == rhs.seq;
  }
};

template<class DataT>
union QueuePointerWrapper 
{
  QueuePointer<DataT>  real;
  __uint128_t          w128bit;

  QueuePointerWrapper()
  {
    this->real.ptr = NULL;
    this->real.seq = 0;
  }

  QueuePointerWrapper(const volatile QueuePointerWrapper &rhs)
  {
    this->w128bit = rhs.w128bit;
  }

  QueuePointerWrapper& operator=(volatile const QueuePointerWrapper &rhs)
  {
    this->w128bit = rhs.w128bit;
    return *this;
  }

  bool operator==(volatile const QueuePointerWrapper &rhs) 
  {
    return this->real == rhs.real;
  }
};

template<class DataT>
struct QueueNode
{
  DataT                      value;
  QueuePointerWrapper<DataT> next;
};

template<class DataT>
class ConcurrentQueue
{
 private:

  volatile QueuePointerWrapper<DataT>  head_;
  volatile QueuePointerWrapper<DataT>  tail_;

  void init() 
  {
    QueueNode<DataT> *node  = new QueueNode<DataT>();
    node->next.real.ptr     = NULL;
    this->head_.real.ptr    = this->tail_.real.ptr = node;
    this->head_.real.seq    = this->tail_.real.seq = 0;
  }
 
 public:
  
  ConcurrentQueue() 
  {
    init();
  }

  ~ConcurrentQueue()
  {
    while(dequeue(NULL) == true) {;}
  }
  
  void enqueue(const DataT &data) 
  {
    QueueNode<DataT>    *node = new QueueNode<DataT>(); // Allocate new node, TODO: from free list
    node->value               = data;            // copy data into node
    node->next.real.ptr       = NULL;  
    QueuePointerWrapper<DataT> pointer;
    pointer.real.ptr = node;
    
    QueuePointerWrapper<DataT> tail = this->tail_;       // read ptr and seq together
    QueuePointerWrapper<DataT> next = tail.real.ptr->next;
    while (true)                           // keep trying unitl enqueue done
    {
      if (tail.real == this->tail_.real)             // tail and next consistent ?
      {
        pointer.real.seq = next.real.seq + 1;        // update seq
        if (next.real.ptr == NULL)              // last one ?
        {
          // try to link node to last one
          if (ATOMIC_CAS(&(tail.real.ptr->next.w128bit), next.w128bit, pointer.w128bit))
            break;  // done, exit loop
        }
        else
        {
          // this->tail_ to next one
          QueuePointerWrapper<DataT> next_pointer;
          next_pointer.real.ptr = next.real.ptr;
          next_pointer.real.seq = tail.real.seq + 1;   
          ATOMIC_CAS(&this->tail_.w128bit, tail.w128bit, next_pointer.w128bit);
        }
      }                 
      // update tail and next
      tail = this->tail_;
      next = tail.real.ptr->next;
    }
    // try to swing  this->tail to the enqueued node;
    pointer.real.seq = tail.real.seq + 1;
    ATOMIC_CAS(&this->tail_.w128bit, tail.w128bit, pointer.w128bit); 
  }

  bool dequeue(DataT* pdata) 
  {
    while (true) 
    {
      QueuePointerWrapper<DataT> head = this->head_;
      QueuePointerWrapper<DataT> tail = this->tail_;
      QueuePointerWrapper<DataT> next = head.real.ptr->next;
      QueuePointerWrapper<DataT> pointer;
      pointer.real.ptr = next.real.ptr;

      if (head.real == this->head_.real)
      {
        if (head.real.ptr == tail.real.ptr)       // empty
        {
          if (next.real.ptr == NULL)
            return false;
          // advance tail 
          pointer.real.seq = tail.real.seq + 1;
          ATOMIC_CAS(&this->tail_.w128bit, tail.w128bit, pointer.w128bit);
        }
        else
        {
          // Read before CAS
          // Otherwise, another dequeue might free the next node
          if (pdata != NULL)
            *pdata = next.real.ptr->value;
          pointer.real.seq = head.real.seq + 1;
          if (ATOMIC_CAS(&this->head_.w128bit, head.w128bit, pointer.w128bit)) 
          {
            // delete safe
            assert(head.real.ptr != NULL);
            delete head.real.ptr;
            break;
          }
        }
      }
    }
    return true;
  }
 
};


}

