/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  WarningBuffer.h,  09/27/2012 05:01:00 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh
 * Description:
 *   fix length string ring buffer for warning message
 *
 */
#ifndef TBSYS_WARNING_BUFFER_H_
#define TBSYS_WARNING_BUFFER_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <new>
#include "tblog.h"

namespace tbsys
{
    class WarningBuffer
  {
    public:
      WarningBuffer() : append_idx_(0), total_warning_count_(0)
    {
      // nop
    }
      ~WarningBuffer()
      {
        reset();
      }
      inline void reset(void)
      {
        append_idx_ = 0;
        total_warning_count_ = 0;
      }
      inline static void set_warn_log_on(const bool is_log_on)
      {
        is_log_on_ = is_log_on;
      }
      inline static bool is_warn_log_on(void)
      {
        return is_log_on_;
      }
      inline uint32_t get_total_warning_count(void) const
      {
        return total_warning_count_;
      }
      inline uint32_t get_buffer_size(void) const
      {
        return BUFFER_SIZE;
      }
      inline uint32_t get_readable_warning_count(void) const
      {
        return (total_warning_count_ < get_buffer_size()) ? total_warning_count_ : get_buffer_size();
      }
      inline uint32_t get_max_warn_len(void) const
      {
        return WarningItem::STR_LEN;
      }

      /*
       * 获取WARNING字符串
       * idx取值范围[0, get_readable_warning_count)
       * 返回值为对应WARNING字符串
       * 当idx超出取值范围的时候返回NULL
       */
      const char *get_warning(const uint32_t idx) const
      {
        const char *ret = NULL;
        if (idx < get_readable_warning_count())
        {
          uint32_t loc = idx;
          if (total_warning_count_ > BUFFER_SIZE)
          {
            loc = (append_idx_ + idx) % BUFFER_SIZE;
          }
          ret = item_[loc].get();
        }
        return ret;
      }
      /*
       * 将WARNING写入到Buffer中
       * 如果buffer已满，覆盖最老的Warning
       * 如果str长度超过STR_LEN，自动截断
       */
      int append_warning(const char *str)
      {
        //if (is_log_on_)
        {
          item_[append_idx_].set(str);
          append_idx_ = (append_idx_ + 1) % BUFFER_SIZE;
          total_warning_count_++;
        }
        return 0;
      }

      void reset_err_msg()
      {
        err_msg_.reset_err_msg();
      }

    void set_err_msg(const char* str)
    {
      err_msg_.set(str);
    }
    const char* get_err_msg() const
    {
      return err_msg_.get();
    }
      WarningBuffer& operator= (const WarningBuffer &other)
    {
      if (this != &other)
      {
        uint32_t n = 0;
        if (total_warning_count_ >= BUFFER_SIZE)
        {
          n = BUFFER_SIZE;
        }
        else
        {
          n = other.append_idx_;
        }
        for (uint32_t i = 0; i < n; ++i)
        {
          item_[i] = other.item_[i];
        }
        err_msg_ = other.err_msg_;
        append_idx_ = other.append_idx_;
        total_warning_count_ = other.total_warning_count_;
      }
      return *this;
    }
    private:
      struct WarningItem{
        static const uint32_t STR_LEN = 512;
        char msg_[STR_LEN];
        int64_t timestamp_;
        int log_level_;
        int line_no_;

        void reset_err_msg()
        {
          msg_[0] = '\0';
        }
        void set(const char*str)
        {
          snprintf(msg_, STR_LEN, "%s", str);
        }
        const char *get() const
        {
          return static_cast<const char*>(msg_);
        }
        WarningItem &operator= (const WarningItem &other)
        {
          if (this != &other)
          {
            strcpy(msg_, other.msg_);
            timestamp_ = other.timestamp_;
            log_level_ = other.log_level_;
            line_no_ = other.line_no_;
          }
          return *this;
        }
      };

    private:
      // const define
      static const uint32_t BUFFER_SIZE = 64;
      WarningItem item_[BUFFER_SIZE];
      WarningItem err_msg_;
      uint32_t append_idx_;
      uint32_t total_warning_count_;
      static bool is_log_on_;
  };

  class WarningBufferFactory
  {
    public:
      WarningBufferFactory() : key_(INVALID_THREAD_KEY)
    {
      create_thread_key();
    }

      ~WarningBufferFactory()
      {
        delete_thread_key();
      }

      WarningBuffer* get_buffer() const
      {
        static __thread WarningBuffer * buffer = NULL;
        if (NULL != buffer)
        {
          //return buffer, faster path
        }
        else if (INVALID_THREAD_KEY != key_)
        {
          void* ptr = pthread_getspecific(key_);
          if (NULL == ptr)
          {
            ptr = malloc(sizeof(WarningBuffer));
            if (NULL != ptr)
            {
              int ret = pthread_setspecific(key_, ptr);
              if (0 != ret)
              {
                // TBSYS_LOG(ERROR, "pthread_setspecific failed:%d", ret);
                free(ptr);
                ptr = NULL;
              }
              else
              {
                buffer = new (ptr) WarningBuffer();
              }
            }
            else
            {
              // malloc failed;
              // TBSYS_LOG(ERROR, "malloc thread specific memeory failed.");
            }
          }
          else
          {
            // got exist ptr;
            buffer = reinterpret_cast<WarningBuffer*>(ptr);
          }
        }
        else
        {
          // TBSYS_LOG(ERROR, "thread key must be initialized "
          //    "and size must great than zero, key:%u,size:%d", key_, size_);
        }
        return buffer;
      }
    private:
      int create_thread_key()
      {
        int ret = pthread_key_create(&key_, destroy_thread_key);
        if (0 != ret)
        {
          TBSYS_LOG(ERROR, "cannot create thread key:%d", ret);
        }
        return (0 == ret) ? 0 : 1;
      }

      int delete_thread_key()
      {
        int ret = -1;
        if (INVALID_THREAD_KEY != key_)
        {
          ret = pthread_key_delete(key_);
        }
        if (0 != ret)
        {
          TBSYS_LOG(ERROR, "delete thread key key_ failed.");
        }
        return (0 == ret) ? 0 : 1;
      }

      static void destroy_thread_key(void* ptr)
      {
        if (NULL != ptr) free(ptr);
        //fprintf(stderr, "destroy %p\n", ptr);
      }
    private:
      static const pthread_key_t INVALID_THREAD_KEY = ((uint32_t)-1);//UINT32_MAX;;
    private:
      pthread_key_t key_;
  };

  WarningBuffer *get_tsi_warning_buffer();
}
#endif //TBSYS_WARNING_BUFFER_H_
