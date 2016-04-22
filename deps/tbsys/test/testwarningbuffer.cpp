/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  testwarningbuffer.cpp,  09/27/2012 06:42:21 PM xiaochu Exp $
 * 
 * Author:  
 *   xiaochu.yh
 * Description:  
 *   test warning buffer
 * 
 */
#include "WarningBuffer.h"
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

using namespace tbsys;
using namespace std;

void check_init_state(WarningBuffer &buffer)
{
  assert(0 == buffer.get_readable_warning_count());
  assert(0 == buffer.get_total_warning_count());
  assert(NULL == buffer.get_warning(0));
  assert(NULL == buffer.get_warning(1));
  assert(NULL == buffer.get_warning(1000));
}

void tsi_basic_check()
{
  uint32_t i = 0;
  WarningBuffer * buffer = get_tsi_warning_buffer();
  assert(NULL != buffer);
  check_init_state(*buffer);
  // over write string test
  buffer->reset();
  check_init_state(*buffer);
  for (i = 0; i < buffer->get_buffer_size() * 2; i++)
  {
    char buf[1024];
    sprintf(buf, "%d", i);
    assert(0 == buffer->append_warning(buf));
  }
  for (i = 0; i < buffer->get_buffer_size(); i++)
  {
    char buf[1024];
    sprintf(buf, "%d", i + buffer->get_buffer_size());
    assert(0 == strcmp(buf, buffer->get_warning(i)));
  }
  fprintf(stderr, "pass overwrite test\n");
}

WarningBuffer *bug_buffer;

void* thread( void* para )
{
  para = NULL;
  pthread_t t = pthread_self();
  WarningBuffer *b1 = get_tsi_warning_buffer();
  fprintf(stderr, "thread %lu buf: %p\n", (uint64_t)t, b1);
  sleep(1);
  WarningBuffer *b2 = get_tsi_warning_buffer();
  fprintf(stderr, "thread %lu buf: %p\n", (uint64_t)t, b2);
  assert(b1 == b2);
  sleep(1);
  bug_buffer = b2;
  return NULL;
}

void tsi_multi_thread_check()
{
  pthread_t t1,t2;
  void *st1, *st2;
  pthread_create( &t1, NULL, thread, NULL );
  pthread_create( &t2, NULL, thread, NULL );
  pthread_join( t1, &st1 );
  pthread_join( t2, &st2 );
  fprintf(stderr, "multi_thread_check ok\n");
}

int main(int argc, char *argv[])
{
  tsi_basic_check();
  tsi_multi_thread_check();
  // basic random test
  uint32_t i = 0;
  WarningBuffer buffer;
  check_init_state(buffer); 
  for (i = 0; i < buffer.get_buffer_size(); i++)
  {
    char buf[1024];
    sprintf(buf, "hello%d", i);
    assert(NULL == buffer.get_warning(i));
    assert(i == buffer.get_readable_warning_count());
    assert(i == buffer.get_total_warning_count());
    assert(0 == buffer.append_warning(buf));
    assert(i+1 == buffer.get_readable_warning_count());
    assert(i+1 == buffer.get_total_warning_count());
    assert(NULL != buffer.get_warning(i));
  }
  
  for (i = 0; i < buffer.get_buffer_size() * 2; i++)
  {
    char buf[1024];
    sprintf(buf, "hello%d", i);
    assert(buffer.get_buffer_size() + i == buffer.get_total_warning_count());
    assert(0 == buffer.append_warning(buf));
    assert(buffer.get_buffer_size()  == buffer.get_readable_warning_count());
    assert(buffer.get_buffer_size() + i + 1 == buffer.get_total_warning_count());
    if (i < buffer.get_buffer_size())
    {
      assert(NULL != buffer.get_warning(i));
    }
    else
    {
      assert(NULL == buffer.get_warning(i));
    }
  }
#if 0
  for (i = 0; i <= buffer.get_buffer_size(); i++)
  {
    fprintf(stderr, "%s\n", buffer.get_warning(i));
  }
#endif
  // none over write string test
  buffer.reset();
  check_init_state(buffer);
  for (i = 0; i < buffer.get_buffer_size(); i++)
  {
    char buf[1024];
    sprintf(buf, "%d", i);
    assert(0 == buffer.append_warning(buf));
  }
  for (i = 0; i < buffer.get_buffer_size(); i++)
  {
    char buf[1024];
    sprintf(buf, "%d", i);
    assert(0 == strcmp(buf, buffer.get_warning(i)));
  }
  fprintf(stderr, "pass none overwrite test\n");

  // over write string test
  buffer.reset();
  check_init_state(buffer);
  for (i = 0; i < buffer.get_buffer_size() * 2; i++)
  {
    char buf[1024];
    sprintf(buf, "%d", i);
    assert(0 == buffer.append_warning(buf));
  }
  for (i = 0; i < buffer.get_buffer_size(); i++)
  {
    char buf[1024];
    sprintf(buf, "%d", i + buffer.get_buffer_size());
    assert(0 == strcmp(buf, buffer.get_warning(i)));
  }
  fprintf(stderr, "pass overwrite test\n");

  // truncate test
  buffer.reset();
  check_init_state(buffer);
  {
    char buf[1024*2];
    memset(buf, 'A', 1024*2);
    assert(0 == buffer.append_warning(buf));
    assert(0 != strcmp(buffer.get_warning(0), buf));
    assert(0 == strncmp(buffer.get_warning(0), buf, buffer.get_max_warn_len()-1));
  }
  fprintf(stderr, "pass truncate test\n");
  fprintf(stderr, "ok\n");
  return 0;
}
