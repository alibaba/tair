#ifndef TEST_ENVIROMENT_H
#define TEST_ENVIROMENT_H
#include <gtest/gtest.h>
#include <tbsys.h>
#include <Time.h>
#include <tbnet.h>
#include "data_entry.hpp"

namespace tair 
{

class TestEnvironment : public ::testing::Environment {
public:
  virtual ~TestEnvironment() {}
    // Override this to define how to set up the environment.
  virtual void SetUp() 
  {  
    srandom(tbutil::Time::now().toMilliSeconds());
    size_t len = BUF_LEN + 8;
    buf = new char[len];
    int fd = open("/dev/urandom", O_RDONLY);
    for (size_t i = 0; i < BUF_LEN; i+=8) {
      read(fd, buf + i, 8);
    }
    close(fd);
  }
    // Override this to define how to tear down the environment.
  virtual void TearDown() 
  {
    delete[] buf;
  }

  std::string random_string(size_t max_len, bool fixed = true) const
  {
    assert(max_len < BUF_LEN);
    int64_t r = random();
    size_t offset = r % BUF_LEN;
    size_t length = fixed ? max_len : random() % max_len ;
    if (length > BUF_LEN - offset)
      offset = BUF_LEN - length;

    if (length < sizeof(int64_t))
      return std::string(buf + offset, length);
    else {
      length -= sizeof(int64_t);
      std::string rstring(buf + offset, length);
      int64_t now = tbutil::Time::now().toMicroSeconds();
      rstring.append((char *)(&now), sizeof(int64_t));
      return rstring;
    }
  }

  tair::common::data_entry random_data_entry(size_t max_len, bool fixed = true) const
  {
    std::string slice = random_string(max_len, fixed); 
    return tair::common::data_entry(slice.c_str(), slice.length(), true);
  }
private:
  char *buf;
  static const size_t BUF_LEN = 1024 * 1024;
};

}

#endif
