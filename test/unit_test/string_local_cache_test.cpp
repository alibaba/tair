#include <gtest/gtest.h>

#include <unistd.h>
#include <string>

#include "test_environment.h"
#include "string_local_cache.h"

using namespace tair;
using namespace std;

TestEnvironment* const env = dynamic_cast<TestEnvironment * const>(
    testing::AddGlobalTestEnvironment(new TestEnvironment));

class string_local_cache_testable : public string_local_cache
{
public:
  string_local_cache_testable(size_t cap) :
      string_local_cache(cap) {}
  using tair::string_local_cache::lru_size;
};

class string_local_cache_test : public testing::Test 
{
public:
  string_local_cache_test() : 
      cache(MAX_COUNT ) 
  {
  }

  virtual ~string_local_cache_test() {}

protected:

  virtual void SetUp() 
  {
    cache.set_capacity(MAX_COUNT);
    cache.set_expire(EXPIRE_TIME);
    cache.clear();
  }

  virtual void TearDown() 
  {
    cache.clear();
  }

  int64_t random_int64t() 
  {
    int fd = open("/dev/urandom", O_RDONLY);
    int64_t ret = 0;
    read(fd, (char *)(&ret), 8);
    close(fd);
    return ret;
  }

protected:
  static const size_t MAX_COUNT = 100;
  static const int64_t EXPIRE_TIME = 20;
  static const size_t KEY_LEN = 64;
  static const size_t VAL_LEN = 1024;

  string_local_cache_testable cache;
};

TEST_F(string_local_cache_test, simple_tc)
{ 
  string test_key("Helll");
  string test_value("World");
  string test_new_value("New World");
  string value;
  string_local_cache::result res;
  // test put new key 
  cache.put(test_key, test_value);
  value.clear();
  cache.set_expire(1000);
  res = cache.get(test_key, value);
  ASSERT_EQ(res, string_local_cache::HIT);
  ASSERT_EQ(test_value, value);
  // test override key
  cache.put(test_key, test_new_value);
  value.clear();
  res = cache.get(test_key, value);
  ASSERT_EQ(res, string_local_cache::HIT);
  ASSERT_EQ(test_new_value, value);
  // test key value expire 
  cache.set_expire(EXPIRE_TIME);
  res = cache.get(test_key, value);
  usleep((EXPIRE_TIME + 1) * 1000);
  value.clear();
  res = cache.get(test_key, value);
  ASSERT_EQ(res, string_local_cache::EXPIRED);
  ASSERT_EQ(test_new_value, value);
}

TEST_F(string_local_cache_test, cap_0_tc)
{ 
  const size_t count = 10;
  cache.set_capacity(0);
  for (size_t i = 0; i < count; ++i) {
    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    string test_val = env->random_string(VAL_LEN);
    string value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::MISS);
    ASSERT_EQ(cache.size(), static_cast<size_t>(0));
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(0));
    ASSERT_EQ(cache.lru_size(), cache.size());
    value.clear();
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::MISS);
  }
}


TEST_F(string_local_cache_test, put_get_remove_10k_uniq_tc) 
{
  const size_t count = 10 * 1000;
  string pre_key;
  for (size_t i = 0; i < count; ++i) {
    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    test_key.append((char *)(&i), sizeof(i));
    string test_val = env->random_string(VAL_LEN);
    string value;
    if (i > 0) {
      res = cache.get(pre_key, value);
      EXPECT_EQ(res, string_local_cache::HIT);
      cache.remove(pre_key);
      res = cache.get(pre_key, value);
      EXPECT_EQ(res, string_local_cache::MISS);
    } else {
      EXPECT_EQ(cache.size(), static_cast<size_t>(0));
    }
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(1));
    ASSERT_EQ(cache.lru_size(), cache.size());
    value.clear();
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::HIT);
    ASSERT_EQ(test_val, value);
    pre_key = test_key;
  }

}

TEST_F(string_local_cache_test, test_lru_tc) 
{
  const size_t count = 150;
  cache.set_capacity(100);
  cache.set_expire(0);
  for (size_t i = 0; i < count; ++i) {
    string_local_cache::result res;
    string test_key((char *)(&i), sizeof(i));
    string test_val = env->random_string(VAL_LEN);
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.lru_size(), cache.size());
    string value;
    res = cache.get(test_key, value);
    EXPECT_EQ(res, string_local_cache::HIT);
    EXPECT_EQ(test_val, value);
  }

  const size_t cap = cache.get_capacity();
  for (size_t i = 0; i < count - cap; ++i) {
    string_local_cache::result res;
    string test_key((char *)(&i), sizeof(i));
    string value;
    res = cache.get(test_key, value);
    EXPECT_EQ(res, string_local_cache::MISS);
  }
  
  for (size_t i = count - cap; i < cap; ++i) {
    string_local_cache::result res;
    string test_key((char *)(&i), sizeof(i));
    string value;
    res = cache.get(test_key, value);
    EXPECT_EQ(res, string_local_cache::HIT);
  }
}


TEST_F(string_local_cache_test, simple_test_evict_tc) 
{
  const size_t count = 100;
  string pre_key;
  cache.set_capacity(1);
  for (size_t i = 0; i < count; ++i) {
    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    test_key.append((char *)(&i), sizeof(i));
    string test_val = env->random_string(VAL_LEN);
    string value;
    if (i > 0) {
      res = cache.get(pre_key, value);
      EXPECT_EQ(res, string_local_cache::HIT);
    } else {
      EXPECT_EQ(cache.size(), static_cast<size_t>(0));
    }
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(1));
    ASSERT_EQ(cache.lru_size(), cache.size());
    value.clear();
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::HIT);
    ASSERT_EQ(test_val, value);
    res = cache.get(pre_key, value);
    ASSERT_EQ(res, string_local_cache::MISS);
    pre_key = test_key;
  }
}

TEST_F(string_local_cache_test, put_get_1k_uniq_tc)
{ 
  const size_t count = 1000;

  cache.set_capacity(100000);
  for (size_t i = 0; i < count; ++i) {
    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    test_key.append((char *)(&i), sizeof(i));
    string test_val = env->random_string(VAL_LEN);
    string value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::MISS);
    size_t cap = cache.get_capacity();
    ASSERT_EQ(cache.size(), i > cap ? cap : i);
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), i + 1 > cap ? cap : i + 1);
    ASSERT_EQ(cache.lru_size(), cache.size());
    value.clear();
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::HIT);
    ASSERT_EQ(test_val, value);
  }
}

TEST_F(string_local_cache_test, put_get_10k_tc)
{ 
  const size_t count = 10 * 1000;

  cache.set_capacity(100);
  for (size_t i = 0; i < count; ++i) {
    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    string test_val = env->random_string(VAL_LEN);
    string value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::MISS);
    size_t cap = cache.get_capacity();
    ASSERT_EQ(cache.size(), i > cap ? cap : i);
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), i + 1 > cap ? cap : i + 1);
    ASSERT_EQ(cache.lru_size(), cache.size());
    value.clear();
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::HIT);
    ASSERT_EQ(test_val, value);
  }
}

TEST_F(string_local_cache_test, put_10k_256b_tc)
{ 
  const size_t count = 10 * 1000;

  for (size_t i = 0; i < count; ++i) {
    string test_key = env->random_string(56);
    string test_val = env->random_string(200);
    cache.put(test_key, test_val);
    size_t cap = cache.get_capacity();
    ASSERT_EQ(cache.size(), i + 1 > cap ? cap : i + 1);
    ASSERT_EQ(cache.lru_size(), cache.size());
    string_local_cache::result res;
    string value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::HIT);
    ASSERT_EQ(test_val, value);
  }
}

TEST_F(string_local_cache_test, put_10k_same_key_tc)
{ 
  const size_t count = 10 * 1000;
  string test_key = env->random_string(KEY_LEN);
  for (size_t i = 0; i < count; ++i) {
    string test_val = env->random_string(VAL_LEN);
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(1));
    ASSERT_EQ(cache.lru_size(), cache.size());
    string_local_cache::result res;
    string value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, string_local_cache::HIT);
    ASSERT_EQ(test_val, value);
  }
}

TEST_F(string_local_cache_test, put_10k_same_key_val_tc)
{ 
  const size_t count = 10 * 1000;
  const size_t expect_size = static_cast<size_t>(1);
  string test_key = env->random_string(KEY_LEN);
  string test_val = env->random_string(VAL_LEN);
  for (size_t i = 0; i < count; ++i) {
    cache.put(test_key, test_val);
    EXPECT_EQ(cache.size(), expect_size);
    EXPECT_EQ(cache.lru_size(), cache.size());
  }
  string_local_cache::result res;
  string value;
  res = cache.get(test_key, value);
  ASSERT_EQ(res, string_local_cache::HIT);
  ASSERT_EQ(test_val, value);
}

TEST_F(string_local_cache_test, evict_tc)
{ 
  const size_t cap = 100;
  tair::string_local_cache cache(cap);
  char buf[1024];
  
  for (int i = 0; i < 110; ++i) {
    snprintf(buf, 1024, "%d", i); 
    string key(buf);
    cache.put(key, key);
  }
  cache.set_expire(10000);
  for (int i = 0; i < 110; ++i) {
    snprintf(buf, 1024, "%d", i); 
    string key(buf);
    string value;
    if (i < 10)
      EXPECT_EQ(cache.get(key, value), string_local_cache::MISS);
    else {
      ASSERT_EQ(cache.get(key, value), string_local_cache::HIT);
      ASSERT_EQ(key, value);
    }
  }
}

