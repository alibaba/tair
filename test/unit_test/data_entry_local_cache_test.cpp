#include <gtest/gtest.h>

#include <unistd.h>

#include "test_environment.h"
#include "data_entry_local_cache.h"

using namespace tair;
using namespace tair::common;
using namespace std;

TestEnvironment* const env = dynamic_cast<TestEnvironment * const>(
    testing::AddGlobalTestEnvironment(new TestEnvironment));

class data_entry_local_cache_test : public testing::Test 
{
public:
  data_entry_local_cache_test() : 
      cache(MAX_COUNT, EXPIRE_TIME) 
  {
  }

  virtual ~data_entry_local_cache_test() {}

protected:
  virtual void SetUp() 
  {
    cache.set_capability(MAX_COUNT);
    cache.set_expire(EXPIRE_TIME);
    cache.clear();
  }

  virtual void TearDown() 
  {
    cache.clear();
  }

protected:
  static const size_t MAX_COUNT = 100;
  static const int64_t EXPIRE_TIME = 20;
  static const size_t KEY_LEN = 64;
  static const size_t VAL_LEN = 1024;

  data_entry_local_cache cache;
};


TEST_F(data_entry_local_cache_test, cap_0_tc)
{ 
  const size_t count = 10;
  cache.set_capability(0);
  for (size_t i = 0; i < count; ++i) {
    data_entry_local_cache::result res;
    data_entry test_key = env->random_data_entry(KEY_LEN);
    data_entry test_val = env->random_data_entry(VAL_LEN);
    data_entry value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, data_entry_local_cache::MISS);
    ASSERT_EQ(cache.size(), static_cast<size_t>(0));
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(0));
    value.set_data(NULL, 0, false);
    res = cache.get(test_key, value);
    ASSERT_EQ(res, data_entry_local_cache::MISS);
  }
}

TEST_F(data_entry_local_cache_test, put_get_10k_tc)
{ 
  const size_t count = 10 * 1000;

  cache.set_capability(100);
  for (size_t i = 0; i < count; ++i) {
    data_entry_local_cache::result res;
    data_entry test_key = env->random_data_entry(KEY_LEN);
    data_entry test_val = env->random_data_entry(VAL_LEN);
    data_entry value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, data_entry_local_cache::MISS);
    size_t cap = cache.get_capability();
    ASSERT_EQ(cache.size(), i > cap ? cap : i);
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), i + 1 > cap ? cap : i + 1);
    value.set_data(NULL, 0, false);
    res = cache.get(test_key, value);
    ASSERT_EQ(res, data_entry_local_cache::HIT);
    ASSERT_FALSE(test_val < value);
    ASSERT_FALSE(value < test_val);
  }
}

TEST_F(data_entry_local_cache_test, put_10k_256b_tc)
{ 
  const size_t count = 10 * 1000;

  for (size_t i = 0; i < count; ++i) {
    data_entry test_key = env->random_data_entry(56);
    data_entry test_val = env->random_data_entry(200);
    cache.put(test_key, test_val);
    size_t cap = cache.get_capability();
    ASSERT_EQ(cache.size(), i + 1 > cap ? cap : i + 1);
    data_entry_local_cache::result res;
    data_entry value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, data_entry_local_cache::HIT);
    ASSERT_FALSE(test_val < value);
    ASSERT_FALSE(value < test_val);
  }
}

TEST_F(data_entry_local_cache_test, put_10k_same_key_tc)
{ 
  const size_t count = 10 * 1000;
  data_entry test_key = env->random_data_entry(KEY_LEN);
  for (size_t i = 0; i < count; ++i) {
    data_entry test_val = env->random_data_entry(VAL_LEN);
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(1));
    data_entry_local_cache::result res;
    data_entry value;
    res = cache.get(test_key, value);
    ASSERT_EQ(res, data_entry_local_cache::HIT);
    ASSERT_FALSE(test_val < value);
    ASSERT_FALSE(value < test_val);
  }
}

TEST_F(data_entry_local_cache_test, put_10k_same_key_val_tc)
{ 
  const size_t count = 10 * 1000;
  const size_t expect_size = static_cast<size_t>(1);
  data_entry test_key = env->random_data_entry(KEY_LEN);
  data_entry test_val = env->random_data_entry(VAL_LEN);
  for (size_t i = 0; i < count; ++i) {
    cache.put(test_key, test_val);
    EXPECT_EQ(cache.size(), expect_size);
  }
  data_entry_local_cache::result res;
  data_entry value;
  res = cache.get(test_key, value);
  ASSERT_EQ(res, data_entry_local_cache::HIT);
  ASSERT_FALSE(test_val < value);
  ASSERT_FALSE(value < test_val);
}

TEST_F(data_entry_local_cache_test, evict_tc)
{ 
  const size_t cap = 100;
  tair::data_entry_local_cache cache(cap, 10);
  char buf[1024];
  
  for (int i = 0; i < 110; ++i) {
    snprintf(buf, 1024, "%d", i); 
    data_entry key(buf);
    cache.put(key, key);
  }
  cache.set_expire(10000);
  for (int i = 0; i < 110; ++i) {
    snprintf(buf, 1024, "%d", i); 
    data_entry key(buf);
    data_entry value;
    if (i < 10)
      EXPECT_EQ(cache.get(key, value), data_entry_local_cache::MISS);
    else {
      ASSERT_EQ(cache.get(key, value), data_entry_local_cache::HIT);
      ASSERT_FALSE(key < value);
      ASSERT_FALSE(value < key);
    }
  }
}

