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
  using tair::string_local_cache::evict_one;
  using tair::string_local_cache::internal_entry;
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

TEST_F(string_local_cache_test, test01_simple_tc)
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

TEST_F(string_local_cache_test, test02_cap_0_tc)
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


TEST_F(string_local_cache_test, test03_put_get_remove_10k_uniq_tc) 
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

TEST_F(string_local_cache_test, test04_test_lru_tc) 
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


TEST_F(string_local_cache_test, test05_simple_test_evict_tc) 
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

TEST_F(string_local_cache_test, test06_put_get_1k_uniq_tc)
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

TEST_F(string_local_cache_test, test07_put_get_10k_tc)
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

TEST_F(string_local_cache_test, test08_put_10k_256b_tc)
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

TEST_F(string_local_cache_test, test09_put_10k_same_key_tc)
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

TEST_F(string_local_cache_test, test10_put_10k_same_key_val_tc)
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

TEST_F(string_local_cache_test, test11_evict_tc)
{ 
  const size_t cap = 100;
  cache.set_capacity(cap);
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

/*TEST_F(string_local_cache_test, test12_cap_n_1)
{
    cache.clear();
    // set capability -1
    const size_t cap = -1;
    cache.set_capacity(cap);

    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    string test_val = env->random_string(VAL_LEN);
    cache.put(test_key, test_key);

    // check capability 0
    res = cache.get(test_key, test_val);
    EXPECT_EQ(res, string_local_cache::MISS);
    EXPECT_EQ(static_cast<size_t>(0), cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    cache.clear();
}*/

TEST_F(string_local_cache_test, test13_touch_normal)
{
    cache.clear();
    // set capability 10
    const size_t cap = 10;
    cache.set_capacity(cap);
    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    string keys[12];
    string values[12];
    string key = "";
    string value = "";
    for(int i = 0; i < 12; i++)
    {
        keys[i] = "";
        values[i] = "";
    }
    // put 10 keys into cache
    for(int j = 0; j < 10; j++)
    {
        key = env->random_string(KEY_LEN);
        value = env->random_string(VAL_LEN);
        cache.put(key, value);
        keys[j] = key;
        values[j] = value;
    }
    // check that all keys exists
    for(int k = 0; k < 10; k++)
        EXPECT_EQ(cache.get(keys[k], values[k]), string_local_cache::HIT);

    //touch the 1st key
    cache.touch(keys[0]);

    //put a new key into cache
    key = env->random_string(KEY_LEN);
    value = env->random_string(VAL_LEN);
    cache.put(key, value);
    keys[10] = key;
    values[10] = value;

    //assert that the 1st is still in
    EXPECT_EQ(cache.get(keys[0], values[0]), string_local_cache::HIT);
    EXPECT_EQ(cache.get(keys[1], values[1]), string_local_cache::MISS);

    //put a new key into cache
    key = env->random_string(KEY_LEN);
    value = env->random_string(VAL_LEN);
    cache.put(key, value);
    keys[11] = key;
    values[11] = value;

    //assert that the 1st is still in
    EXPECT_EQ(cache.get(keys[0], values[0]), string_local_cache::HIT);
    EXPECT_EQ(cache.get(keys[2], values[2]), string_local_cache::MISS);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test14_touch_missed)
{
    cache.clear();
    // set capability 10
    const size_t cap = 10;
    cache.set_capacity(cap);
    string keys[11];
    string values[11];
    string key = "";
    string value = "";
    for(int i = 0; i < 11; i++)
    {
        keys[i] = "";
        values[i] = "";
    }
    // put 10 keys into cache
    for(int j = 0; j < 11; j++)
    {
        key = env->random_string(KEY_LEN);
        value = env->random_string(VAL_LEN);
        cache.put(key, value);
        keys[j] = key;
        values[j] = value;
    }

    // touch the 1st key (evicted)
    cache.touch(keys[0]);

    // check that the 1st key evicted
    EXPECT_EQ(cache.get(keys[0], values[0]), string_local_cache::MISS);
    // check that the rest keys exists
    for(int k = 1; k < 11; k++)
        EXPECT_EQ(cache.get(keys[k], values[k]), string_local_cache::HIT);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test15_clear_normal)
{
    cache.clear();
    // set capability 1000
    const size_t cap = 1000;
    cache.set_capacity(cap);
    string keys[1000];
    string values[1000];
    string key = "";
    string value = "";
    for(int i = 0; i < 1000; i++)
    {
        keys[i] = "";
        values[i] = "";
    }
    // put 10 keys into cache
    for(int j = 0; j < 1000; j++)
    {
        key = env->random_string(KEY_LEN);
        value = env->random_string(VAL_LEN);
        cache.put(key, value);
        keys[j] = key;
        values[j] = value;
    }

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    // clear the cache
    cache.clear();

    for(int k = 0; k < 1000; k++)
    {
        // check that all key evicted
        EXPECT_EQ(cache.get(keys[k], values[k]), string_local_cache::MISS);
    }
    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test16_clear_empty_cap)
{
    cache.clear();
    // set capability 0
    const size_t cap1 = 0;
    cache.set_capacity(cap1);

    string key = env->random_string(KEY_LEN);
    string value = env->random_string(VAL_LEN);
    cache.put(key, value);
    EXPECT_EQ(cache.get(key, value), string_local_cache::MISS);

    EXPECT_EQ(static_cast<size_t>(0), cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    cache.clear();

    // set capability 0
    const size_t cap2 = 0;
    cache.set_capacity(cap2);

    cache.put(key, value);
    EXPECT_EQ(cache.get(key, value), string_local_cache::MISS);

    EXPECT_EQ(static_cast<size_t>(0), cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test17_remove_many_times)
{
    cache.clear();
    const size_t cap = 1;
    cache.set_capacity(cap);

    string key1 = env->random_string(KEY_LEN);
    string value1 = env->random_string(VAL_LEN);
    string key2 = env->random_string(KEY_LEN);
    string value2 = env->random_string(VAL_LEN);

    // put key1 then remove
    cache.put(key1, value1);
    EXPECT_EQ(cache.get(key1, value1), string_local_cache::HIT);
    cache.remove(key1);
    EXPECT_EQ(cache.get(key1, value1), string_local_cache::MISS);

    // remove key1 500 times
    for(int i = 0; i < 500; i++)
        cache.remove(key1);

    // put key2 then remove key1 500 times
    cache.put(key2, value2);
    for(int j = 0; j < 500; j++)
        cache.remove(key1);
    EXPECT_EQ(cache.get(key1, value1), string_local_cache::MISS);
    EXPECT_EQ(cache.get(key2, value2), string_local_cache::HIT);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

/*TEST_F(string_local_cache_test, test18_setexpire_ne_2)
{
    cache.clear();
    const size_t cap = 1;
    
    cache.set_capacity(cap);
    cache.set_expire(1000);

    string_local_cache::result res = cache.set_expire(-2);
    EXPECT_EQ(string_local_cache::SETERROR, res);

    // put key then get
    string key = env->random_string(KEY_LEN);
    string value = env->random_string(VAL_LEN);
    cache.put(key, value);
    EXPECT_EQ(cache.get(key, value), string_local_cache::HIT);

    usleep(1000 * 1000);
    EXPECT_EQ(cache.get(key, value), string_local_cache::EXPIRED);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}*/

TEST_F(string_local_cache_test, test19_setexpire_0)
{
    cache.clear();
    const size_t cap = 1;

    cache.set_capacity(cap);
    cache.set_expire(1000);

    string_local_cache::result res = cache.set_expire(0);
    EXPECT_EQ(string_local_cache::SETERROR, res);

    // put key then get
    string key = env->random_string(KEY_LEN);
    string value = env->random_string(VAL_LEN);
    cache.put(key, value);
    EXPECT_EQ(cache.get(key, value), string_local_cache::HIT);

    usleep(1100 * 1000);
    EXPECT_EQ(cache.get(key, value), string_local_cache::EXPIRED);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test20_setexpire_normal)
{
    cache.clear();
    const size_t cap = 1;

    cache.set_capacity(cap);
    cache.set_expire(500);
    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    // put key then get
    string key = env->random_string(KEY_LEN);
    string value = env->random_string(VAL_LEN);
    cache.put(key, value);
    EXPECT_EQ(cache.get(key, value), string_local_cache::HIT);

    cache.set_expire(1000);
    usleep(600 * 1000);
    EXPECT_EQ(cache.get(key, value), string_local_cache::HIT);

    usleep(400 * 1000);
    EXPECT_EQ(cache.get(key, value), string_local_cache::EXPIRED);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test21_refresh_start_time)
{
    cache.clear();
    const size_t cap = 1;

    cache.set_capacity(cap);
    cache.set_expire(1000);
    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    string keys[20];
    string values[20];
    string key = "";
    string value = "";
    for (int i = 0; i < 20; i++) {
        keys[i] = "";
        values[i] = "";
    }
    // put 20 keys into cache
    for (int j = 0; j < 20; j++) {
        key = env->random_string(KEY_LEN);
        value = env->random_string(VAL_LEN);
        cache.put(key, value);
        keys[j] = key;
        values[j] = value;
        usleep(100 * 1000);
    }

    for(int j = 0; j < 19; j++)
        EXPECT_EQ(cache.get(keys[j], values[j]), string_local_cache::MISS);
    EXPECT_EQ(cache.get(keys[19], values[19]), string_local_cache::HIT);

    usleep(500 * 1000);
    EXPECT_EQ(cache.get(keys[19], values[19]), string_local_cache::HIT);
    usleep(500 * 1000);
    EXPECT_EQ(cache.get(keys[19], values[19]), string_local_cache::EXPIRED);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test22_evict_one_normal)
{
    cache.clear();
    const size_t cap = 10;
    cache.set_capacity(cap);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(0), cache.size());
    EXPECT_EQ(static_cast<size_t>(0), cache.lru_size());

    string keys[10];
    string values[10];
    string key = "";
    string value = "";
    for (int i = 0; i < 10; i++) {
        keys[i] = "";
        values[i] = "";
    }
    // put 10 keys into cache
    for (int j = 0; j < 10; j++) {
        key = env->random_string(KEY_LEN);
        value = env->random_string(VAL_LEN);
        cache.put(key, value);
        keys[j] = key;
        values[j] = value;
    }

    for(int j = 0; j < 10; j++)
        EXPECT_EQ(cache.get(keys[j], values[j]), string_local_cache::HIT);

    const string_local_cache_testable::internal_entry *res = cache.evict_one();
    EXPECT_EQ(keys[0], res->key);
    EXPECT_EQ(values[0], res->value);
    delete res;

    EXPECT_EQ(cache.get(keys[0], values[0]), string_local_cache::MISS);
    for(int k = 1; k < 10; k++)
        EXPECT_EQ(cache.get(keys[k], values[k]), string_local_cache::HIT);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(static_cast<size_t>(9), cache.size());
    EXPECT_EQ(static_cast<size_t>(9), cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test23_evict_one_cap_0)
{
    cache.clear();
    const size_t cap = 0;
    cache.set_capacity(cap);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    string key = env->random_string(KEY_LEN);
    string value = env->random_string(VAL_LEN);
    cache.put(key, value);
    EXPECT_EQ(cache.get(key, value), string_local_cache::MISS);

    const string_local_cache_testable::internal_entry *res = cache.evict_one();
    EXPECT_EQ(NULL, res);

    EXPECT_EQ(cache.get(key, value), string_local_cache::MISS);

    EXPECT_EQ(cap, cache.get_capacity());
    EXPECT_EQ(cap, cache.size());
    EXPECT_EQ(cap, cache.lru_size());

    cache.clear();
}

TEST_F(string_local_cache_test, test24_put_get) 
{
    string_local_cache::result res;
    string test_key = env->random_string(KEY_LEN);
    string test_val = env->random_string(VAL_LEN);
    string value;
      
    cache.put(test_key, test_val);
    ASSERT_EQ(cache.size(), static_cast<size_t>(1));
    ASSERT_EQ(cache.lru_size(), cache.size());

    value.clear();
    res = cache.get(test_key, value);
}
