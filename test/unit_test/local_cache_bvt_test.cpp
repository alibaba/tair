#include <gtest/gtest.h>
#include <stdarg.h>
#include "tbsys.h"

#include "tair_client_api.hpp"
#include "tair_client_api_impl.hpp"
#include "test_environment.h"
#include "query_info_packet.hpp"

using namespace std;
using namespace tair;
using namespace tair::common;

TestEnvironment* const env = dynamic_cast<TestEnvironment * const>(
    testing::AddGlobalTestEnvironment(new TestEnvironment));

class local_cache_bvt_test : public ::testing::Test 
{
public:
  local_cache_bvt_test() : client(NULL), client2(NULL), internal(NULL)
  {
  }


  virtual ~local_cache_bvt_test()
  {
  }

protected:

  virtual void SetUp()
  {
    tbsys::CLogger::_logger._level = -1;
    assert(client == NULL);
    assert(client2 == NULL);
    if(config.load("tair.conf") == EXIT_FAILURE) {
      return ;
    }

    master = config.getString("tair", "master", "127.0.0.1:5198");
    slave = config.getString("tair", "slave", "");
    group = config.getString("tair", "group", "group_1");
    client = new tair_client_api();
    client2 = new tair_client_api();
    if (false == client->startup(
                          master.c_str(), 
                          slave.c_str(), 
                          group.c_str())) {
      delete client;
      client = NULL;
      return ;
    }
    if (false == client2->startup(
                          master.c_str(),
                          slave.c_str(),
                          group.c_str())) {
      delete client2;
      client2 = NULL;
      return ;
    }
//    client->setup_cache(AREA, 1024);

    if (internal != NULL) 
      return;
    internal = new tair_client_impl();
    if (false == 
        internal->startup(master.c_str(), slave.c_str(), group.c_str())) {
      delete internal;
      internal = NULL;
      return;
    }
  }

  virtual void TearDown() 
  {
    if (client != NULL) {
      client->close();
      delete client; 
      client = NULL;
    }

    if (client2 != NULL) {
      client2->close();
      delete client2;
      client2 = NULL;
    }

    if (internal != NULL) {
      internal->close();
      delete internal;
      internal = NULL;
    }
  }

  void get_stat_info(map<string, string> &out) 
  {
    internal->query_from_configserver(request_query_info::Q_STAT_INFO, group, out, 0);
  }
      

  tair_client_api *client; 
  tair_client_api *client2;
  tair_client_impl *internal;

  const static int KEY_LEN = 64;
  const static int VALUE_LEN = 1024;
  const static int AREA = 0;
  const static int EXPIRE = 300;

protected:
  tbsys::CConfig config;
private:
  string master;
  string slave;
  string group;
};

TEST_F(local_cache_bvt_test, test01_connet_tc)
{
  ASSERT_NE(config.load("tair.conf"), EXIT_FAILURE);

  string master = config.getString("tair", "master", "127.0.0.1:5198");
  string slave = config.getString("tair", "slave", "");
  string group = config.getString("tair", "group", "group_1");
  tair_client_api client;
  bool startup = client.startup(
        master.c_str(), 
        slave.c_str(), 
        group.c_str()); 
  ASSERT_EQ(startup, true);
  client.setup_cache(AREA, 1023);
  client.close();
}

TEST_F(local_cache_bvt_test, test02_get_with_local_cache_tc)
{ 
  client->setup_cache(AREA, 1024);
  ASSERT_NE(client, (void *)(NULL));
  data_entry test_key = env->random_data_entry(64);
  data_entry test_value = env->random_data_entry(VALUE_LEN);
  int ret = 0;
  ret = client->put(0, test_key, test_value, 0, 0);
  ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
  ret = client->put(1, test_key, test_value, 0, 0);
  ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);

  int start = tbutil::Time::now().toSeconds();
  while (tbutil::Time::now().toSeconds() - start < 10) 
  {
    {
      data_entry *value = NULL;
      ret = client->get(0, test_key, value);
      EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
      EXPECT_EQ(*value, test_value);
      delete value;
    }
    {
      data_entry *value = NULL;
      ret = client->get(1, test_key, value);
      EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
      EXPECT_EQ(*value, test_value);
      delete value;
    }
  }
  sleep(3);
  
  map<string, string> out; 
  get_stat_info(out);
  string n0_count = out["0 getCount"];
  string n1_count = out["1 getCount"];

  EXPECT_LT(atoi(n0_count.c_str()), 5 * (1000 + EXPIRE) / EXPIRE);
  EXPECT_GT(atoi(n1_count.c_str()), atoi(n0_count.c_str()) * 10);

  test_value = env->random_data_entry(KEY_LEN);
  ret = client->put(0, test_key, test_value, 0, 0);
  ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
  data_entry *value = NULL;
  ret = client->get(0, test_key, value);
  EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
  EXPECT_EQ(*value, test_value);
  delete value;
}

TEST_F(local_cache_bvt_test, test03_namespace_ne_1_1024)
{
    for (int i = -1; i < 1025; i += 1025)
    {
        client->setup_cache(i, 1024);
        ASSERT_NE(client, (void *)(NULL));
        data_entry test_key = env->random_data_entry(KEY_LEN);
        data_entry test_value = env->random_data_entry(VALUE_LEN);
        int ret = 0;
        ret = client->put(0, test_key, test_value, 0, 0);
        ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
        ret = client->put(i, test_key, test_value, 0, 0);
        ASSERT_EQ(ret, TAIR_RETURN_INVALID_ARGUMENT);

        int start = tbutil::Time::now().toSeconds();
        while (tbutil::Time::now().toSeconds() - start < 5)
        {
            {
                data_entry *value = NULL;
                ret = client->get(0, test_key, value);
                ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
                EXPECT_EQ(*value, test_value);
                delete value;
            }
            {
                data_entry *value = NULL;
                ret = client->get(i, test_key, value);
                ASSERT_EQ(ret, TAIR_RETURN_INVALID_ARGUMENT);
                delete value;
            }
        }
//        sleep(3);

//        map<string, string> out;
//        get_stat_info(out);
//        string n0_count = out["0 getCount"];
//        string n1_count = out["1 getCount"];

//        ASSERT_EQ(atoi(n0_count.c_str()), atoi(n1_count.c_str()));
    }
}

TEST_F(local_cache_bvt_test, test04_namespace_1023)
{
    client->setup_cache(1023, 1024);
    ASSERT_NE(client, (void *)(NULL));
    data_entry test_key = env->random_data_entry(KEY_LEN);
    data_entry test_value = env->random_data_entry(VALUE_LEN);
    int ret = 0;
    ret = client->put(1023, test_key, test_value, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(1, test_key, test_value, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);

    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 5)
    {
        {
            data_entry *value = NULL;
            ret = client->get(1023, test_key, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value);
            delete value;
        }
    }
    sleep(3);

    map<string, string> out;
    get_stat_info(out);
    string n0_count = out["1023 getCount"];
    string n1_count = out["1 getCount"];

    EXPECT_LT(atoi(n0_count.c_str()), 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(atoi(n1_count.c_str()), atoi(n0_count.c_str()) * 10);

    test_value = env->random_data_entry(KEY_LEN);
    ret = client->put(1023, test_key, test_value, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    data_entry *value = NULL;
    ret = client->get(1023, test_key, value);
    EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
    EXPECT_EQ(*value, test_value);
    delete value;
}

TEST_F(local_cache_bvt_test, test05_capability_0)
{
    client->setup_cache(AREA, 0);
    ASSERT_NE(client, (void *)(NULL));
    data_entry test_key = env->random_data_entry(KEY_LEN);
    data_entry test_value = env->random_data_entry(VALUE_LEN);
    int ret = 0;
    ret = client->put(0, test_key, test_value, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(1, test_key, test_value, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);

    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 5)
    {
        {
            data_entry *value = NULL;
            ret = client->get(0, test_key, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value);
            delete value;
        }
    }
    sleep(5);

    map<string, string> out;
    get_stat_info(out);
    string n0_count = out["0 getCount"];
    string n1_count = out["1 getCount"];

    ASSERT_EQ(atoi(n0_count.c_str()), atoi(n1_count.c_str()));
}

TEST_F(local_cache_bvt_test, test06_capability_1)
{
    client->setup_cache(AREA, 1);

    ASSERT_NE(client, (void *)(NULL));
    data_entry test_key1 = env->random_data_entry(KEY_LEN);
    data_entry test_value1 = env->random_data_entry(VALUE_LEN);
    data_entry test_key2 = env->random_data_entry(KEY_LEN);
    data_entry test_value2 = env->random_data_entry(VALUE_LEN);
    int ret = 0;
    ret = client->put(0, test_key1, test_value1, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(0, test_key2, test_value2, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(1, test_key1, test_value1, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(1, test_key2, test_value2, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);

    // get kv1 in 5s
    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 10)
    {
        {
            data_entry *value = NULL;
            ret = client->get(0, test_key1, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value1);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key1, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value1);
            delete value;
        }
    }
    sleep(3);

    map<string, string> out;
    get_stat_info(out);
    int n0_old = atoi(out["0 getCount"].c_str());
    int n1_old = atoi(out["1 getCount"].c_str());

    EXPECT_LT(n0_old, 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(n1_old, n0_old * 10);
    sleep(5);

    // get kv1 and kv2 in 5s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 10)
    {
        {
            data_entry *value = NULL;
            ret = client->get(0, test_key2, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value2);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(0, test_key1, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value1);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key2, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value2);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key1, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value1);
            delete value;
        }
    }
    sleep(4);

    get_stat_info(out);
    int n0_now = atoi(out["0 getCount"].c_str());
    int n1_now = atoi(out["1 getCount"].c_str());
    EXPECT_LT(n1_now, n0_now + 10);
    EXPECT_GT(n1_now, n0_now - 10);
//    EXPECT_EQ(n1_now-n1_old, n0_now-n0_old+10);

    test_value1 = env->random_data_entry(KEY_LEN);
    ret = client->put(0, test_key1, test_value1, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    data_entry *value = NULL;
    ret = client->get(0, test_key1, value);
    EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
    EXPECT_EQ(*value, test_value1);
    delete value;
    value=NULL;

    test_value1 = env->random_data_entry(KEY_LEN);
    ret = client->put(1, test_key1, test_value1, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
//    data_entry *value = NULL;
    ret = client->get(1, test_key1, value);
    EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
    EXPECT_EQ(*value, test_value1);
    delete value;
}

TEST_F(local_cache_bvt_test, test07_capability_1_lru)
{
    client->setup_cache(AREA, 1);

    ASSERT_NE(client, (void *)(NULL));
    data_entry test_key1 = env->random_data_entry(KEY_LEN);
    data_entry test_value1 = env->random_data_entry(VALUE_LEN);
    data_entry test_key2 = env->random_data_entry(KEY_LEN);
    data_entry test_value2 = env->random_data_entry(VALUE_LEN);
    int ret = 0;
    ret = client->put(0, test_key1, test_value1, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(0, test_key2, test_value2, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(1, test_key1, test_value1, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
    ret = client->put(1, test_key2, test_value2, 0, 0);
    ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);

    // get kv1 in 5s
    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 10)
    {
        {
            data_entry *value = NULL;
            ret = client->get(0, test_key1, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value1);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key1, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value1);
            delete value;
        }
    }
    sleep(3);

    map<string, string> out;
    get_stat_info(out);
    int n0_old = atoi(out["0 getCount"].c_str());
    int n1_old = atoi(out["1 getCount"].c_str());
    EXPECT_LT(n0_old, 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(n1_old, n0_old * 10);
    sleep(5);

    // get kv1 and kv2 in 5s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 10)
    {
        {
            data_entry *value = NULL;
            ret = client->get(0, test_key2, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value2);
            delete value;
        }
        {
            data_entry *value = NULL;
            ret = client->get(1, test_key2, value);
            EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, test_value2);
            delete value;
        }
    }
    sleep(3);

    get_stat_info(out);
    int n0_now = atoi(out["0 getCount"].c_str());
    int n1_now = atoi(out["1 getCount"].c_str());
    EXPECT_LT(n0_now, 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(n1_now, (n0_now) * 10);
}

TEST_F(local_cache_bvt_test, test08_capability_enough)
{
    client->setup_cache(AREA, 10);
    client->get_local_cache(AREA)->set_expire(60000);
    ASSERT_NE(client, (void *)(NULL));

    // prepare 1000 keys into area 0 & 1
    data_entry keys[1000];
    data_entry vals[1000];
    for(int i = 0; i < 1000; i++)
    {
        data_entry test_key = env->random_data_entry(KEY_LEN);
        data_entry test_value = env->random_data_entry(VALUE_LEN);
        ASSERT_EQ(client->put(0, test_key, test_value, 0, 0), TAIR_RETURN_SUCCESS);
        ASSERT_EQ(client->put(1, test_key, test_value, 0, 0), TAIR_RETURN_SUCCESS);
        keys[i] = test_key;
        vals[i] = test_value;
    }

    // get 5 keys in 5s
    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 5)
    {
        for(int j = 0; j < 5; j++)
        {
            data_entry *value = NULL;
            EXPECT_EQ(client->get(0, keys[j], value), TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, vals[j]);
            delete value;

//            data_entry *value = NULL;
            EXPECT_EQ(client->get(1, keys[j], value), TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, vals[j]);
            delete value;
        }
    }
    sleep(3);

    map<string, string> out;
    get_stat_info(out);
    int n0_1st = atoi(out["0 getCount"].c_str());
    int n1_1st = atoi(out["1 getCount"].c_str());
    EXPECT_LT(n0_1st, 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(n1_1st, n0_1st * 10);
    sleep(5);

    // get another 5 keys in 5s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 5) {
        for (int k = 5; k < 10; k++) {
            data_entry *value = NULL;
            EXPECT_EQ(client->get(0, keys[k], value), TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, vals[k]);
            delete value;

//            data_entry *value = NULL;
            EXPECT_EQ(client->get(1, keys[k], value), TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, vals[k]);
            delete value;
        }
    }
    sleep(3);

    get_stat_info(out);
    int n0_2ed = atoi(out["0 getCount"].c_str());
    int n1_2ed = atoi(out["1 getCount"].c_str());
    EXPECT_LT(n0_2ed, 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(n1_2ed, (n0_2ed) * 10);

    sleep(5);
    // get 10 keys in 5s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 10) {
        for (int t = 0; t < 10; t++) {
            data_entry *value = NULL;
            EXPECT_EQ(client->get(0, keys[t], value), TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, vals[t]);
            delete value;

//            data_entry *value = NULL;
            EXPECT_EQ(client->get(1, keys[t], value), TAIR_RETURN_SUCCESS);
            EXPECT_EQ(*value, vals[t]);
            delete value;
        }
    }
    sleep(3);

    get_stat_info(out);
    int n0_3rd = atoi(out["0 getCount"].c_str());
    int n1_3rd = atoi(out["1 getCount"].c_str());
    EXPECT_LT(n0_3rd, 5 * (1000 + EXPIRE) / EXPIRE);
    EXPECT_GT(n1_3rd, (n0_3rd) * 10);
}

TEST_F(local_cache_bvt_test, test09_change_key_in_another_client)
{
    client->setup_cache(AREA, 10);
    client2->setup_cache(AREA, 10);
    client->get_local_cache(AREA)->set_expire(5000);
    client2->get_local_cache(AREA)->set_expire(5000);
    ASSERT_NE(client, (void *)(NULL));
    ASSERT_NE(client2, (void *)(NULL));

    // put 1 into area 0 & 1
    data_entry key = env->random_data_entry(KEY_LEN);
    data_entry value1 = env->random_data_entry(VALUE_LEN);
    data_entry value2 = env->random_data_entry(VALUE_LEN);
    ASSERT_EQ(client->put(0, key, value1, 0, 0), TAIR_RETURN_SUCCESS);
    ASSERT_EQ(client->put(1, key, value1, 0, 0), TAIR_RETURN_SUCCESS);

    // get value1 3s
    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 3)
    {
        data_entry *value = NULL;
        ASSERT_EQ(client->get(0, key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, value1);
        if(value != NULL)
            delete value;
        ASSERT_EQ(client2->get(0, key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, value1);
        delete value;
    }

    // change value1 to value2 by client2
    ASSERT_EQ(client2->put(0, key, value2, 0, 0), TAIR_RETURN_SUCCESS);

    // get value2 2s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 2)
    {
        data_entry *value = NULL;
        ASSERT_EQ(client->get(0, key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, value1);
        if(value != NULL)
            delete value;
        ASSERT_EQ(client2->get(0, key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, value2);
        delete value;
    }

    sleep(1);
    data_entry *value = NULL;
    ASSERT_EQ(client->get(0, key, value), TAIR_RETURN_SUCCESS);
    EXPECT_EQ(*value, value2);
    if (value != NULL) {
        delete value;
        value = NULL;
    }
    ASSERT_EQ(client2->get(0, key, value), TAIR_RETURN_SUCCESS);
    EXPECT_EQ(*value, value2);
    delete value;
}

TEST_F(local_cache_bvt_test, test10_change_key_expire_in_another_client)
{
    client->setup_cache(AREA, 10);
    client2->setup_cache(AREA, 10);
    client->get_local_cache(AREA)->set_expire(5000);
    client2->get_local_cache(AREA)->set_expire(5000);
    ASSERT_NE(client, (void *)(NULL));
    ASSERT_NE(client2, (void *)(NULL));

    // put 1 key into area 0
    data_entry test_key = env->random_data_entry(KEY_LEN);
    data_entry test_value = env->random_data_entry(VALUE_LEN);
    ASSERT_EQ(client->put(0, test_key, test_value, 0, 0), TAIR_RETURN_SUCCESS);

    // get key 2s
    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 2)
    {
        data_entry *value = NULL;
        ASSERT_EQ(client->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, test_value);
        if (value != NULL) {
            delete value;
            value = NULL;
        }
        ASSERT_EQ(client2->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, test_value);
        if (value != NULL)
            delete value;
    }

    // change expire to 1 by client2
    ASSERT_EQ(client2->put(0, test_key, test_value, 1, 0), TAIR_RETURN_SUCCESS);
    usleep(1000);

    // get key 3s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 2)
    {
        data_entry *value = NULL;
        ASSERT_EQ(client->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, test_value);
        if (value != NULL)
        {
            delete value;
            value = NULL;
        }
        ASSERT_EQ(client2->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        if (value != NULL)
            delete value;
    }

    sleep(7);
    data_entry *value = NULL;
    int ret = client->get(0, test_key, value);
    EXPECT_TRUE(ret == TAIR_RETURN_DATA_NOT_EXIST || ret == TAIR_RETURN_DATA_EXPIRED);
    ret = client2->get(0, test_key, value);
    EXPECT_TRUE(ret == TAIR_RETURN_DATA_NOT_EXIST || ret == TAIR_RETURN_DATA_EXPIRED);
    delete value;
}

TEST_F(local_cache_bvt_test, test11_delete_key_in_another_client)
{
    client->setup_cache(AREA, 10);
    client2->setup_cache(AREA, 10);
    client->get_local_cache(AREA)->set_expire(5000);
    client2->get_local_cache(AREA)->set_expire(5000);
    ASSERT_NE(client, (void *)(NULL));
    ASSERT_NE(client2, (void *)(NULL));

    // put 1 into area 0 & 1
    data_entry test_key = env->random_data_entry(KEY_LEN);
    data_entry test_value = env->random_data_entry(VALUE_LEN);
    ASSERT_EQ(client->put(0, test_key, test_value, 0, 0), TAIR_RETURN_SUCCESS);
    ASSERT_EQ(client->put(1, test_key, test_value, 0, 0), TAIR_RETURN_SUCCESS);

    // get key 2s
    int start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 2)
    {
        data_entry *value = NULL;
        ASSERT_EQ(client->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, test_value);
        if (value != NULL)
        {
            delete value;
            value = NULL;
        }
        ASSERT_EQ(client2->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, test_value);
        if (value != NULL)
            delete value;
    }

    // delete key by client2
    ASSERT_EQ(client2->remove(0, test_key), TAIR_RETURN_SUCCESS);

    // get key 3s
    start = tbutil::Time::now().toSeconds();
    while (tbutil::Time::now().toSeconds() - start < 3)
    {
        data_entry *value = NULL;
        ASSERT_EQ(client->get(0, test_key, value), TAIR_RETURN_SUCCESS);
        EXPECT_EQ(*value, test_value);
        if (value != NULL)
        {
            delete value;
            value = NULL;
        }
        ASSERT_EQ(client2->get(0, test_key, value), TAIR_RETURN_DATA_NOT_EXIST);
        delete value;
    }

    sleep(1);
    data_entry *value = NULL;
    ASSERT_EQ(client->get(0, test_key, value), TAIR_RETURN_DATA_NOT_EXIST);
    ASSERT_EQ(client2->get(0, test_key, value), TAIR_RETURN_DATA_NOT_EXIST);
    delete value;
}


