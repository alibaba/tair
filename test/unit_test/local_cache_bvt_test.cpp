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
  local_cache_bvt_test() : client(NULL), internal(NULL)
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
    if(config.load("tair.conf") == EXIT_FAILURE) {
      return ;
    }

    master = config.getString("tair", "master", "127.0.0.1:5198");
    slave = config.getString("tair", "slave", "");
    group = config.getString("tair", "group", "group_1");
    client = new tair_client_api();
    if (false == client->startup(
                          master.c_str(), 
                          slave.c_str(), 
                          group.c_str())) {
      delete client;
      client = NULL;
      return ;
    }
    client->setup_cache(AREA, 1024, EXPIRE);

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
    if (client == NULL)
      return;
    client->close();
    delete client; 
    client = NULL;

    if (internal == NULL)
      return ;
    internal->close();
    delete internal;
    internal = NULL;
  }

  void get_stat_info(map<string, string> &out) 
  {
    internal->query_from_configserver(request_query_info::Q_STAT_INFO, group, out, 0);
  }
      

  tair_client_api *client; 
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

TEST_F(local_cache_bvt_test, test_connet_tc)
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
  client.setup_cache(AREA, 1024, 300);
  client.close();
}

TEST_F(local_cache_bvt_test, get_with_local_cache_tc)
{ 
  ASSERT_NE(client, (void *)(NULL));
  data_entry test_key = env->random_data_entry(64);
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
  sleep(3);
  
  map<string, string> out; 
  get_stat_info(out);
  string n0_count = out["0 getCount"];
  string n1_count = out["1 getCount"];

  ASSERT_LT(atoi(n0_count.c_str()), 5 * (1000 + EXPIRE) / EXPIRE);
  ASSERT_GT(atoi(n1_count.c_str()), atoi(n0_count.c_str()) * 10);

  test_value = env->random_data_entry(KEY_LEN);
  ret = client->put(0, test_key, test_value, 0, 0);
  ASSERT_EQ(ret, TAIR_RETURN_SUCCESS);
  data_entry *value = NULL;
  ret = client->get(0, test_key, value);
  EXPECT_EQ(ret, TAIR_RETURN_SUCCESS);
  EXPECT_EQ(*value, test_value);
  delete value;
}

