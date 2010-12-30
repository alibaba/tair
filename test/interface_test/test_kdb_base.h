#include <gtest/gtest.h>
#include <tbsys.h>
#include "data_entry.hpp"
#include <vector>

#define TEST_DATA_COUNT 100

using namespace tair::common; 

class TestKdbBase: public ::testing::Test
{
  public:
    TestKdbBase()
    {
      if(TBSYS_CONFIG.load("kdb_test.conf") == EXIT_FAILURE){
        TBSYS_LOG(ERROR, "read config file error: %s", "kdb_test.conf");
        exit(0);
      }
    }

    ~TestKdbBase()
    {
    }

    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }

    static bool compareDataValue(const data_entry& v1, const data_entry& v2)
    {
      if (v1.get_size() != v2.get_size()) return false;
      return memcmp(v1.get_data(), v2.get_data(), v1.get_size()) == 0;
    }

    static bool compareDataValueWithMeta(const data_entry& v1, const data_entry& v2)
    {
      if (compareDataValue(v1, v2) == false) return false;
      return memcmp(&v1.data_meta, &v2.data_meta, sizeof(v1.data_meta));
    }
    
    static void print(data_entry &data) 
    {
      char * temp = data.get_data();
      int i = (int)temp[0] + ((int)temp[1])*10;
      printf("area:%.3d\tdata:%s\n", i, temp+2);
    }

    static void print(tair::item_data_info &item)
    {
      char * key = (char *)malloc(item.header.keysize+1);
      memcpy(key, item.m_data, item.header.keysize);
      key[item.header.keysize] = '\0';
      char * value = (char *)malloc(item.header.valsize+1);
      memcpy(value, item.m_data + item.header.keysize, item.header.valsize);
      value[item.header.valsize] = '\0';
      int karea = (int)key[0] + ((int)key[1])*10;
      int varea = (int)value[0] + ((int)value[1])*10;
      printf("key:[%.3d]%s\tvalue:[%.3d]%s\n", karea, key+2, varea, value+2);
      free(key);
      free(value);
    }

  protected:
};

class TestKdbData
{
  public:
    TestKdbData()
    {
      set_test_data();
    }

    void set_test_data()
    {
      char data1[] = "hello_kdb_key";
      char data2[] = "hello_kdb_value";
      for(int i = 0; i < TEST_DATA_COUNT; i++) {
        keys[i].set_data(data1, strlen(data1),true);
        keys[i].merge_area(i);
        values[i].set_data(data2, strlen(data2), true);
        values[i].merge_area(i);
      }
      for(int i = 0; i < 5; i++) {
        buckets.push_back(i);
      }
    }

    data_entry * get_test_key(int index)
    {
      return keys + index;
    }

    data_entry * get_test_value(int index)
    {
      return values + index;
    }

    std::vector<int> get_buckets()
    {
      return buckets;
    }
 
 private:
    data_entry keys[TEST_DATA_COUNT];
    data_entry values[TEST_DATA_COUNT];
    std::vector<int> buckets;
};

