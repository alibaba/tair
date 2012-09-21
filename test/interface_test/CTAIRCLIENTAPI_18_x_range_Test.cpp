#include "interface_test_base.h" 
namespace tair{ 
  void mPrefixPut(tair_client_api &client_handle, int area, int start, int end, data_entry &pkey, data_entry &value)
  {
    int ret; 
    char buf[10]; 
    for(int i = start; i < end; i++)
    {
      sprintf(buf, "%09d", i); 
      data_entry skey(buf); 
      DO_WITH_RETRY(client_handle.prefix_put(area, pkey, skey, value, 0, 0), ret, TAIR_RETURN_SUCCESS); 
      ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    }
  }
  
  void mPrefixRemove(tair_client_api &client_handle, int area, int start, int end, data_entry &pkey) 
  {
    int ret; 
    char buf[10]; 
    for(int i = start; i < end; i++)
    {
      sprintf(buf, "%09d", i); 
      data_entry skey(buf); 
      DO_WITH_RETRY(client_handle.prefix_remove(area, pkey, skey), ret, TAIR_RETURN_SUCCESS); 
      EXPECT_EQ(TAIR_RETURN_SUCCESS, ret); 
    }
  } 
  TEST_F(CTestInterfaceBase, GetRangeTestBase) { 
    data_entry value1("1_value_1"); 
    data_entry pkey("18_01_data_01"); 
    int area = 9; 
    int ret; 
    std::vector<data_entry*> res; 

    char buf[10]; 
    for(int i = 0; i < 100; i++)
    {
      sprintf(buf, "%09d", i); 
      data_entry skey(buf); 
      DO_WITH_RETRY(client_handle.prefix_put(area, pkey, skey, value1, 0, 0), ret, TAIR_RETURN_SUCCESS); 
      ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    }

    ret = client_handle.get_range(area, pkey, "", "", 0, 100, res);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(200, res.size()); 
    res.clear();

    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 100, res), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(200, res.size()); 
    res.clear();

    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 100, res, CMD_RANGE_VALUE_ONLY), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(100, res.size()); 
    res.clear();

    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 100, res, CMD_RANGE_KEY_ONLY), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(100, res.size()); 
    res.clear();

    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(200, res.size()); 
    res.clear();

    for(int i = 0; i < 50; i++)
    {
      sprintf(buf, "%09d", i); 
      data_entry skey(buf); 
      DO_WITH_RETRY(client_handle.prefix_remove(area, pkey, skey), ret, TAIR_RETURN_SUCCESS); 
      ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    }
 
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 100, res), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(100, res.size()); 
    res.clear();

    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 100, res, 2), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(50, res.size()); 
    res.clear();

    for(int i = 50; i < 100; i++)
    {
      sprintf(buf, "%09d", i); 
      data_entry skey(buf); 
      DO_WITH_RETRY(client_handle.prefix_remove(area, pkey, skey), ret, TAIR_RETURN_SUCCESS); 
      ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    }
 

    ret = client_handle.get_range(area, pkey, "", "", 0, 0, res);
    EXPECT_EQ(0, res.size()); 
    EXPECT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret); 
    res.clear();
  } 

  TEST_F(CTestInterfaceBase, GetRangeTestLimit) { 
    int area = 8; 
    int ret; 
    data_entry value1("1_value_1"); 
    data_entry pkey("18_01_data_02"); 
    std::vector<data_entry*> res; 
 
    mPrefixPut(client_handle, area, 0, 2000, pkey, value1);

    //limit ==0 (use default limit)
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(2000, res.size()); 
    res.clear();

    //limit <0
    ret = client_handle.get_range(area, pkey, "", "", 0, -1, res);
    EXPECT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret); 

    //offset <0
    ret = client_handle.get_range(area, pkey, "", "", -1, 0, res);
    EXPECT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret); 

    //offset > limit
    ret = client_handle.get_range(area, pkey, "", "", 3000, 2000, res);
    EXPECT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret); 

    //end < start 
    ret = client_handle.get_range(area, pkey, "5", "4", 0, 0, res);
    EXPECT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret); 

    mPrefixRemove(client_handle, area, 0, 2000, pkey);

    //has more data
    char lvalue[5000];
    for (int i = 0; i < 4999; i++)
      lvalue[i] = '1';
    lvalue[4999]='\0';
    data_entry value2(lvalue); 

    mPrefixPut(client_handle, area, 0, 500, pkey, value2);
    
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_HAS_MORE_DATA, ret); 
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res, 2), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_HAS_MORE_DATA, ret); 
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res, 3), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    
    mPrefixRemove(client_handle, area, 0, 500, pkey);
  }
  
  TEST_F(CTestInterfaceBase, GetRangeTestKeyValue) 
  { 
    int area = 8; 
    int ret; 
    data_entry value1("1_value_1"); 
    data_entry pkey("18_01_data_03"); 
    std::vector<data_entry*> res; 
 
    mPrefixPut(client_handle, area, 0, 2000, pkey, value1);

    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(2000, res.size()); 
    for(size_t i = 0; i < res.size(); i+=2)
    {
      fprintf(stderr,"key:%s, value:%s ",res[i]->get_printable_key().c_str(), res[i+1]->get_printable_key().c_str());  
    }
    res.clear();
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res, CMD_RANGE_KEY_ONLY), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(1000, res.size()); 
    for(size_t i = 0; i < res.size(); i++)
    {
      fprintf(stderr,"key:%s ",res[i]->get_printable_key().c_str());  
    }
    res.clear();
    DO_WITH_RETRY(client_handle.get_range(area, pkey, "", "", 0, 0, res, CMD_RANGE_VALUE_ONLY), ret, TAIR_RETURN_SUCCESS); 
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret); 
    EXPECT_EQ(1000, res.size()); 
    for(size_t i = 0; i < res.size(); i++)
    {
      fprintf(stderr,"value:%s ",res[i]->get_printable_key().c_str());  
    }
    res.clear();

    mPrefixRemove(client_handle, area, 0, 2000, pkey);
  }
}
