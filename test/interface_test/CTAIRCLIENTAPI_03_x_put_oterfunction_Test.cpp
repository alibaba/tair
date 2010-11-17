#include "interface_test_base.h"
namespace tair{
	 TEST_F(CTestInterfaceBase, PutTestCase_01) {
	 	data_entry key1("03_01_key_01");
        data_entry value1("1");
        data_entry value2("03_01_data_02");
        int area = 8;
        int ret;
        data_entry* p;

        //step1
		DO_WITH_RETRY(client_handle.put(area, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        vector<int> value;
        for(int i=0;i<10;++i){
            value.push_back(i);
        }

        //step2
        vector<int>::iterator start = value.begin();
        vector<int>::iterator end = value.end();
        ret = client_handle.add_items(area,key1,value,0,0);
        ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE, ret);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step3
        vector<int> result;
        ret = client_handle.get_items(area,key1,0,tair_client_api::ALL_ITEMS,result);
        ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH, ret);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step4
        ret = client_handle.get_and_remove(area,key1,0,1,result);
        ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH, ret);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step5
        ret = client_handle.remove_items(area,key1,0,1);
        ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH, ret);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step6
        ret = client_handle.get_items_count(area,key1);
        ASSERT_GT(0, ret);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step7
        int ret_value=99;
        ret = client_handle.incr(area,key1,10,&ret_value);
        ASSERT_GT(0, ret);
        ASSERT_EQ(99, ret_value);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step8
        ret_value=99;
        ret = client_handle.decr(area,key1,10,&ret_value);
        ASSERT_GT(0, ret);
        ASSERT_EQ(99, ret_value);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value1, *p));
		delete p;
		p = NULL;

        //step9
		DO_WITH_RETRY(client_handle.put(area, key1, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value2, *p));
		delete p;
		p = NULL;

        //step10
		DO_WITH_RETRY(client_handle.get(area, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value2, *p));
		delete p;
		p = NULL;

        //step11
        ret = client_handle.remove(area, key1);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ret = client_handle.get(area, key1, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
        

     }
}
