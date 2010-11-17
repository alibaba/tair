#include "interface_test_base.h"
namespace tair{
	 TEST_F(CTestInterfaceBase, RemoveTestCase_01_dateNotExist) {
	 	data_entry key1("05_01_key_01");
        data_entry value1("05_01_data_01");
        
        data_entry key2("05_01_key_02");
        data_entry value2("05_01_data_02");

        data_entry* p;
        int ret = 0;
		//data1:area equals,key not equal
		DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		
		//data2:area not equas, key equal
		DO_WITH_RETRY(client_handle.put(2, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		ret = client_handle.remove(1, key2);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);

		ret = client_handle.get(1, key1, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value1, *p));
        delete p;
        p = NULL;
		
		ret = client_handle.get(2, key2, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value2, *p));
        delete p;
		p = NULL;

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);

     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_02_areaNegative) {
	 	data_entry key("05_02_key_01");

        int ret = 0;

		ret = client_handle.remove(-1, key);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_03_areaEquals0) {
	 	data_entry key("05_03_key_01");

        int ret = 0;
		ret = client_handle.remove(0, key);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_04_areaEquals1023) {
	 	data_entry key("05_04_key_01");

        int ret = 0;
		ret = client_handle.remove(1023, key);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_05_areaEquals1024) {
	 	data_entry key("05_04_key_01");

        int ret = 0;
		ret = client_handle.remove(1024, key);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_05_02_areaEquals65536) {
	 	data_entry key("05_05_key_01");

        int ret = 0;
		ret = client_handle.remove(65536, key);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_06_emptyKey) {
	 	data_entry key;

        int ret = 0;
		ret = client_handle.remove(0, key);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);

		data_entry key1("");
		ret = client_handle.remove(0, key1);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_07_dateNotExpire) {
	 	data_entry key1("05_07_key_01");
        data_entry value1("05_07_data_01");
        
        data_entry key2("05_07_key_02");
        data_entry value2("05_07_data_02");

        data_entry value3("05_07_data_03");

        data_entry* p;
        int ret = 0;
		//data1:area equals,key not equal
		DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		
		//data2:area not equas, key equal
		DO_WITH_RETRY(client_handle.put(2, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		client_handle.remove(1,key2);
		DO_WITH_RETRY(client_handle.put(1, key2, value3, 3, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		ret = client_handle.remove(1, key2);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		ret = client_handle.get(1, key1, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value1, *p));
        delete p;
        p = NULL;
		ret = client_handle.get(2, key2, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value2, *p));
        delete p;
        p = NULL;

		ret = client_handle.get(1, key2, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
        delete p;
        p = NULL;

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);
		client_handle.remove(1, key2);
     }
	 TEST_F(CTestInterfaceBase, RemoveTestCase_08_dateExpired) {
	 	data_entry key1("05_08_key_01");
        data_entry value1("05_08_data_01");
        
        data_entry key2("05_08_key_02");
        data_entry value2("05_08_data_02");

        data_entry value3("05_08_data_03");

        data_entry* p;
        int ret = 0;
		//data1:area equals,key not equal
		DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		
		//data2:area not equas, key equal
		DO_WITH_RETRY(client_handle.put(2, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		DO_WITH_RETRY(client_handle.put(1, key2, value3, 1, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        sleep(2);
		ret = client_handle.remove(1, key2);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		ret = client_handle.get(1, key1, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value1, *p));
        delete p;
        p = 0;

		ret = client_handle.get(2, key2, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value2, *p));
        delete p;
        p = 0;

		ret = client_handle.get(1, key2, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
		delete p;
		p = NULL;

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);
		client_handle.remove(1, key2);

     }
}

