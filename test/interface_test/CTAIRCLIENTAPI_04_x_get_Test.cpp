#include "interface_test_base.h"
namespace tair{
	 TEST_F(CTestInterfaceBase, GetTestCase_01_dateNotExist) {
	 	data_entry key1("04_01_key_01");
        data_entry value1("04_01_data_01");
        
        data_entry key2("04_01_key_02");
        data_entry value2("04_01_data_02");

        data_entry* p;
        int ret = 0;
		//data1:area equals,key not equal
		DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		
		//data2:area not equas, key equal
		DO_WITH_RETRY(client_handle.put(2, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		ret = client_handle.get(1, key2, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);

     }
	 TEST_F(CTestInterfaceBase, GetTestCase_02_areaNegative) {
	 	data_entry key("04_02_key_01");

        data_entry* p;
        int ret = 0;

		ret = client_handle.get(-1, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
     }
	 TEST_F(CTestInterfaceBase, GetTestCase_03_areaEquals0) {
	 	data_entry key("04_03_key_01");

        data_entry* p;
        int ret = 0;
		ret = client_handle.get(0, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
     }
	 TEST_F(CTestInterfaceBase, GetTestCase_04_areaEquals1023) {
	 	data_entry key("04_04_key_01");

        data_entry* p;
        int ret = 0;
		ret = client_handle.get(1023, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
     }
	 TEST_F(CTestInterfaceBase, GetTestCase_05_areaEquals1024) {
	 	data_entry key("04_05_key_01");

        data_entry* p;
        int ret = 0;
		ret = client_handle.get(1024, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
     }
	 TEST_F(CTestInterfaceBase, GetTestCase_05_01_areaEquals65536) {
	 	data_entry key("04_05_key_01");

        data_entry* p;
        int ret = 0;
		ret = client_handle.get(65536, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
     }
	 TEST_F(CTestInterfaceBase, GetTestCase_06_emptyKey) {
	 	data_entry key;

        data_entry* p;
        int ret = 0;
		ret = client_handle.get(0, key, p);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);

		data_entry key1("");
		ret = client_handle.get(0, key1, p);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
     }
	 TEST_F(CTestInterfaceBase, GetTestCase_07_dateNotExpire) {
	 	data_entry key1("04_07_key_01");
        data_entry value1("04_07_data_01");
        
        data_entry key2("04_07_key_02");
        data_entry value2("04_07_data_02");

        data_entry value3("04_07_data_03");

        data_entry* p;
        int ret = 0;
		//data1:area equals,key not equal
		DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		
		//data2:area not equas, key equal
		DO_WITH_RETRY(client_handle.put(2, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		DO_WITH_RETRY(client_handle.put(1, key2, value3, 3, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		ret = client_handle.get(1, key2, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value3, *p));
        delete p;
		p = NULL;

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);
		client_handle.remove(1, key2);

     }
	 TEST_F(CTestInterfaceBase, GetTestCase_08_dateExpired) {
	 	data_entry key1("04_08_key_01");
        data_entry value1("04_08_data_01");
        
        data_entry key2("04_08_key_02");
        data_entry value2("04_08_data_02");

        data_entry value3("04_08_data_03");

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

		ret = client_handle.get(1, key2, p);
		ASSERT_EQ(TAIR_RETURN_DATA_EXPIRED, ret);

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);
		client_handle.remove(1, key2);

     }
}

