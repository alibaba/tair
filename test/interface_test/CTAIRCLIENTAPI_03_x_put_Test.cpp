#include "interface_test_base.h"
namespace tair{
	 TEST_F(CTestInterfaceBase, PutTestCase_01_dateNotExist) {
	 	data_entry key1("03_01_key_01");
        data_entry value1("03_01_data_01");
        
        data_entry key2("03_01_key_02");
        data_entry value2("03_01_data_02");
        
        data_entry value3("03_01_data_03");

		
        data_entry* p;
        int ret = 0;
		
		//data1:area equals,key not equal
		DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		
		//data2:area not equas, key equal
		DO_WITH_RETRY(client_handle.put(2, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);

		//clean old data
		client_handle.remove(1, key2);

		//data3:test data
		
		DO_WITH_RETRY(client_handle.put(1, key2, value3, 0, 0), ret, TAIR_RETURN_SUCCESS);

		//verify data
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value3, *p));
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		//verify other data 
		DO_WITH_RETRY(client_handle.get(2, key2, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_NE(true,compareDataValue(value3, *p));
		delete p;
		p = NULL;
		
		DO_WITH_RETRY(client_handle.get(1, key1, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_NE(true,compareDataValue(value3, *p));
		delete p;
		p = NULL;

		client_handle.remove(1, key1);
		client_handle.remove(2, key2);
		client_handle.remove(1, key2);
		
    };

	 TEST_F(CTestInterfaceBase, PutTestCase_02_areaNegative){
        data_entry key("03_02_key_01");
        data_entry value("03_02_data_01");
        data_entry* p;

		int ret = client_handle.put(-1, key, value, 0, 0);

		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		ret = client_handle.get(-1, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		
		
    };

   TEST_F(CTestInterfaceBase, PutTestCase_03_areaEquals0){
        data_entry key("03_03_key_01");
        data_entry value("03_03_data_01");
        data_entry* p;
        int ret;

		client_handle.remove(0, key);
		DO_WITH_RETRY(client_handle.put(0, key, value, 0, 5), ret, TAIR_RETURN_SUCCESS);

		//verify data
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value, *p));
		ASSERT_EQ(1,p->get_version());
		

		//clean data
		delete p;
		p = NULL;
		client_handle.remove(0, key);
		
    }
   TEST_F(CTestInterfaceBase, PutTestCase_04_areaEquals1023Max){
        data_entry key("03_04_key_01");
        data_entry value("03_04_data_01");
        data_entry* p;
        int ret;
		
		client_handle.remove(1023, key);

		ret = client_handle.put(1023, key, value, 0, 0);

		//verify data
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		DO_WITH_RETRY(client_handle.get(1023, key, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true,compareDataValue(value, *p));
		ASSERT_EQ(1,p->get_version());
		

		//clean data
		delete p;
		p = NULL;
		client_handle.remove(1023, key);

    }

   TEST_F(CTestInterfaceBase, PutTestCase_05_areaEquals1024Max){
        data_entry key("03_05_key_01");
        data_entry value("03_05_data_01");
        data_entry* p;
        int ret;
		
		client_handle.remove(1024, key);

		ret = client_handle.put(1024, key, value, 0, 0);

		//verify data
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		ret = client_handle.get(1024, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

    }

    TEST_F(CTestInterfaceBase, PutTestCase_05_02_areaEquals65536) {
        data_entry key("03_05_key_01");
        data_entry value("03_05_data_01");
        data_entry* p;
        int ret;
		
		client_handle.remove(65536, key);

		ret = client_handle.put(65536, key, value, 0, 0);

		//verify data
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		ret = client_handle.get(65536, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		
    }

	 TEST_F(CTestInterfaceBase, PutTestCase_06_emptyKey) {
        data_entry key("");
        data_entry value("03_06_data_01");
        data_entry* p;
		
        int ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
        
        ret = client_handle.get(0, key, p);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
       
        ret = client_handle.remove(0, key);
                
        data_entry key2;
        ret = client_handle.put(0, key2, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
    }

	  TEST_F(CTestInterfaceBase, PutTestCase_07_emptyData) {
        data_entry key("03_07_key_01");
        data_entry value("");
        data_entry* p;
        
        client_handle.remove(0, key);
        int ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
        ret = client_handle.get(0, key, p);
        ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
        
        
        data_entry value2;
        ret = client_handle.put(0, key, value2, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
        ret = client_handle.get(0, key, p);
        ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
        
    }
    
	  
	   TEST_F(CTestInterfaceBase, PutTestCase_08_versionEquals) {
        data_entry key("03_08_key_01");
        data_entry value("03_08_data_01");
        data_entry* p;
        int ret;

        client_handle.remove(1, key);
        DO_WITH_RETRY(client_handle.put(1, key, value, 5, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        int version = p->get_version();
		ASSERT_EQ(1,version);

        delete p;
		p = NULL;
        
		string value2("03_08_data_02");
        value.set_data(value2.c_str(), value2.length());
        int versionOld = version;
        DO_WITH_RETRY(client_handle.put(1, key, value, 6, version), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(versionOld + 1 , p->get_version());
		ASSERT_EQ(true, compareDataValue(value, *p));
		//ASSERT_EQ(6, p->dataMeta.edate);
       
        delete p;
		p = NULL;
        client_handle.remove(0, key);
        
    };
   
    TEST_F(CTestInterfaceBase, PutTestCase_09_versionBigger) {
        data_entry key("03_key_09_01");
        data_entry value("03_09_data_01");
        data_entry* p;
        int ret;

        client_handle.remove(1, key);
        DO_WITH_RETRY(client_handle.put(1, key, value, 5, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        int version = p->get_version();
		ASSERT_EQ(1,version);
		
        delete p;
		p = NULL;
        
		string value2("03_09_data_02");
        value.set_data(value2.c_str(), value2.length());
        int versionOld = version;
        DO_WITH_RETRY(client_handle.put(1, key, value, 6, version+1), ret, TAIR_RETURN_VERSION_ERROR);
        ASSERT_EQ(TAIR_RETURN_VERSION_ERROR, ret);
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(versionOld , p->get_version());
     	ASSERT_NE(true, compareDataValue(value, *p));
		//ASSERT_EQ(5, p->dataMeta.edate);

        delete p;
		p = NULL;
        DO_WITH_RETRY(client_handle.remove(1, key), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    };
	 TEST_F(CTestInterfaceBase, PutTestCase_10_versionLess) {
        data_entry key("03_key_10_01");
        data_entry value("03_10_data_01");
        data_entry* p;
        int ret;

        client_handle.remove(1, key);
        DO_WITH_RETRY(client_handle.put(1, key, value, 5, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        int version = p->get_version();
        
        delete p;
		p = NULL;
        DO_WITH_RETRY(client_handle.put(1, key, value, 5, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        int version2 = p->get_version();
        ASSERT_LT(version, p->get_version());
        delete p;
		p = NULL;
        
		string value2("03_09_data_02");
        value.set_data(value2.c_str(), value2.length());
       
        DO_WITH_RETRY(client_handle.put(1, key, value, 6, version), ret, TAIR_RETURN_VERSION_ERROR);
        ASSERT_EQ(TAIR_RETURN_VERSION_ERROR, ret);
        DO_WITH_RETRY(client_handle.get(1, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(version2 , p->get_version());
    	ASSERT_NE(true, compareDataValue(value, *p));
		//ASSERT_EQ(5, p->dataMeta.edate);

        delete p;
		p = NULL;
        DO_WITH_RETRY(client_handle.remove(1, key), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    };
	 TEST_F(CTestInterfaceBase, PutTestCase_11_dataExpire) {
        data_entry key("03_11_key_01");
        data_entry value("03_11_data_01");
        data_entry* p;
        int ret;
        client_handle.remove(0, key);
        DO_WITH_RETRY(client_handle.put(0, key, value, 3, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        delete p;
        p = NULL;
        sleep(4);
        ret = client_handle.get(0, key, p);
        ASSERT_EQ(TAIR_RETURN_DATA_EXPIRED, ret);
        ASSERT_EQ(NULL, p);
        
        ret = client_handle.remove(0, key);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);

    };
    TEST_F(CTestInterfaceBase, PutTestCase_12_less1kKey) {
    	int key_size = (int)(1024 * 0.99);
    	char key_str[key_size + 1];
    	for (int i = 0 ; i < key_size; ++i) {
    		key_str[i] = 'A';
    	}
    	key_str[key_size] = 0;
        data_entry key(key_str);
        data_entry value("03_12_data_01");
        data_entry* p;
        int ret;
		
        client_handle.remove(0, key);
        DO_WITH_RETRY(client_handle.put(0, key, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        delete p;
        p = NULL;
        
        ret = client_handle.remove(0, key);
		

    };
    TEST_F(CTestInterfaceBase, PutTestCase_13_equal1kKey) {
    	int key_size = 1024 ;
    	char key_str[key_size + 1];
    	for (int i = 0 ; i < key_size; ++i) {
    		key_str[i] = 'A';
    	}
    	key_str[key_size] = 0;
        data_entry key(key_str);
        data_entry value("03_13_data_01");
        int ret;
        
        ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
      
    };
    
    TEST_F(CTestInterfaceBase, PutTestCase_14_greater1kKey) {
    	int key_size = (int)(1024 * 1.1);
    	char key_str[key_size + 1];
    	for (int i = 0 ; i < key_size; ++i) {
    		key_str[i] = 'A';
    	}
    	key_str[key_size] = 0;
        data_entry key(key_str);
        data_entry value("03_14_data_01");
        int ret;
        
        ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
      
    };
    
    TEST_F(CTestInterfaceBase, PutTestCase_15_less1Mdata) {
    	int data_size_value = (int)(1000000 * 0.99);
    	char data_str[data_size_value + 1];
    	memset(data_str, 'A', data_size_value);
    	data_str[data_size_value] = 0;
        data_entry key("03_15_key_01");
        data_entry value(data_str);
        data_entry* p;
       
        int ret;

		client_handle.remove(0, key);
		
        DO_WITH_RETRY(client_handle.put(0, key, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        delete p;
        p = NULL;
        
        ret = client_handle.remove(0, key);
		
    };
	 
	TEST_F(CTestInterfaceBase, PutTestCase_16_equal1Mdata) {
    	int data_size_value = 1000000 ;
    	char data_str[data_size_value + 1];
    	memset(data_str, 'A', data_size_value);
    	data_str[data_size_value] = 0;
        data_entry key("03_16_key_01");
        data_entry value(data_str);
        int ret;
        
        ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
		
    };
    
    TEST_F(CTestInterfaceBase, PutTestCase_17_greater1Mdata) {
    	int data_size_value = (int)(1000000 * 1.1);
    	char data_str[data_size_value + 1];
    	memset(data_str, 'A', data_size_value);
    	data_str[data_size_value] = 0;
        data_entry key("03_17_key_01");
        data_entry value(data_str);
        int ret;
        
        ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
		
    };
    
    TEST_F(CTestInterfaceBase, PutTestCase_18_greater1MdataUpdate) {
    	int data_size_value = (int)(1000000 * 0.9);
    	char data_str[data_size_value + 1];
    	memset(data_str, 'A', data_size_value);
    	data_str[data_size_value] = 0;
        data_entry key("03_18_key_01");
        data_entry value(data_str);
        data_entry* p;
        int ret;

		client_handle.remove(0, key);
        DO_WITH_RETRY(client_handle.put(0, key, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        delete p;
        p = NULL;
        
        data_size_value = (int)(1000000 * 1.1);
        char data_str2[data_size_value + 1];
    	memset(data_str2, 'A', data_size_value);
    	data_str2[data_size_value] = 0;
    	value.set_data(data_str2, data_size_value);
    	
    	ret = client_handle.put(0, key, value, 0, 0);
        ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
		DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_NE(true, compareDataValue(value, *p));
        ret = client_handle.remove(0, key);
		
    };
	  
    TEST_F(CTestInterfaceBase, PutTestCase_19_expiredless0) {
    	data_entry key("03_19_key_01");
        data_entry value("03_19_data_01");
        data_entry* p;
        int ret;
        client_handle.remove(0, key);
        DO_WITH_RETRY(client_handle.put(0, key, value, 3, 0), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
        ASSERT_EQ(true, compareDataValue(value, *p));
        delete p;
        p = NULL;

		ret = client_handle.put(0, key, value, -1, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
  	    ASSERT_EQ(true, compareDataValue(value, *p));
		ASSERT_EQ(1, p->get_version());
        
        client_handle.remove(0, key);
		
    };
	
	 TEST_F(CTestInterfaceBase, PutTestCase_20_expiredless0) {
    	data_entry key("03_20_key_01");
        data_entry value("03_20_data_01");
        data_entry* p;
        int ret;
        client_handle.remove(0, key);

		ret = client_handle.put(0, key, value, -1, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
        
        client_handle.remove(0, key);
		
    };
	 
	TEST_F(CTestInterfaceBase, PutTestCase_21_versionLimited) {
    	data_entry key("03_21_key_01");
        data_entry value("03_21_data_01");
        data_entry* p;
        int ret;
        client_handle.remove(0, key);
		for(int i=0;i<65538;i++)
		{

			ret = client_handle.put(0, key, value, 0, 0);
			ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		}
		
        DO_WITH_RETRY(client_handle.get(0, key, p), ret, TAIR_RETURN_SUCCESS);
        ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(true, compareDataValue(value, *p));
		ASSERT_EQ(2, p->get_version());
        
        client_handle.remove(0, key);
		
    };

}
