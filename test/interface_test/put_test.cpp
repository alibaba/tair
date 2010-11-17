#include "interface_test_base.h"
namespace tair{

	//http://10.1.6.138:8080/qcbin/start_a.htm
	//CTAIRCLIENTAPI_03_x_put_Test

    TEST_F(CTestInterfaceBase, put_test_case_01)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    delete p;
	    p=0;
	   
	    ret = client_handle.remove(1, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

    }
    TEST_F(CTestInterfaceBase, put_test_case_02){
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(-1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(-1, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);

	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

    } ;
    TEST_F(CTestInterfaceBase, put_test_case_03)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(0, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(0, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(0, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    delete p;
	    p=0;
	   
	    ret = client_handle.remove(0, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(0, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

    };
    TEST_F(CTestInterfaceBase, put_test_case_04)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1024, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1024, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1024, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    delete p;
	    p=0;
	   
	    ret = client_handle.remove(1024, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1024, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

    };
    TEST_F(CTestInterfaceBase, put_test_case_05)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1025, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1025, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1025, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    delete p;
	    p=0;
	   
	    ret = client_handle.remove(1025, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1025, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

    };
	TEST_F(CTestInterfaceBase, put_test_case_06)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    //key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1, key2, value2, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);
	   
	    ret = client_handle.remove(1, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	   
    };
    TEST_F(CTestInterfaceBase, put_test_case_07)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    //value1.set_data(val_str1.c_str(), val_str1.size());
	
	    //key2.set_data(key_str1.c_str(), key_str1.size());
	    //value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);
    };
    TEST_F(CTestInterfaceBase, put_test_case_08)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1, key2, value2, 5, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    int version = p->get_version();
	    delete p;
	    p=0;
	    
	    DO_WITH_RETRY(client_handle.put(1, key2, value1, 6, version ), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value1, *p));
	    ASSERT_EQ(version + 1, p->get_version());
	    ASSERT_EQ(6, p->data_meta.edate);
	    delete p;
	    p=0;
	   
	   
	    ret = client_handle.remove(1, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
    };
    
    TEST_F(CTestInterfaceBase, put_test_case_09)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1, key2, value2, 5, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    int version = p->get_version();
	    delete p;
	    p=0;
	    
	    ret = client_handle.put(1, key2, value1, 6, version + 3 );
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    ASSERT_EQ(version , p->get_version());
	    ASSERT_EQ(5, p->data_meta.edate);
	    delete p;
	    p=0;
	   
	   
	    ret = client_handle.remove(1, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
    };
    
    TEST_F(CTestInterfaceBase, put_test_case_10)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1, key2, value2, 5, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    
	    DO_WITH_RETRY(client_handle.put(1, key2, value2, 5, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    int version = p->get_version();
	    delete p;
	    p=0;
	    ASSERT_LT(1, version);
	    
	    ret = client_handle.put(1, key2, value1, 6, 1 );
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    ASSERT_EQ(version , p->get_version());
	    ASSERT_EQ(5, p->data_meta.edate);
	    delete p;
	    p=0;
	   
	   
	    ret = client_handle.remove(1, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
    };
    TEST_F(CTestInterfaceBase, put_test_case_10)
    {
	    data_entry key1;
	    data_entry value1;
	    data_entry key2;
	    data_entry value2;
	    
	    data_entry* p = 0;
	    
	    string key_str1 = "test01key";
	    string key_str2 = "test02key";
	    string val_str1 = "test01val";
	    string val_str2 = "test02val";
	   
	    key1.set_data(key_str1.c_str(), key_str1.size());
	    value1.set_data(val_str1.c_str(), val_str1.size());
	
	    key2.set_data(key_str1.c_str(), key_str1.size());
	    value2.set_data(val_str1.c_str(), val_str1.size());

	    int ret = 0;

	    DO_WITH_RETRY(client_handle.put(1, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.put(2, key2, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

        DO_WITH_RETRY(client_handle.put(1, key2, value2, 5, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    
	    DO_WITH_RETRY(client_handle.put(1, key2, value2, 5, 0), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    int version = p->get_version();
	    delete p;
	    p=0;
	    ASSERT_LT(1, version);
	    
	    ret = client_handle.put(1, key2, value1, 6, 1 );
	    ASSERT_NE(TAIR_RETURN_SUCCESS, ret);

	    DO_WITH_RETRY(client_handle.get(1, key2, p), ret, TAIR_RETURN_SUCCESS);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
	    ASSERT_EQ(true,compareDataValue(value2, *p));
	    ASSERT_EQ(version , p->get_version());
	    ASSERT_EQ(5, p->data_meta.edate);
	    delete p;
	    p=0;
	   
	   
	    ret = client_handle.remove(1, key1);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	   
	    ret = client_handle.remove(2, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	    ret = client_handle.remove(1, key2);
	    ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
    };
}
