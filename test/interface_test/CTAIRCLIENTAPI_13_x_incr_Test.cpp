
#include "interface_test_base.h"

namespace tair {

	TEST_F(CTestInterfaceBase, incr_TestCase_01_areaNegative) {
		data_entry key("13_01_key_01");
		data_entry *p;

		int area = -1;
		int retValue = 0;

		int ret = client_handle.incr(area, key, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_02_areaEquals0) {
		data_entry key("13_02_key_01");
		data_entry *p;

		int area = 0;
		int retValue = 0;
		int ret;
		client_handle.remove(area, key);
		DO_WITH_RETRY(client_handle.incr(area, key, 1, &retValue, 1, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
        p = NULL;

		DO_UNITLL(client_handle.remove(area, key), ret, TAIR_RETURN_SUCCESS);
	};
	TEST_F(CTestInterfaceBase, incr_TestCase_03_areaEquals1023) {
		data_entry key("13_02_key_01");
		data_entry *p;

		int area = 1023;
		int retValue = 0;
		int ret;
		client_handle.remove(area, key);
		DO_WITH_RETRY(client_handle.incr(area, key, 1, &retValue, 1, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
        p = NULL;

		DO_UNITLL(client_handle.remove(area, key), ret, TAIR_RETURN_SUCCESS);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_04_areaEquals1024Max) {
		data_entry key("13_03_key_01");

		int area = 1024;
		int retValue = 0;
		int ret;

		client_handle.remove(area, key);
		ret = client_handle.incr(area, key, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

	};

	TEST_F(CTestInterfaceBase, incr_TestCase_04_02_areaEquals65536) {
		data_entry key("13_04_key_01_02");

		int area = 65536;
		int retValue = 0;
		int ret;
		
		client_handle.remove(area, key);
		ret = client_handle.incr(area, key, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

	};

	TEST_F(CTestInterfaceBase, incr_TestCase_05_emptyKey) {
		data_entry key("");

		int area = 1;
		int retValue = 0;
		int ret;

		client_handle.remove(area, key);
		ret = client_handle.incr(area, key, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
		ASSERT_EQ(0, retValue);

		data_entry key2;
		ret = client_handle.incr(area, key2, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
		ASSERT_EQ(0, retValue);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_06_countAndInitValuePositive) {
		data_entry key("13_06_key_01");
		data_entry key2("13_06_key_02");

		int area = 1;
		int area2 = 2;

		int count = 2;
		int initValue = 1;
		int expect = count + initValue;

		int retValue = 0;
		int ret;

		// data1: area equals, key not equal
		DO_WITH_RETRY(client_handle.incr(area, key2, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		// data2: area not equal, key equal
		DO_WITH_RETRY(client_handle.incr(area2, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		// clear old data
		client_handle.remove(area, key);
		retValue = 0;

		// main data
		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// check other data
		retValue = 0;
		DO_WITH_RETRY(client_handle.incr(area, key2, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		retValue = 0;
		DO_WITH_RETRY(client_handle.incr(area2, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
		client_handle.remove(area, key2);
		client_handle.remove(area2, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_07_countPositiveInitValueNagative) {
		data_entry key("13_07_key_01");

		int area = 1;
		int retValue = 0;
		int ret;

		int count = 2;
		int initValue = -1;
		int expect = count + initValue;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_08_count0WithInitValue) {
		data_entry key("13_08_key_01");

		int area = 1;
		int retValue = 0;
		int ret;

		int count = 0;
		int initValue = 2;
		int expect = count + initValue;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_09_countNagativeWithInitValue) {
		data_entry key("13_09_key_01");

		int area = 1;
		int retValue = 0;
		int ret;

		int count = -1;
		int initValue = 2;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		ASSERT_EQ(0, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_10_count0WithoutInitValue) {
		data_entry key("13_10_key_01");

		int area = 1;
		int retValue = 0;
		int count = 0;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_11_updateWithInitValue) {
		data_entry key("13_11_key_01");

		int area = 1;
		int retValue = 0;
		int count = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		count = 2;
		int initValue = 2;
		int expect = count + retValue;
		retValue = 0;

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_12_updateWithoutInitValue) {
		data_entry key("13_12_key_01");

		int area = 1;
		int retValue = 0;
		int count = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		count = 4;
		int expect = count + retValue;
		retValue = 0;

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_13_updateCount0WithInitValue) {
		data_entry key("13_13_key_01");

		int area = 1;
		int retValue = 0;
		int count = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		count = 0;
		int initValue = 1;
		int expect = count + retValue;
		retValue = 0;

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_14_updateCountNagativeWithInitValue) {
		data_entry key("13_14_key_01");

		int area = 1;
		int retValue = 0;
		int count = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		count = -1;
		int initValue = 1;
		int expect = retValue;
		retValue = 0;

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_15_overflow) {
		data_entry key("13_15_key_01");

		int area = 1;
		int retValue = 0;
		int count = 0x7FFFFFFF;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		count = 1;
		int initValue = 1;
		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_LT(retValue, 0);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_16_expire) {
		data_entry key("13_16_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 5;
		int expire = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, 0, expire), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);
		
		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		sleep(4);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_EXPIRED, ret);

		count = 1;
		int initValue = 1;
		int expect = count + initValue;
		retValue = 0;

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};
	
	TEST_F(CTestInterfaceBase, incr_TestCase_17_expireless0) {
		data_entry key("13_16_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 5;
		int expire = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue, 0, expire), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);
		
		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		ret = client_handle.incr(area, key, count, &retValue, 0, -1);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, incr_TestCase_18_expireless0) {
		data_entry key("13_18_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 5;
		int ret;

		client_handle.remove(area, key);

		ret = client_handle.incr(area, key, count, &retValue, 0, -1);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);

		client_handle.remove(area, key);
	};
	TEST_F(CTestInterfaceBase, incr_TestCase_19_versionLimited) {
		data_entry key("13_19_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 1;
		int ret;

		client_handle.remove(area, key);
		for(int i=0;i<65538;i++)
			{
				ret = client_handle.incr(area, key, count, &retValue, 0, 0);
				ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
			}

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2,p->get_version());
		ASSERT_EQ(65538,retValue);
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};
}
