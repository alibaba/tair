#include "interface_test_base.h"

namespace tair {

	TEST_F(CTestInterfaceBase, decr_TestCase_01_areaNegative) {
		data_entry key("14_01_key_01");
		data_entry *p;

		int area = -1;
		int retValue = 0;

		int ret = client_handle.decr(area, key, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_02_areaEquals0) {
		data_entry key("14_02_key_01");
		data_entry *p;

		int area = 0;
		int retValue = 0;
		int ret;

		DO_WITH_RETRY(client_handle.decr(area, key, 1, &retValue, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(-1, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		DO_UNITLL(client_handle.remove(area, key), ret, TAIR_RETURN_SUCCESS);
	};
	TEST_F(CTestInterfaceBase, decr_TestCase_03_areaEquals1023) {
		data_entry key("14_03_key_01");
		data_entry *p;

		int area = 1023;
		int retValue = 0;
		int ret;

		DO_WITH_RETRY(client_handle.decr(area, key, 1, &retValue, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(-1, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		DO_UNITLL(client_handle.remove(area, key), ret, TAIR_RETURN_SUCCESS);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_04_areaEquals1024Max) {
		data_entry key("14_04_key_01");

		int area = 1024;
		int retValue = 0;
		int ret;

		ret = client_handle.decr(area, key, 1, &retValue, 2, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

	};

	TEST_F(CTestInterfaceBase, decr_TestCase_04_01_areaEauals65536) {
		data_entry key("14_04_key_01_01");

		int area = 65536;
		int retValue = 0;
		int ret;

		ret = client_handle.decr(area, key, 1, &retValue, 2, 0);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

	};

	TEST_F(CTestInterfaceBase, decr_TestCase_05_emptyKey) {
		data_entry key("");

		int area = 1;
		int retValue = 0;
		int ret;

		client_handle.remove(area, key);
		ret = client_handle.decr(area, key, 1, &retValue, 2, 0);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
		ASSERT_NE(1, retValue);

		data_entry key2;
		ret = client_handle.decr(area, key2, 1, &retValue, 0, 0);
		ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_06_countAndInitValuePositive) {
		data_entry key("14_06_key_01");
		data_entry key2("14_06_key_02");

		int area = 1;
		int area2 = 2;

		int count = 2;
		int initValue = 2;
		int expect = initValue - count;

		int retValue = -1;
		int ret;

		// data1: area equals, key not equal
		DO_WITH_RETRY(client_handle.decr(area, key2, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		// data2: area not equal, key equal
		DO_WITH_RETRY(client_handle.decr(area2, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

		// clear old data
		client_handle.remove(area, key);
		retValue = -1;

		// main data
		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// check other data
		retValue = -1;
		DO_WITH_RETRY(client_handle.decr(area, key2, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		retValue = -1;
		DO_WITH_RETRY(client_handle.decr(area2, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
		client_handle.remove(area, key2);
		client_handle.remove(area2, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_07_countPositiveInitValueNagative) {
		data_entry key("14_07_key_01");

		int area = 1;
		int retValue = 0;
		int ret;

		int count = 2;
		int initValue = -1;
		int expect = initValue - count;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_08_count0WithInitValue) {
		data_entry key("14_08_key_01");

		int area = 1;
		int retValue = 0;
		int ret;

		int count = 0;
		int initValue = 2;
		int expect = initValue - count;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_09_countNagativeWithInitValue) {
		data_entry key("14_09_key_01");

		int area = 1;
		int retValue = 0;
		int ret;

		int count = -1;
		int initValue = 2;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
		ASSERT_EQ(0, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_10_count0WithoutInitValue) {
		data_entry key("14_10_key_01");

		int area = 1;
		int retValue = 0;
		int count = 0;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_11_updateWithInitValue) {
		data_entry key("14_11_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 3;
		int initValue = 3 + count;
		int expect = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		count = 2;
		initValue = 2;
		expect = retValue - count;
		retValue = 0;

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_12_updateWithoutInitValue) {
		data_entry key("14_12_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 3;
		int initValue = 3 + count;
		int expect = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		count = 2;
		expect = retValue - count;
		retValue = 0;

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_13_updateCount0WithInitValue) {
		data_entry key("14_13_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 3;
		int initValue = 3 + count;
		int expect = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		count = 0;
		initValue = 1;
		expect = retValue - count;
		retValue = 0;

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_14_updateCountNagativeWithInitValue) {
		data_entry key("14_14_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 3;
		int initValue = 3 + count;
		int expect = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		count = -1;
		initValue = 1;
		expect = retValue;
		retValue = 0;

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		DO_WITH_RETRY(client_handle.decr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_15_overflow) {
		data_entry key("14_15_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 0;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(count, retValue);
		
		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		count = 1;
		int initValue = 1;
		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_LT(retValue, 0);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(2, p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_16_expire) {
		data_entry key("14_16_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 5;
		int expect = -5;
		int expire = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, 0, expire), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		sleep(4);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_EXPIRED, ret);
		delete p;
		p = NULL;

		count = 2;
		int initValue = 1;
		expect = initValue - count;
		retValue = 0;

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};
	
	TEST_F(CTestInterfaceBase, decr_TestCase_17_expireless0) {
		data_entry key("13_16_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 5;
		int expire = 3;
		int ret;

		client_handle.remove(area, key);

		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, 0, expire), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(-5, retValue);
		
		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		ret = client_handle.decr(area, key, count, &retValue, 0, -1);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1,p->get_version());
		delete p;
		p = NULL;

		client_handle.remove(area, key);
	};

	TEST_F(CTestInterfaceBase, decr_TestCase_18_expireless0) {
		data_entry key("13_18_key_01");
		data_entry *p;

		int area = 1;
		int retValue = 0;
		int count = 5;
		int ret;

		client_handle.remove(area, key);

		ret = client_handle.decr(area, key, count, &retValue, 0, -1);
		ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);

		client_handle.remove(area, key);
	};
}
