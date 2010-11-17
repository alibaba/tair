#include "interface_test_base.h"

namespace tair {

	TEST_F(CTestInterfaceBase, decr_other_TestCase_01_mixed) {
		data_entry key("14_01_key_01");

		int area = 1;
		int count = 1;
		int initValue = 10;
		int retValue = 0;
		int ret;

		client_handle.remove(area, key);

		// step 1
		int expect = initValue - count;
		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 2
		data_entry *p;
		DO_WITH_RETRY(client_handle.get(area, key, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(1, p->get_version());
		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);
		delete p;
		p = NULL;

		// step 3
        vector<int> itemList;
        for(int i=0;i<10;++i){
            itemList.push_back(i);
        }
		ret = client_handle.add_items(area, key, itemList, 0, 0);
		ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE, ret);
		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 4
        vector<int> itemResult;
        ret = client_handle.get_items(area, key, 0, 1, itemResult);
        ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH, ret);
		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 5
		itemResult.clear();
		ret = client_handle.get_and_remove(area, key, 0, 1, itemResult);
		ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH, ret);
		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 6
		ret = client_handle.remove_items(area, key, 0, 1);
		ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH, ret);
		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 7
		ret = client_handle.get_items_count(area, key);
		ASSERT_LE(ret, 0);
		DO_WITH_RETRY(client_handle.incr(area, key, 0, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 8
		count = 1;
		expect += count;
		DO_WITH_RETRY(client_handle.incr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 9
		expect -= count;
		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 10
		DO_WITH_RETRY(client_handle.remove(area, key), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ret = client_handle.get(area, key, p);
		ASSERT_EQ(ret, TAIR_RETURN_DATA_NOT_EXIST);

		// step 11
		count = 1;
		expect = initValue - count;
		DO_WITH_RETRY(client_handle.decr(area, key, count, &retValue, initValue, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_EQ(expect, retValue);

		// step 12
		DO_WITH_RETRY(client_handle.put(area, key, key, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		DO_WITH_RETRY(client_handle.get(area, key, p), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ASSERT_TRUE(compareDataValue(key, *p));
		delete p;
		p = NULL;

		// step 13
		ret = client_handle.incr(area, key, count, &retValue);
		ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE, ret);

		// step 14
		ret = client_handle.decr(area, key, count, &retValue);
		ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE, ret);

		// step 15
		DO_WITH_RETRY(client_handle.remove(area, key), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		ret = client_handle.get(area, key, p);
		ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);

	};
}
