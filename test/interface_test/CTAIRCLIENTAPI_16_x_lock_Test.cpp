#include "interface_test_base.h"
namespace tair{
  TEST_F(CTestInterfaceBase, LockCase_01_all) {
    int ret = TAIR_RETURN_SUCCESS;
      data_entry key1("16_01_key_01");
    data_entry value1("16_01_value_01");
    data_entry* ret_value1;
    int result_count = 0;
    int area = 1;
    int count = 10;
    client_handle.remove(area, key1);

    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
    // put
    DO_WITH_RETRY(client_handle.put(area, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can lock
    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can not relock
    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_EXIST, ret);
    // can get
    DO_WITH_RETRY(client_handle.get(area, key1, ret_value1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(memcmp(ret_value1->get_data(), value1.get_data(), ret_value1->get_size()), 0);
    delete ret_value1;
    ret_value1 = NULL;
    // can not put
    DO_WITH_RETRY(client_handle.put(area, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_EXIST, ret);

    // unlock
    // can unlock
    DO_WITH_RETRY(client_handle.unlock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can not reunlock
    DO_WITH_RETRY(client_handle.unlock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_NOT_EXIST, ret);
    // can get
    DO_WITH_RETRY(client_handle.get(area, key1, ret_value1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(memcmp(ret_value1->get_data(), value1.get_data(), ret_value1->get_size()), 0);
    delete ret_value1;
    ret_value1 = NULL;
    // can put
    DO_WITH_RETRY(client_handle.put(area, key1, value1, 0, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

    // can lock
    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can delete
    DO_WITH_RETRY(client_handle.remove(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

    // incr/decr
    DO_WITH_RETRY(client_handle.incr(area, key1, count, &result_count, 1, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(result_count, count + 1);
    DO_WITH_RETRY(client_handle.decr(area, key1, 1, &result_count, 1, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(result_count, count);
    // can lock
    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can not relock
    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_EXIST, ret);
    // can get
    DO_WITH_RETRY(client_handle.get(area, key1, ret_value1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    delete ret_value1;
    ret_value1 = NULL;

    // can not incr/decr
    DO_WITH_RETRY(client_handle.incr(area, key1, count, &result_count, 1, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_EXIST, ret);
    DO_WITH_RETRY(client_handle.decr(area, key1, count, &result_count, 1, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_EXIST, ret);
    // unlock
    // can unlock
    DO_WITH_RETRY(client_handle.unlock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can not reunlock
    DO_WITH_RETRY(client_handle.unlock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_LOCK_NOT_EXIST, ret);
    // can get
    DO_WITH_RETRY(client_handle.get(area, key1, ret_value1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    delete ret_value1;
    ret_value1 = NULL;

    // can incr/decr
    DO_WITH_RETRY(client_handle.incr(area, key1, 1, &result_count, 1, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(result_count, count + 1);
    DO_WITH_RETRY(client_handle.decr(area, key1, 1, &result_count, 1, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(result_count, count);
    // can lock
    DO_WITH_RETRY(client_handle.lock(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    // can delete
    DO_WITH_RETRY(client_handle.remove(area, key1), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
  }
    
}
