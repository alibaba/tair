#include "interface_test_base.h"
namespace tair{
  TEST_F(CTestInterfaceBase, SetCountCase_01_all) {
    int ret = TAIR_RETURN_SUCCESS;
      data_entry key1("15_01_key_01");
    int count = 10;
    int result_count = 0;
    int area = 1;
    client_handle.remove(area, key1);
    DO_WITH_RETRY(client_handle.set_count(area, key1, count, 0, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    DO_WITH_RETRY(client_handle.incr(area, key1, 3, &result_count, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(count + 3, result_count);
    DO_WITH_RETRY(client_handle.decr(area, key1, 1, &result_count, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(count + 2, result_count);
    DO_WITH_RETRY(client_handle.set_count(area, key1, count, 0, 0), ret, TAIR_RETURN_SUCCESS);
    DO_WITH_RETRY(client_handle.incr(area, key1, 1, &result_count, 0), ret, TAIR_RETURN_SUCCESS);
    ASSERT_EQ(count + 1, result_count);
  }
    
}
