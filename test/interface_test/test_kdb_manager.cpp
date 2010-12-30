#include "kdb_manager.h"
#include "test_kdb_base.h"

TEST_F(TestKdbBase, TestKdbStart)
{
  using namespace tair::common;
  tair::storage::kdb::kdb_manager *kdb_mgr = new tair::storage::kdb::kdb_manager();
  TestKdbData db;
  ASSERT_EQ(kdb_mgr->init_buckets(db.get_buckets()), true);
  EXPECT_EQ(kdb_mgr->put(1, *(db.get_test_key(1)), *(db.get_test_value(1)), false, 0), TAIR_RETURN_SUCCESS);
  if (kdb_mgr) {
    delete kdb_mgr;
  }
  kdb_mgr = new tair::storage::kdb::kdb_manager();
  ASSERT_EQ(kdb_mgr->init_buckets(db.get_buckets()), true);
  data_entry temp;
  EXPECT_EQ(kdb_mgr->get(1, *(db.get_test_key(1)), temp), TAIR_RETURN_SUCCESS);
  print(temp);
  EXPECT_EQ(compareDataValue(temp, *(db.get_test_value(1))), true);
  kdb_mgr->close_buckets(db.get_buckets());
  if (kdb_mgr) {
    delete kdb_mgr;
  }
}

TEST_F(TestKdbBase, TestKdbManagerPut1)
{
  using namespace tair::storage::kdb;

}

TEST_F(TestKdbBase, TestKdbManagerPut2)
{
  using namespace tair::storage::kdb;
}

TEST_F(TestKdbBase, TestKdbManagerGet1)
{
  using namespace tair::storage::kdb;
  TestKdbData db;
  kdb_manager kdb_mgr;
  tair::common::data_entry key, value;
  ASSERT_EQ(kdb_mgr.init_buckets(db.get_buckets()), true);
  key = *db.get_test_key(1);
  kdb_mgr.remove(1, key, 0);
  ASSERT_EQ(kdb_mgr.put(1, key, *db.get_test_value(1),0,10000),TAIR_RETURN_SUCCESS);

  ASSERT_EQ(kdb_mgr.put(1,key,*db.get_test_value(2),1,10000),TAIR_RETURN_SUCCESS);  
  printf("version:key[%d]-value[%d]\n",key.data_meta.version, value.data_meta.version);
 
  //key.data_meta.version++;
  ASSERT_EQ(kdb_mgr.put(1,key,*db.get_test_value(3),1,10000),TAIR_RETURN_SUCCESS);
  printf("version:key[%d]-value[%d]\n",key.data_meta.version, value.data_meta.version);
 
  //key.data_meta.version++;
  ASSERT_EQ(kdb_mgr.put(1,key,*db.get_test_value(4),1,10000),TAIR_RETURN_SUCCESS);
  ASSERT_EQ(kdb_mgr.get(1,key,value),TAIR_RETURN_SUCCESS);
  printf("version:key[%d]-value[%d]\n",key.data_meta.version, value.data_meta.version);
  ASSERT_EQ(compareDataValue(value,*db.get_test_value(4)),true);
}

TEST_F(TestKdbBase, TestKdbManagerRemove)
{
  using namespace tair::storage::kdb;
  TestKdbData db;
  kdb_manager kdb_mgr;
  tair::common::data_entry temp;
  ASSERT_EQ(kdb_mgr.init_buckets(db.get_buckets()), true);
  EXPECT_EQ(kdb_mgr.put(1, *(db.get_test_key(1)), *(db.get_test_value(1)),0,0), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(kdb_mgr.remove(2, *(db.get_test_key(1)), 0), TAIR_RETURN_DATA_NOT_EXIST);
  EXPECT_EQ(kdb_mgr.remove(1, *(db.get_test_key(1)), 0), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(kdb_mgr.get(1, *(db.get_test_key(1)), temp), TAIR_RETURN_DATA_NOT_EXIST);
  EXPECT_EQ(kdb_mgr.remove(1, *(db.get_test_key(1)), 0), TAIR_RETURN_DATA_NOT_EXIST);
  EXPECT_EQ(kdb_mgr.remove(9, *(db.get_test_key(1)), 0), TAIR_RETURN_FAILED);
}

TEST_F(TestKdbBase, TestKdbBucketScan)
{

}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
