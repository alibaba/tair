#include "kdb_bucket.h"
#include "test_kdb_base.h"
#include "data_entry.hpp"
#include "define.hpp"
#include "item_data_info.hpp"

using namespace tair::common;

TEST_F(TestKdbBase, TestKdbBucket1)
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  ASSERT_EQ(bucket.start(1), true);
  TestKdbData db;
  data_entry temp_value;
  EXPECT_EQ(bucket.put(*(db.get_test_key(5)), *(db.get_test_value(5)), false, 0), TAIR_RETURN_SUCCESS);
  ASSERT_EQ(bucket.get(*(db.get_test_key(5)), temp_value), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(compareDataValue(temp_value, *(db.get_test_value(5))), true);
  print(temp_value);
  bucket.destory();
}

TEST_F(TestKdbBase, TestKdbBucketScan)
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  ASSERT_EQ(bucket.start(2),true);
  TestKdbData db;
  for (int i=29; i>=20; i--) {
     EXPECT_EQ(bucket.put(*(db.get_test_key(i)), *(db.get_test_value(29-i)), false, 0), TAIR_RETURN_SUCCESS);
  }
  for (int i=10; i<20; i++) {
     EXPECT_EQ(bucket.put(*(db.get_test_key(i)), *(db.get_test_value(29-i)), false, 0), TAIR_RETURN_SUCCESS);
  }
  for (int i=9; i>=0; i--) {
     EXPECT_EQ(bucket.put(*(db.get_test_key(i)), *(db.get_test_value(29-i)), false, 0), TAIR_RETURN_SUCCESS);
  }
  ASSERT_EQ(bucket.begin_scan(), true);
  for (int i=0; i<1000; i++) {
    tair::item_data_info *data = NULL;
    int rc = bucket.get_next_item(data);
    if (rc == 0){
      printf("item[%.3d]::",i);
      print(*data);
    } else {
      if(rc == 1) printf("end of scan!\n");
      else printf("meet a error!\n");
    }
    if(data) {
      free(data);
      printf("free item!\n");
    }
    if(rc) break;
  }
  ASSERT_EQ(bucket.end_scan(), true);
  bucket.destory();
}

TEST_F(TestKdbBase, TestBucketStat)
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  ASSERT_EQ(bucket.start(3), true);
  TestKdbData db;
  EXPECT_EQ(bucket.put(*(db.get_test_key(0)), *(db.get_test_value(0)), false, 0), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(bucket.remove(*(db.get_test_key(0)), false), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(bucket.put(*(db.get_test_key(0)), *(db.get_test_value(1)), false, 0), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(bucket.put(*(db.get_test_key(0)), *(db.get_test_value(2)), false, 0), TAIR_RETURN_SUCCESS);
  EXPECT_EQ(bucket.remove(*(db.get_test_key(0)), false), TAIR_RETURN_SUCCESS);
  tair::tair_stat stat[TAIR_MAX_AREA_COUNT];
  bucket.get_stat(stat);
  EXPECT_EQ(stat[0].item_count_value, 1);
  EXPECT_EQ(stat[0].data_size_value, 0);
  EXPECT_EQ(stat[0].use_size_value, 0);
  bucket.destory();
}

TEST_F(TestKdbBase,TestKdbBucketStartandStop1) //happy path
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value;
  int bucket_number=1;
  int index=1;
  ASSERT_EQ(bucket.start(bucket_number),true);
  ASSERT_EQ(bucket.put(*db.get_test_key(index),*db.get_test_value(index),1,10000),TAIR_RETURN_SUCCESS);
  bucket.stop();
  ASSERT_EQ(bucket.start(index),true);
  bucket.get(*db.get_test_key(index),value);
  ASSERT_EQ(compareDataValue(value,*db.get_test_value(index)),true);
}

TEST_F(TestKdbBase,TestKdbBucketStartandStop2)  //start twice times
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value;
  int bucket_number=2;
  int index=2;
  ASSERT_EQ(bucket.start(bucket_number),true);
  ASSERT_EQ(bucket.put(*db.get_test_key(index),*db.get_test_value(index),1,10000),TAIR_RETURN_SUCCESS);
  ASSERT_EQ(bucket.start(bucket_number),false);
  bucket.stop();
  ASSERT_EQ(bucket.start(index),true);
  bucket.get(*db.get_test_key(2),value);
  ASSERT_EQ(compareDataValue(value,*db.get_test_value(index)),true);
}

TEST_F(TestKdbBase,TestKdbBucketStartandStop3)  //get value after stop the bucket
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value;
  int bucket_number=3;
  int index=3;
  ASSERT_EQ(bucket.start(bucket_number),true);
  ASSERT_EQ(bucket.put(*db.get_test_key(index),*db.get_test_value(index),1,10000),TAIR_RETURN_SUCCESS);
  ASSERT_EQ(bucket.start(bucket_number),false);
  bucket.stop();
  ASSERT_EQ(bucket.get(*db.get_test_key(2),value),TAIR_RETURN_DATA_NOT_EXIST);
  ASSERT_EQ(compareDataValue(value,*db.get_test_value(index)),false);// can not get value
}

TEST_F(TestKdbBase,TestKdbBucket_put4)  //put interface happy path
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value;
  int bucket_number=4;
  int index,count=20;
  ASSERT_EQ(bucket.start(bucket_number),true);
  for(index=1;index<count;index++)
    ASSERT_EQ(bucket.put(*db.get_test_key(index),*db.get_test_value(index),1,10000),TAIR_RETURN_SUCCESS);
  for(index=1;index<count;index++){
    ASSERT_EQ(bucket.get(*db.get_test_key(index),value),TAIR_RETURN_SUCCESS);
    ASSERT_EQ(compareDataValue(value,*db.get_test_value(index)),true);
  }
}

TEST_F(TestKdbBase,TestKdbBucket_put5)  //test  put interface expire_time param
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value;
  int bucket_number=5;
  int index=5;
  ASSERT_EQ(bucket.start(bucket_number),true);
  ASSERT_EQ(bucket.put(*db.get_test_key(index),*db.get_test_value(index),1,1),TAIR_RETURN_SUCCESS);
  sleep(2);
  ASSERT_EQ(bucket.get(*db.get_test_key(index),value),TAIR_RETURN_DATA_EXPIRED);
}

TEST_F(TestKdbBase,TestKdbBucket_put6)  //test  put interface version_care param
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value,key;
  int bucket_number=6;
  int index=1;
  ASSERT_EQ(bucket.start(bucket_number),true);
  key=*db.get_test_key(index);
  key.data_meta.version=1;
  bucket.remove(key,0);
  ASSERT_EQ(bucket.put(key,*db.get_test_value(1),1,10000),TAIR_RETURN_SUCCESS);
  printf("key's version:%d\n",key.data_meta.version);
  ASSERT_EQ(bucket.put(key,*db.get_test_value(2),1,10000),TAIR_RETURN_SUCCESS);
  printf("key's version:%d\n",key.data_meta.version);
  ASSERT_EQ(bucket.put(key,*db.get_test_value(3),1,10000),TAIR_RETURN_SUCCESS);
  printf("key's version:%d\n",key.data_meta.version);
  ASSERT_EQ(bucket.get(key,value),TAIR_RETURN_SUCCESS);
  ASSERT_EQ(compareDataValue(value,*db.get_test_value(3)),true);
  printf("value's version:%d\n",value.data_meta.version);
}

TEST_F(TestKdbBase,TestKdbBucket_remove7)  //test  remove happy path
{
  using namespace tair::storage::kdb;
  kdb_bucket bucket;
  TestKdbData db;
  data_entry value;
  int bucket_number=4;
  int index,count=20;
  ASSERT_EQ(bucket.start(bucket_number),true);
  for(index=1;index<count;index++)
    ASSERT_EQ(bucket.put(*db.get_test_key(index),*db.get_test_value(index),1,10000),TAIR_RETURN_SUCCESS);
  for(index=1;index<count;index++){
    ASSERT_EQ(bucket.get(*db.get_test_key(index),value),TAIR_RETURN_SUCCESS);
    ASSERT_EQ(compareDataValue(value,*db.get_test_value(index)),true);
  }
  for(index=1;index<count;index++)
    bucket.remove(*db.get_test_key(index),0);
  for(index=1;index<count;index++){
    ASSERT_EQ(bucket.get(*db.get_test_key(index),value),TAIR_RETURN_DATA_NOT_EXIST);
  }
}


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
