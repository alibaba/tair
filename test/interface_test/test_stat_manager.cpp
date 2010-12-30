#include "stat_manager.h"
#include "stat_info.hpp"
#include "test_kdb_base.h"

TEST_F(TestKdbBase, TestStatManagerAll)
{
  using namespace tair::storage::kdb;
  const char* file_dir = TBSYS_CONFIG.getString("kdb", "data_dir", NULL);
  stat_manager stat_mgr;

  ASSERT_EQ(stat_mgr.start(1,file_dir), true);
  uint64_t use_size;
  uint64_t data_size;
  uint64_t item_count;
  int rand_area;
  int rand_use_size;
  int rand_data_size;
  tair::tair_pstat *stat = stat_mgr.get_stat();
  printf("test stat_add() and stat_sub() start:\n");
  for(int j=0; j<100; j++){
    //fprintf(stderr, "%d => %llu, %llu,%llu\n", i, stat[i].data_size(), stat[i].use_size(), stat[i].item_count());
    rand_area = rand() % 1024;
    rand_data_size = rand() % 10000;
    rand_use_size = rand() % 10000;
    data_size = stat[rand_area].data_size();
    use_size = stat[rand_area].use_size();
    item_count = stat[rand_area].item_count();
    if (rand()%2) {
      stat_mgr.stat_add(rand_area, rand_data_size, rand_use_size);
      EXPECT_EQ(stat[rand_area].data_size(), data_size + rand_data_size);
      EXPECT_EQ(stat[rand_area].use_size(), use_size + rand_use_size);
      EXPECT_EQ(stat[rand_area].item_count(), item_count + 1);
    } else {
      stat_mgr.stat_sub(rand_area, rand_data_size, rand_use_size);
      EXPECT_EQ(stat[rand_area].data_size(), data_size - rand_data_size);
      EXPECT_EQ(stat[rand_area].use_size(), use_size - rand_use_size);
      EXPECT_EQ(stat[rand_area].item_count(), item_count - 1);
    }
  }
  printf("test end\n");

  printf("test start() and stop() start:\n");
  tair::tair_pstat *temp_stat = (tair::tair_pstat *)malloc(sizeof(tair::tair_pstat)*1024);
  memcpy(temp_stat, stat, sizeof(tair::tair_pstat)*1024);
  stat_mgr.stop();
  ASSERT_EQ(stat_mgr.start(1,file_dir), true);
  stat = stat_mgr.get_stat();
  for(int j=0; j<1024; j++){
    EXPECT_EQ(temp_stat[j].data_size(), stat[j].data_size());
    EXPECT_EQ(temp_stat[j].use_size(), stat[j].use_size());
    EXPECT_EQ(temp_stat[j].item_count(), stat[j].item_count());
  }
  free(temp_stat);
  stat_mgr.stop();
  ASSERT_EQ(remove("test_kdb_data/tair_kdb_000001.stat"), 0);
  ASSERT_EQ(stat_mgr.start(1,file_dir), true);
  stat = stat_mgr.get_stat();
  for(int j=0; j<1024; j++){
    EXPECT_EQ(stat[j].data_size(), 0);
    EXPECT_EQ(stat[j].use_size(), 0);
    EXPECT_EQ(stat[j].item_count(), 0);
  }
  printf("test end\n");
  printf("%s\n",TBSYS_CONFIG.getString("kdb","data_dir",NULL));
  remove("test_kdb_data/tair_kdb_000001.stat");

}


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
