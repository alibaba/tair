#include "kdb_manager.h"
#include "test_kdb_base.h"
#include <vector>

TEST_F(TestKdbBase, TestKdbStart)
{
  using namespace tair::common;
  tair::storage::kdb::kdb_manager *kdb_mgr = new tair::storage::kdb::kdb_manager();
  TestKdbData db;
  std::vector<int> buckets;
  for(int i = 0; i < 600; i++) {
    buckets.push_back(i);
  }
  kdb_mgr->init_buckets(buckets);
  kdb_mgr->close_buckets(buckets);
  if (kdb_mgr) {
    delete kdb_mgr;
  }
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
