
#ifndef INVAL_LOADER_TEST_H
#define INVAL_LOADER_TEST_H

#include "inval_loader.hpp"

namespace tair {

  class inval_loader_test : public tair::InvalLoader {
  public:
    inval_loader_test(bool this_faild_abtain_group_names, bool this_faild_startup);
    ~inval_loader_test();
    void run(tbsys::CThread *thread, void *arg);
  private:
    void load_group_name();
    void do_check_client();
    //for every cluster, and building the connections with the cluster's group.
    void retrieve_group_names();
    void build_connections();
    bool failed_abtain_group_names;
    bool failed_startup;
  };
}

#endif
