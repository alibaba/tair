#include "interface_test_base.h"
#include <stdarg.h>
namespace tair {
    tair_client_api CTestInterfaceBase::client_handle;

    void CTestInterfaceBase::SetUpTestCase() {
        tbsys::CConfig config;
        if (config.load("inte_test.conf") == EXIT_FAILURE) {
            TBSYS_LOG(ERROR, "read config file error: %s", "inte_test.conf");
            exit(0);
        }
        vector<const char*> strList = config.getStringList("test_server", "config_server");
        if (strList.size() != 2) {
            TBSYS_LOG(ERROR, "[test_server] config_server must have 2 value");
            exit(0);
        }

        TBSYS_LOG(INFO," configserver1=%s configserver2=%s\n", strList[0], strList[1]);

        string group_name = config.getString("test_server", "group_name", "group_1");
        TBSYS_LOG(INFO," got group_name =%s", group_name.c_str());
		client_handle.set_timeout(2000);

        if(!client_handle.startup(strList[0], strList[1], group_name.c_str())) {
            TBSYS_LOG(ERROR, "can not start clinet_hande");
            exit(0);
        }
        return;
    }

    void CTestInterfaceBase::TearDownTestCase() {
	TBSYS_LOG(INFO,"tear down testcase");
        client_handle.close();
    }

    bool CTestInterfaceBase::compareDataValue(const data_entry& v1, const data_entry& v2) {
        if (v1.get_size() != v2.get_size()) return false;
        return memcmp(v1.get_data(), v2.get_data(), v1.get_size()) == 0;

    }

    bool CTestInterfaceBase::compareDataValueWithMeta(const data_entry& v1, const data_entry& v2) {
        if (compareDataValue(v1,v2) == false) return false;
        return memcmp(&v1.data_meta, &v2.data_meta, sizeof(v1.data_meta)) == 0;
    }
	    
}
