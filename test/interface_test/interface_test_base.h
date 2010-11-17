#ifndef INTERFACE_TEST_BASE_H
#include <gtest/gtest.h>
#include <boost/any.hpp>
#include "tbsys.h"
#include "tair_client_api.hpp"
#include "data_entry.hpp"
#include "define.hpp"
#define RETRY_TIMES (3)
#define DO_WITH_RETRY(opfun, ret, expected) for(int ___i = 0; ___i < RETRY_TIMES;++___i) {\
                            ret = opfun;\
                            if (ret == expected) break;\
                            sleep(1);};

#define DO_UNITLL(opfun, ret, expected) while (1) {\
                            ret = opfun;\
                            if (ret == expected) break;\
                            printf("========= expect: %d, got: %d================\n", expected, ret);\
                            sleep(1);};

                         

namespace tair{
    using namespace common;
    class CTestInterfaceBase: public testing::Test {
        public:
            void SetUp() {
            }
            void TearDown() {
            }

            static void SetUpTestCase();
            static void TearDownTestCase();
            static bool compareDataValue(const data_entry&, const data_entry&);
            static bool compareDataValueWithMeta(const data_entry&, const data_entry&);
        protected:
            static uint64_t  config_server1;
            static uint64_t  config_server2;
            static vector<uint64_t> data_server;
            static tair_client_api client_handle;
    };

    template <typename T>
    class CTestInterfaceItemBase : public CTestInterfaceBase {
	public:
	    CTestInterfaceItemBase(){ //prepare data
		    if(typeid(T) == typeid(int)){
			    _data.push_back(1);
			    _data.push_back(2);
			    _data.push_back(3);
			    _data.push_back(3);
			    _data.push_back(13);
			    _data.push_back(14);

			    _data2.push_back(3);
			    _data2.push_back(4);
			    _data2.push_back(24);
			    _data2.push_back(4);

			    _data3.push_back(5);
			    _data3.push_back(6);
			    _data3.push_back(35);
			    _data3.push_back(5);
		    }else if(typeid(T) == typeid(int64_t)){
			    _data.push_back(static_cast<int64_t>(1));
			    _data.push_back(static_cast<int64_t>(2));
			    _data.push_back(static_cast<int64_t>(3));
			    _data.push_back(static_cast<int64_t>(3));
			    _data.push_back(static_cast<int64_t>(13));
			    _data.push_back(static_cast<int64_t>(14));

			    _data2.push_back(static_cast<int64_t>(3)); 
			    _data2.push_back(static_cast<int64_t>(4)); 
			    _data2.push_back(static_cast<int64_t>(24));
			    _data2.push_back(static_cast<int64_t>(4)); 

			    _data3.push_back(static_cast<int64_t>(5)); 
			    _data3.push_back(static_cast<int64_t>(6)); 
			    _data3.push_back(static_cast<int64_t>(6)); 
			    _data3.push_back(static_cast<int64_t>(36));

		    }else if(typeid(T) == typeid(double)){
			    _data.push_back(1.15);
			    _data.push_back(2.27);
			    _data.push_back(3.38);
			    _data.push_back(-3.38);
			    _data.push_back(0.38);
			    _data.push_back(9.938);

			    _data2.push_back(3.79);
			    _data2.push_back(4.14);
			    _data2.push_back(4.14);
			    _data2.push_back(0.000);
			    
			    _data3.push_back(5.89);
			    _data3.push_back(6.90);
			    _data3.push_back(-0.90);
			    _data3.push_back(6.90);

		    }else if(typeid(T) == typeid(std::string)){
			    _data.push_back(std::string("Hi,"));
			    _data.push_back(std::string("How"));
			    _data.push_back(std::string("are"));
			    _data.push_back(std::string("lllllllllllllllllllllllll"));
			    _data.push_back(std::string("are"));
			    _data.push_back(std::string("12123"));

			    _data2.push_back(std::string("you"));
			    _data2.push_back(std::string("doing!"));
			    _data2.push_back(std::string("sfifio2ui390429049494-@!@!@#$!@#%#%^@%^@^!"));
			    _data2.push_back(std::string("end"));
			    
			    _data3.push_back(std::string("I\'m"));
			    _data3.push_back(std::string("fine"));
			    _data3.push_back(std::string("Im"));
			    _data3.push_back(std::string("345L"));

		    }
	    }
        protected:
	    std::vector<boost::any> _data;
	    std::vector<boost::any> _data2;
	    std::vector<boost::any> _data3;

	    typedef std::vector<T> List;
    };
}
#endif
