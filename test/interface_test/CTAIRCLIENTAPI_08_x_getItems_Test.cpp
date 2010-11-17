#include "interface_test_base.h"
#include <string>
using testing::Types;
using namespace boost;
using namespace std;

namespace tair {
	
typedef ::testing::Types<int32_t,int64_t,double,std::string> ItemTypes;
//typedef ::testing::Types<int64_t> ItemTypes;

TYPED_TEST_CASE(CTestInterfaceItemBase,ItemTypes);

TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_01_dataNotExist)
{
	data_entry key1("ItemTestCase_01_key_01");
	data_entry key2("ItemTestCase_01_key_02");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(ret,TAIR_RETURN_SUCCESS);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(ret,TAIR_RETURN_SUCCESS);

	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(1,key2,0,tair_client_api::ALL_ITEMS,result_3);
    ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_01_1dataNotExist)
{

	
	data_entry key1("ItemTestCase_08_key_01_1");
	typename TestFixture::List result_1;

	int ret = CTestInterfaceBase::client_handle.get_items(-1,key1,0,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
    ASSERT_TRUE(result_1.empty());

}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_01_2dataNotExist_0area)
{

	
	data_entry key1("ItemTestCase_08_key_01_2");
	typename TestFixture::List result_1;

	int ret = CTestInterfaceBase::client_handle.get_items(0,key1,0,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
    ASSERT_TRUE(result_1.empty());

}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_01_3dataNotExist_1023area)
{

	
	data_entry key1("ItemTestCase_08_key_01_3");
	typename TestFixture::List result_1;

	int ret = CTestInterfaceBase::client_handle.get_items(1023,key1,0,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST, ret);
    ASSERT_TRUE(result_1.empty());

}

TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_01_4dataNotExist_1024area)
{

	
	data_entry key1("ItemTestCase_08_key_01_4");
	typename TestFixture::List result_1;

	int ret = CTestInterfaceBase::client_handle.get_items(1024,key1,0,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
    ASSERT_TRUE(result_1.empty());

}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_01_5dataNotExist_65536area)
{

	
	data_entry key1("ItemTestCase_08_key_01_5");
	typename TestFixture::List result_1;

	int ret = CTestInterfaceBase::client_handle.get_items(65536,key1,0,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
    ASSERT_TRUE(result_1.empty());

}

TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_02)
{
	data_entry key1("ItemTestCase_02_key_01");
	data_entry key2("ItemTestCase_02_key_02");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key2,value_3,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(1,key2,0,1,result_3);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(*value_3.begin(), *result_3.begin());

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(1,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_03)
{
	data_entry key1("ItemTestCase_03_key_01");
	data_entry key2("ItemTestCase_03_key_02");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_3.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key2,value_3,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(1,key2,1,2,result_3);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(2,result_3.size());

    typename TestFixture::List::iterator v1=value_3.begin();
    v1++;
    typename TestFixture::List::iterator s1=result_3.begin();

    for(int i=0; i<2; ++i) {
        ASSERT_EQ(*(v1++), *(s1++));
    }

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(1,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_04)
{
	data_entry key1("ItemTestCase_04_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_3.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,-1,2,result_1);
    ASSERT_EQ(1,result_1.size());
    typename TestFixture::List::iterator v1 = value_1.end();
    v1--;
	ASSERT_TRUE(*v1 == *(result_1.begin()));

	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_05)
{
	data_entry key1("ItemTestCase_05_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_3.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

    int off_set = value_1.size() + 3;

	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,off_set,1,result_1);
    ASSERT_EQ(TAIR_RETURN_OUT_OF_RANGE,ret);
    ASSERT_TRUE(result_1.empty());
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_06)
{
	data_entry key1("ItemTestCase_06_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(4,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,1,3,result_1);
    ASSERT_EQ(3,result_1.size());
    typename TestFixture::List::iterator v1=value_1.begin();
    v1++;
    typename TestFixture::List::iterator s1=result_1.begin();

    for(int i=0; i<3; ++i) {
        ASSERT_EQ(*(v1++), *(s1++));
    }

	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_07)
{
	data_entry key1("ItemTestCase_07_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(4,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,1,value_1.size(),result_1);
    ASSERT_EQ(value_1.size() - 1,result_1.size());
    typename TestFixture::List::iterator v1=value_1.begin();
    v1++;
    typename TestFixture::List::iterator s1=result_1.begin();

    for(int i=0; i<value_1.size() - 1; ++i) {
        ASSERT_EQ(*(v1++), *(s1++));
    }

	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_08) {
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_09) {
	data_entry key1("ItemTestCase_09_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}



	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);

	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,0,0,result_1);
    ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_10)
{
	data_entry key1("ItemTestCase_10_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,2,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(value_1.size() - 2,result_1.size());
    typename TestFixture::List::iterator v1=value_1.begin();
    v1++;
    v1++;
    typename TestFixture::List::iterator s1=result_1.begin();

    for(int i=0; i<value_1.size() - 2; ++i) {
        ASSERT_EQ(*(v1++), *(s1++));
    }

	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_11)
{
	data_entry key1("ItemTestCase_11_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,-2,tair_client_api::ALL_ITEMS,result_1);
    ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
    ASSERT_EQ(2,result_1.size());
    typename TestFixture::List::iterator v1=value_1.end();
    v1--;
    v1--;
    typename TestFixture::List::iterator s1=result_1.begin();

    for(int i=0; i<2; ++i) {
        ASSERT_EQ(*(v1++), *(s1++));
    }

	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_12)
{
	data_entry key1("ItemTestCase_12_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,1,-1,result_1);
    ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item08_x_getItems_13)
{
	data_entry key1("ItemTestCase_13_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
		for(uint32_t i=0;i < this->_data3.size();++i){
			value_3.push_back(any_cast<TypeParam> (this->_data3[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
    sleep(2);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,0,1,result_1);
   	ASSERT_EQ(TAIR_RETURN_DATA_EXPIRED,ret);
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
}
} /* tair */
