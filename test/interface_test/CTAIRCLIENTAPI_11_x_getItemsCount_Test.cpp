#include "interface_test_base.h"
#include <string>
using testing::Types;
using namespace boost;
using namespace std;

namespace tair {
	
typedef ::testing::Types<int32_t,int64_t,double,std::string> ItemTypes;
//typedef ::testing::Types<int64_t> ItemTypes;

TYPED_TEST_CASE(CTestInterfaceItemBase,ItemTypes);

TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_01_dataNotExist)
{
	data_entry key1("ItemTestCase_11_key_01");
	data_entry key2("ItemTestCase_11_key_02");

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

	ret = CTestInterfaceBase::client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items_count(1,key2);
    ASSERT_EQ(0, ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_02_dataNotExist_areaNegative)
{

	
	data_entry key1("ItemTestCase_11_key_02_1");

	int ret = CTestInterfaceBase::client_handle.get_items_count(-1,key1);
    ASSERT_EQ(0, ret);

}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_03_dataNotExist_areaEquals0)
{

	
	data_entry key1("ItemTestCase_11_key_03_2");

	int ret = CTestInterfaceBase::client_handle.get_items_count(0,key1);
    ASSERT_EQ(0, ret);

}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_04_dataNotExist_areaEuals1024Max)
{

	
	data_entry key1("ItemTestCase_11_key_04_1");
	typename TestFixture::List result_1;

	int ret = CTestInterfaceBase::client_handle.get_items_count(1024,key1);
    ASSERT_EQ(0, ret);

}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_04_dataNotExist_areaEuals1025)
{

	
	data_entry key1("ItemTestCase_11_key_04_2");

	int ret = CTestInterfaceBase::client_handle.get_items_count(1025,key1);
    ASSERT_EQ(0, ret);

}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_05_dataNotExist_keyNull)
{

	
	data_entry key1;

	int ret = CTestInterfaceBase::client_handle.get_items_count(1025,key1);
    ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);

}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_06_dataExist)
{
	data_entry key1("ItemTestCase_11_key_06_1");
	data_entry key2("ItemTestCase_11_key_06_2");

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

	ret = CTestInterfaceBase::client_handle.get_items_count(1,key2);
    ASSERT_EQ(4, ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(1,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_07_dataExpired)
{
	data_entry key1("ItemTestCase_11_key_07");

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
    sleep(2);

	ret = CTestInterfaceBase::client_handle.get_items_count(1,key1);
   	ASSERT_EQ(0,ret);
	
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_08_itemsNum65535)
{
	data_entry key1("ItemTestCase_11_key_08");

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < 65535;++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[0]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0,65535);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.get_items_count(1,key1);
   	ASSERT_EQ(65535,ret);
	
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
}
TYPED_TEST(CTestInterfaceItemBase,Item11_x_getItemsCount_09_itemsNum65536)
{
	data_entry key1("ItemTestCase_11_key_08");

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < 65536;++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[0]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

    ASSERT_LE(3,value_1.size());


	int ret = CTestInterfaceBase::client_handle.add_items(1,key1,value_1,0,0,65536);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.get_items_count(1,key1);
   	ASSERT_EQ(65535,ret);
	
	ret = CTestInterfaceBase::client_handle.remove(1,key1);
}


} /* tair */

