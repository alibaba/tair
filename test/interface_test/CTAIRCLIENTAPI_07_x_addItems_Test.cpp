#include "interface_test_base.h"
#include <string>
using testing::Types;
using namespace boost;
using namespace std;

namespace tair {
	
typedef ::testing::Types<int32_t,int64_t,double,std::string> ItemTypes;
//typedef ::testing::Types<int32_t> ItemTypes;

TYPED_TEST_CASE(CTestInterfaceItemBase,ItemTypes);
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_dataNotExist)
{
	data_entry key("ItemTestCase_01_key_01");
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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key2,value_3,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(1,key2,0,tair_client_api::ALL_ITEMS,result_3);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_3 == result_3);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(1,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_1_dataNotExist)
{
	data_entry key("ItemTestCase_01_1_key_01");
	data_entry key2("ItemTestCase_01_1_key_02");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	CTestInterfaceBase::client_handle.remove(1,key);
	CTestInterfaceBase::client_handle.remove(2,key2);

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	typename TestFixture::List value_3; //null

	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key2,value_3,0,0); //null
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);
	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(1,key2,0,tair_client_api::ALL_ITEMS,result_3);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_2_dataNotExist)
{
	data_entry key("ItemTestCase_01_2_key_01");
	data_entry key2("ItemTestCase_01_2_key_02");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;
	CTestInterfaceBase::client_handle.remove(1,key);
	CTestInterfaceBase::client_handle.remove(2,key2);

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	data_entry key3(""); //NULL
	
	ret = CTestInterfaceBase::client_handle.add_items(1,key3,value_3,0,0);
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);
	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(1,key3,0,tair_client_api::ALL_ITEMS,result_3);
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_3_dataNotExist)
{
	data_entry key("ItemTestCase_01_3_key_01");
	data_entry key2("ItemTestCase_01_3_key_02");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;
	CTestInterfaceBase::client_handle.remove(1,key);
	CTestInterfaceBase::client_handle.remove(2,key2);

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(2,key2,value_2,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	//area  < 0
	ret = CTestInterfaceBase::client_handle.add_items(-1,key2,value_3,0,0);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);
	
	typename TestFixture::List result_1;
	typename TestFixture::List result_2;
	typename TestFixture::List result_3;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_1 == result_1);

	ret = CTestInterfaceBase::client_handle.get_items(2,key2,0,tair_client_api::ALL_ITEMS,result_2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_2 == result_2);

	ret = CTestInterfaceBase::client_handle.get_items(-1,key2,0,tair_client_api::ALL_ITEMS,result_3);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ret = CTestInterfaceBase::client_handle.remove(2,key2);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_4_0area)
{
	data_entry key("ItemTestCase_01_4_key_01");

	typename TestFixture::List value;


	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(0,key,value,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	
	typename TestFixture::List result;

	ret = CTestInterfaceBase::client_handle.get_items(0,key,0,tair_client_api::ALL_ITEMS,result);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value == result);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(0,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_5_1023area)
{
	data_entry key("ItemTestCase_01_5_key_01");

	typename TestFixture::List value;


	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1023,key,value,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	
	typename TestFixture::List result;

	ret = CTestInterfaceBase::client_handle.get_items(1023,key,0,tair_client_api::ALL_ITEMS,result);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value == result);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1023,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_6_1024area)
{
	data_entry key("ItemTestCase_01_6_key_01");

	typename TestFixture::List value;


	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1024,key,value,0,0);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);
	
	typename TestFixture::List result;

	ret = CTestInterfaceBase::client_handle.get_items(1024,key,0,tair_client_api::ALL_ITEMS,result);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1024,key);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

}


TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_01_7_65536area)
{
	data_entry key("ItemTestCase_01_7_key_01");

	typename TestFixture::List value;


	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(65536,key,value,0,0);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);
	
	typename TestFixture::List result;

	ret = CTestInterfaceBase::client_handle.get_items(65536,key,0,tair_client_api::ALL_ITEMS,result);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(65536,key);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

}


TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_02_dataNotExist)
{
	data_entry key("ItemTestCase_02_key_01");

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_1 == result_1);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_03_versionEquals)
{
	data_entry key("ItemTestCase_03_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,10,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,1,0,30);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(result_1.size() == value_1.size() + value_2.size());

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_04_versionBig)
{
	data_entry key("ItemTestCase_04_key_01");

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

		for(uint32_t i=0;i < this->_data2.size();++i){
			value_2.push_back(any_cast<TypeParam> (this->_data2[i]));
		}
	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,10,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,10,0,20); //version > current version
	ASSERT_EQ(TAIR_RETURN_VERSION_ERROR,ret);
	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(result_1 == value_1);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_05_versionLess)
{
	data_entry key("ItemTestCase_05_key_01");

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,5);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,1,0,5);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_3,1,10,10); //version < current version
	ASSERT_EQ(TAIR_RETURN_VERSION_ERROR,ret);


	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( result_1[0] == value_1[value_1.size() - 1]);
	for(uint32_t i=1;i<result_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_2[i-1]);
	}

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_06_maxCountBig)
{
	data_entry key("ItemTestCase_06_key_01");

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,0,20); //max_count > value_2.size()
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_1[i]);
	}
	for(uint32_t j=value_1.size();j<value_1.size()+value_2.size();++j){
		ASSERT_TRUE( result_1[j] == value_2[j-value_1.size()]);
	}
	
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_07_maxcountless)
{
	data_entry key("ItemTestCase_07_key_01");

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,0,value_2.size()-1); //max_count < value_2.size()
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=1;i<result_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_2[i+1]);
	}

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_08_maxcountEqual)
{
	data_entry key("ItemTestCase_08_key_01");

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,0,value_2.size()); //max_count == value_2.size()
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	uint32_t i =0;
	for(i=0;i<result_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_2[i]);
	}
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_09_maxcount0)
{
	data_entry key("ItemTestCase_09_key_01");

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,0,0); //max_count == 0
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_1[i]);
	}
	
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_10_KeyTest)
{
	data_entry key("ItemTestCase_10_key_01"); //key < 1K

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_1[i]);
	}
	

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_11_KeyTest)
{
	char key_buf[2048];
	memset(key_buf,'A',2048);
	data_entry key(key_buf,2048,true); //key > 1K

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_12_KeyTest)
{
	char key_buf[1024];
	memset(key_buf,'B',1024);
	data_entry key(key_buf,1024,true); //key == 1K

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10);
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_13_DataTest)
{
	data_entry key("ItemTestCase_13_key_01"); 

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;
	typename TestFixture::List value_3;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,10); //data < 1000K
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_1[i]);
	}

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
}
TEST_F(CTestInterfaceBase,addItemsTestCase_14_DataTest)
{
	data_entry key("ItemTestCase_14_key_01"); 

	std::vector<std::string> value_1;

	try {
		for(uint32_t j=0;j < 65535;++j){
			value_1.push_back(std::string("abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz"));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65535); //data =1000K
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);

	std::vector<std::string> result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);


	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);
}
TEST_F(CTestInterfaceBase,addItemsTestCase_15_DataTest)
{
	data_entry key("ItemTestCase_15_key_01"); 

	std::vector<std::string> value_1;

	try {
		for(uint32_t j=0;j < 65535;++j){
			value_1.push_back(std::string("abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyzdddddddddddd"));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65535); //data >1000K
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);

	std::vector<std::string> result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);


	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);
}

TEST_F(CTestInterfaceBase,addItemsTestCase_16_DataTest)
{
	data_entry key("ItemTestCase_16_key_01"); 

	std::vector<std::string> value_1;

	try {
		for(uint32_t j=0;j < 30000;++j){
			value_1.push_back(std::string("abcdefghijklmno"));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65535); //data <1000K
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	std::vector<std::string> result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65535); //add new data ,total>1000K
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR,ret);

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_1[i]);
	}
	
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_17_itemCountTest)
{
	data_entry key("ItemTestCase_17_key_01"); 

	typename TestFixture::List value_1;
	typename TestFixture::List value_2;

	try {
		for(uint32_t i=0;i < 65535;++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[0]));
		}
		value_2.push_back(any_cast<TypeParam> (this->_data[1]));

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}
	CTestInterfaceBase::client_handle.remove(1,key);
	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65535); //itemcount=65535
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,0,65536);//itemcount=65536
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_EQ(65535,result_1.size());

	for(uint32_t i=1;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i-1] == value_1[i]);
	}
	ASSERT_TRUE( result_1[65534] == value_2[0]);
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	
}

TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_18_dataExpired)
{
	data_entry key("ItemTestCase_18_key_01");

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


	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,3);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	sleep(4); //expired
	
	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_TRUE(TAIR_RETURN_SUCCESS != ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,10);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE(value_2 == result_1);

	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_19_maxCountTest)
{
	data_entry key("ItemTestCase_19_key_01"); 

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < 65536;++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[0]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}
	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65536); //itemcount=65536
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size()-1;++i){
		ASSERT_TRUE( result_1[i] == value_1[i+1]);
	}
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_20_expiretimeTest)
{
	data_entry key("ItemTestCase_20_key_01"); 

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}
	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65536); 
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,1,-1,65536); //expiretime=-1
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	for(uint32_t i=0;i<value_1.size();++i){
		ASSERT_TRUE( result_1[i] == value_1[i]);
	
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
	}
}
TYPED_TEST(CTestInterfaceItemBase,addItemsTestCase_21_expiretimeTest)
{
	data_entry key("ItemTestCase_21_key_01"); 

	typename TestFixture::List value_1;

	try {
		for(uint32_t i=0;i < this->_data.size();++i){
			value_1.push_back(any_cast<TypeParam> (this->_data[i]));
		}

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}
	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,-1,65536); //expiretime=-1
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT,ret);

	typename TestFixture::List result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);
	
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
}
TEST_F(CTestInterfaceBase,addItemsTestCase_22_DataTypeTest)
{
	data_entry key("ItemTestCase_22_key_01"); 

	std::vector<int> value_1;
	std::vector<int64_t> value_2;
	std::vector<double> value_3;
	std::vector<std::string> value_4;

	try {
		value_1.push_back(1);
		value_2.push_back(static_cast<int64_t>(1));
		value_3.push_back(-3.38);
		value_4.push_back(std::string("adb"));

	}catch(bad_any_cast& e){
		cout << e.what() << endl;
		exit(-1);
	}

	int ret = CTestInterfaceBase::client_handle.add_items(1,key,value_1,0,0,65535); //add int32 data
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	std::vector<int> result_1;

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( result_1[0] == value_1[0]);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_2,0,0,65535); //add int64 data
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( result_1[0] == value_1[0]);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_3,0,0,65535); //add double data
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);
	
	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( result_1[0] == value_1[0]);

	ret = CTestInterfaceBase::client_handle.add_items(1,key,value_4,0,0,65535); //add string data
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);
	
	ret = CTestInterfaceBase::client_handle.get_items(1,key,0,tair_client_api::ALL_ITEMS,result_1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( result_1[0] == value_1[0]);
	
	//clean
	ret = CTestInterfaceBase::client_handle.remove(1,key);
}

} /* tair */







