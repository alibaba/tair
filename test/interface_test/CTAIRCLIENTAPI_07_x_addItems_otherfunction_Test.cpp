#include "interface_test_base.h"
#include <string>
using namespace std;
namespace tair {


TEST_F(CTestInterfaceBase, addItemsTestCase_other_01) 
{
	data_entry key1("07_addItemsTestCase_Other_key_01");

	vector<int> items;
	for(int i=0;i<3;++i){
		items.push_back(i);
	}

	vector<int> result_int;
	client_handle.remove(1,key1);

	//setp 1
	//data : [0,1,2]
	int ret = client_handle.add_items(1,key1,items,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	//setp 2
	int retCount = 0;
	ret = client_handle.incr(1,key1,5,&retCount);
	ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE,ret);

	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( items == result_int );
	result_int.clear();

	//setp 3
	ret = client_handle.decr(1,key1,4,&retCount);
	ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE,ret);

	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( items == result_int );
	result_int.clear();


	//setp 4
	data_entry *p = 0;
	ret = client_handle.get(1,key1,p);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	delete p; 
	p = 0;

	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( items == result_int );
	result_int.clear();


	//setp 5
	vector<double> items_d;
	items_d.push_back(7.8);
	items_d.push_back(-8.1);
	ret = client_handle.add_items(1,key1,items_d,0,0);
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( items == result_int );
	result_int.clear();

	//step 6
	vector<int> items1;
	items1.push_back(1);
	ret = client_handle.add_items(1,key1,items1,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	items.push_back(1);
	
	//setp 7
	vector<std::string> result_str;
	ret = client_handle.get_items(1,key1,1,1,result_str);
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( items == result_int );
	result_int.clear();

	//setp 8
	//data:
	//before : 0 1 2 1
	//after :  0 2 1
	ret = client_handle.get_and_remove(1,key1,1,1,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_EQ(1,result_int.size());
	ASSERT_EQ(1,result_int[0]);
	result_int.clear();

	//step 9
	//data:0 2 1 
	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_EQ(0,result_int[0]);
	ASSERT_EQ(2,result_int[1]);
	ASSERT_EQ(1,result_int[2]);

	result_int.clear();

	//setp 10
	ret = client_handle.get_items_count(1,key1);
	ASSERT_EQ(3,ret);

	//setp 11
	//data:
	//before 0 2 1
	//after 0 1
	ret = client_handle.remove_items(1,key1,1,1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_TRUE( result_int.size() == 2 );
	ASSERT_EQ(0,result_int[0]);
	ASSERT_EQ(1,result_int[1]);

	result_int.clear();

	//setp 12
	ret = client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,0);

	ret = client_handle.get_items(1,key1,0,1,result_int);
	ASSERT_EQ(TAIR_RETURN_DATA_NOT_EXIST,ret);

	//step 13
	//data : 0 1
	ret = client_handle.add_items(1,key1,items,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	//step 14
	//data :0
	data_entry value("07_01_data_01");
	ret = client_handle.put(1,key1,value,0,0);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);

	ret = client_handle.get(1,key1,p);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,ret);
	ASSERT_EQ(true,compareDataValue(value, *p));
	delete p; 
	p = 0;

	//step 15
	ret = client_handle.add_items(1,key1,items,0,0);
	ASSERT_EQ(TAIR_RETURN_CANNOT_OVERRIDE,ret);

	//step 16
	ret = client_handle.get_items(1,key1,0,tair_client_api::ALL_ITEMS,result_int);
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	//step 17
	ret = client_handle.get_and_remove(1,key1,1,1,result_int);
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	//setp 18
	ret = client_handle.get_items_count(1,key1);
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	//step 19
	ret = client_handle.remove_items(1,key1,1,1);
	ASSERT_EQ(TAIR_RETURN_TYPE_NOT_MATCH,ret);

	//step 20
	ret = client_handle.remove(1,key1);
	ASSERT_EQ(TAIR_RETURN_SUCCESS,0);

}

	

}
