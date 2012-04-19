#include "interface_test_base.h"
#include <vector>
#include <string>
#include <map>
using namespace std;

namespace tair {

TEST_F(CTestInterfaceBase, mGetTestCase_01_all_exists) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(0, *pkey);
		DO_WITH_RETRY(client_handle.put(0, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(0,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
		ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_02_part_exists) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(0, *pkey);
		if(i % 2 ==0) {
			DO_WITH_RETRY(client_handle.put(0, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
		}
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(0,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_PARTIAL_SUCCESS, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
		ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_03_all_not_exists) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "mGetTestCase_03_all_not_exists_key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "mGetTestCase_03_all_not_exists_value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(0, *pkey);
//		DO_WITH_RETRY(client_handle.put(0, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
//		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(0,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_PARTIAL_SUCCESS, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
//		ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_04_areaNegative) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "mGetTestCase_04_areaNegative_key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "mGetTestCase_04_areaNegative_value_" + num;
		data_entry value(temp.c_str());

		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(-1,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;
	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_05_areaEquals1023) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(1023, *pkey);
		DO_WITH_RETRY(client_handle.put(1023, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(1023,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
		ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_06_areaEquals1024) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "mGetTestCase_06_areaEquals1024_key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "mGetTestCase_06_areaEquals1024_value_" + num;
		data_entry value(temp.c_str());

		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(1024,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_INVALID_ARGUMENT, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;
	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_07_emptyKey) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(0, *pkey);
		if(i % 2 ==0) {
			DO_WITH_RETRY(client_handle.put(0, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
		}
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}
	string temp = "";
	data_entry *pkey1=new data_entry(temp.c_str());
	keys_get.push_back(pkey1);

	int ret=client_handle.mget(0,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_ITEMSIZE_ERROR, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
		//ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_08_dataNotExpire) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(0, *pkey);
		if(i % 2 == 0) {
			DO_WITH_RETRY(client_handle.put(0, *pkey, value, 3, 0), ret, TAIR_RETURN_SUCCESS);
		}
		else {
			DO_WITH_RETRY(client_handle.put(0, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
		}
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}

	int ret=client_handle.mget(0,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
		ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

TEST_F(CTestInterfaceBase, mGetTestCase_09_dataExpired) {
	vector<data_entry *> keys_get;
	tair_keyvalue_map map_get;

	for(int i=0; i<30; i++) {
		char buf[10];
		sprintf(buf, "%d", i);
		string num = buf;

		string temp = "key_" + num;
		data_entry *pkey=new data_entry(temp.c_str());
		temp = "value_" + num;
		data_entry value(temp.c_str());

		int ret=0;
		client_handle.remove(0, *pkey);
		if(i % 2 == 0) {
			DO_WITH_RETRY(client_handle.put(0, *pkey, value, 1, 0), ret, TAIR_RETURN_SUCCESS);
		}
		else {
			DO_WITH_RETRY(client_handle.put(0, *pkey, value, 0, 0), ret, TAIR_RETURN_SUCCESS);
		}
		ASSERT_EQ(TAIR_RETURN_SUCCESS, ret);
		keys_get.push_back(pkey);
	}

	sleep(2);

	int ret=client_handle.mget(0,keys_get,map_get);
	ASSERT_EQ(TAIR_RETURN_PARTIAL_SUCCESS, ret);

	tair_keyvalue_map::iterator it = map_get.begin();
	tair_keyvalue_map::iterator itr_tmp;

	while (it != map_get.end())
	{
		itr_tmp=it++;
		data_entry * _key = (itr_tmp->first);
		data_entry * _value=(itr_tmp->second);
		ASSERT_TRUE(strcmp(_key->get_data()+4,_value->get_data()+6)==0);
        //delete(_key);
        delete(_value);
		map_get.erase(itr_tmp);
	}
	for (vector<data_entry*>::iterator j=keys_get.begin(); j!=keys_get.end(); ++j)
	{
		delete *j;
	}
}

}
