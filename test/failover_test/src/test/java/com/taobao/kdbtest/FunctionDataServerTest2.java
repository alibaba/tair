package com.taobao.kdbtest;

import static org.junit.Assert.*;
import org.junit.Test;

public class FunctionDataServerTest2 extends FailOverBaseCase {

	@Test
	public void testFunction_01_add_ds_and_migration()
	{
		log.error("start function test case 01");
		
		int waitcnt=0;
		execute_copy_tool("local", conf_5);
		execute_copy_tool("local", dat_4);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList_5.subList(0, dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))fail("modify configure file failed");
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float==1);	
		log.error("put data over!");
		
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify tool config file failed!");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		log.error("get data over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("end function test case 01");
	}
	
	@Test
	public void testFunction_02_add_ds_and_migration_then_write_20w_data()
	{
		log.error("start function test case 02");
		
		int waitcnt=0;
		execute_copy_tool("local", conf_5);
		execute_copy_tool("local", dat_4);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList_5.subList(0, dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(FailOverBaseCase.down_time);

		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))fail("modify configure file failed");
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float==1);	
		log.error("put data over!");
		
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate started!");
		
		//write data while migration
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		datacnt+=getVerifySuccessful();
		log.error("put data over!");
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(5);
			if(++waitcnt>300)break;
		}
		if(waitcnt>300)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify tool config file failed!");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		log.error("get data over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("end function test case 02");
	}
	
	@Test
	public void testFunction_03_add_ds_and_migration_then_get_data()
	{
		log.error("start function test case 03");
		
		int waitcnt=0;
		execute_copy_tool("local", conf_5);
		execute_copy_tool("local", dat_4);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList_5.subList(0, dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//start ycsb
		control_kdb_ycsb(ycsb_client, start);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))fail("modify configure file failed");
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float==1);	
		log.error("put data over!");
		
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate started!");
		
		//get data while migration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify configure file failed!");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		log.error("get data over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("end function test case 03");
	}
	
	@Test
	public void testFunction_04_add_ds_and_migration_then_remove_data()
	{
		log.error("start function test case 04");
		
		int waitcnt=0;
		execute_copy_tool("local", conf_5);
		execute_copy_tool("local", dat_4);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList_5.subList(0, dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(FailOverBaseCase.down_time);
		
		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))fail("modify configure file failed");
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float==1);	
		log.error("put data over!");
		
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate started!");
		
		//remove data while migration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "rem"))fail("modify configure file failed!");
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>300)break;
		}
		if(waitcnt>300)fail("rem data time out!");
		waitcnt=0;
		//verify get result
		datacnt-=getVerifySuccessful();
		log.error("rem data over!");
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify configure file failed!");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		log.error("get data over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("end function test case 04");
	}
	
	@Test
	public void testFunction_06_recover__ds_before_rebuild_120s_noRebuild()
	{
		log.error("start function test case 06");
		int waitcnt=0;
		
//		execute_copy_tool("local", conf_4);
		execute_copy_tool("local", dat_4);
		
		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
//		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		
		log.error("Write data over!");		

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float==1);
		
		//close ds
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close ds failed!");
		log.error("ds has been closed!");
		log.error("wait to restart before rebuild ...");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		//restart ds
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.start, 0))fail("restart ds failed!");
		log.error("Restart ds successful!");	
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.error("Read data over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//wait downtime
		waitto(FailOverBaseCase.down_time);
		
		//verify no migration
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		//end test
		log.error("end function test case 06");
	}
	
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		clean_point_tool("local");
		reset_cluster(csList,dsList_6);
		execute_copy_tool("local", copy_kv4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
            fail("modify configure file failure");
	}
	
	public void tearDown()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		clean_point_tool("local");
		control_kdb_ycsb(ycsb_client, stop);
		reset_cluster(csList,dsList_6);
	}
}
