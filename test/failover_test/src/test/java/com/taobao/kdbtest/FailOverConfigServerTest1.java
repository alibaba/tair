package com.taobao.kdbtest;
import org.junit.Test;

/**
 * @author ashu.cs
 *
 */

public class FailOverConfigServerTest1 extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_master_cs()
	{
		log.error("start config test Failover case 01");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("Start Cluster Successful!");		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//start ycsb
		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		
		log.error("master cs has been closed!");

		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("get data verify failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("get data verify failure!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully verified data!");
		//end test
		log.error("end config test Failover case 01");
	}
	
	@Test
	public void testFailover_02_restart_slave_cs()
	{
		log.error("start config test Failover case 02");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

//		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		
		log.error("slave cs has been closed!");

		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		//read data from cluster
		execute_data_verify_tool();
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
		assertEquals("verify data failure!", datacnt,getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		log.error("Read data over!");
		if(waitcnt>200)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully verified data!");
		//end test
		log.error("end config test Failover case 02");
	}
	
	@Test
	public void testFailover_03_restart_all_cs()
	{
		log.error("start config test Failover case 03");
		int waitcnt=0;

		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//start ycsb
		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);
		log.error("Write data over!");		

		//close all cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("close all cs failure!");
		
		log.error("all cs has been closed! wait for 2 seconds to restart ...");

		waitto(FailOverBaseCase.down_time);
		
		//restart all cs
		if(!batch_control_cs(csList, FailOverBaseCase.start, 0))fail("restart all cs failure!");
		log.error("Restart all cs successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully verified data!");
		//end test
		log.error("end config test Failover case 03");
	}
	
	@Test
	public void testFailover_04_add_ds_and_recover_master_cs_before_rebuild()
	{
		log.error("start config test Failover case 04");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
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
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);
		log.error("finish put data!");
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failed!");
		log.error("master cs has been closed!");
		waitto(2);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration start!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");

		//stop ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("stop ds failed!");
		log.error("stop ds successful!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
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

		//end test
		log.error("end config test Failover case 04");
	}
	
	@Test
	public void testFailover_05_add_ds_and_recover_master_cs_when_rebuild()
	{
		log.error("start config test Failover case 05");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failed!");
		log.error("master cs has been closed!");
		waitto(2);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration start!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("Restart master cs successful!");
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(1)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 05");
	}
	
	@Test
	public void testFailover_06_add_ds_and_recover_master_cs_afterrebuild()
	{
		log.error("start config test Failover case 06");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failed!");
		log.error("master cs has been closed!");
		waitto(2);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(1)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("Restart master cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 06");
	}
	
	public void testFailover_07_recover_master_cs_when_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 07");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		log.error("master cs has been closed!");
		waitto(FailOverBaseCase.down_time);

		int versionCount = check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close ds
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=versionCount+1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("down time arrived,but no migration started!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
	
		//restart ds
		if(!control_ds((String)dsList_4.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("down time arrived,but no migration started!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("Read data time out!");
		waitcnt=0;	
		log.error("Read data over!");
		
		//verify get result
		assertTrue("verify data failure!", getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertEquals("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 07");
	}
	
	@Test
	public void testFailover_08_recover_master_cs_after_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 08");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);		
		log.error("Write data over!");		

		int version_count = check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		log.error("master cs has been closed!");
		//close ds
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		if(check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")==version_count)
			fail("version changed failed!");
		
		//restart ds
		if(!control_ds((String)dsList_4.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration stop on cs "+(String)csList.get(1)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or stoped!");
		waitcnt=0;
		log.error("down time arrived,migration stoped!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertTrue("verify data failure!", getVerifySuccessful()/(float)datacnt>0.75);
		assertEquals("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 08");
	}
	
	@Test
	public void testFailover_09_recover_master_cs_after_add_ds_and_rebuild_then_ds_close_again_generate_second_rebuild_rebuid()
	{
		log.error("start config test Failover case 09");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);		
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failed!");
		log.error("master cs has been closed!");
		
		waitto(2);
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");

		//close ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("close ds failed!");
		log.error("ds has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//check rebuild stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild table finished on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no rebuild started or finished!");
		}
		waitcnt=0;
		log.error("down time arrived,rebuild table finished!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("Restart master cs successful!");
		waitto(FailOverBaseCase.down_time);
		
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!",datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 09");
	}
	
	@Test
	public void testFailover_10_recover_master_cs_after_add_ds_and_rebuild_then_ds_close_again_generate_second_rebuild_after_rebuild()
	{
		log.error("start config test Failover case 10");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);		
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failed!");
		log.error("master cs has been closed!");
		
		waitto(2);
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check first migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");

		//close ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("close ds failed!");
		log.error("ds has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//check rebuild stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild table finished on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no rebuild started or finished!");
		}
		waitcnt=0;
		log.error("down time arrived,rebuild table finished!");
		waitto(FailOverBaseCase.down_time);
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("Restart master cs successful!");
		
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!",datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 10");
	}
	
	@Test
	public void testFailover_11_recover_slave_cs_and_add_ds_before_rebuild()
	{
		log.error("start config test Failover case 11");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failed!");
		log.error("slave cs has been closed!");
		waitto(FailOverBaseCase.down_time);
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failed!");
		log.error("Restart slave cs successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check first migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");

//		//restart ds
//		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("restart ds failed!");
//		log.error("Restop ds successful!");	
		
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration finished!");
		}
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failed!",datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 11");
	}
	
	@Test
	public void testFailover_12_recover_slave_cs_when_add_ds_and_rebuild()
	{
		log.error("start config test Failover case 12");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failed!");
		log.error("slave cs has been closed!");
		waitto(2);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration start!");
		
//		//restop slave cs
//		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("restop slave cs failed!");
//		log.error("Restop slave cs successful!");
		
		//wait for system restart to work
//		log.error("wait system initialize ...");
//		waitto(FailOverBaseCase.down_time);
		
		//check migration finished
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
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
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 12");
	}
	
	@Test
	public void testFailover_13_recover_slave_cs_after_add_ds_and_rebuild()
	{
		log.error("start config test Failover case 13");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failed!");
		log.error("slave cs has been closed!");
		waitto(2);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check first migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");

//		//restart ds
//		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("restart ds failed!");
//		log.error("Restop ds successful!");	
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failed!");
		log.error("Restart slave cs successful!");
		
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)fail("down time arrived,but no migration finished!");
		}
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failed!",datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//end test
		log.error("end config test Failover case 13");
	}
	
	@Test
	public void testFailover_14_recover_slave_cs_when_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 14");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		

		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		waitto(2);
		
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close ds
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")==versionCount)
		{
			log.debug("check if rebuild finished on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)
			fail("down time arrived,but no rebuild started!");
		waitcnt=0;
		log.error("down time arrived,rebuild finished!");

		//restart ds
		if(!control_ds((String)dsList_4.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("down time arrived,but no migration started!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
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
		assertTrue("verify data failure!", getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 14");
	}
	
	@Test
	public void testFailover_15_recover_slave_cs_after_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 15");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);

		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		waitto(2);
		
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		//close ds
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//check rebuild stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")==versionCount)
		{
			log.debug("check if rebuild finished on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("down time arrived,but no rebuild started!");
		waitcnt=0;
		log.error("down time arrived,rebuild finished!");
		
		//restart ds
		if(!control_ds((String)dsList_4.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration stop on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or stoped!");
		waitcnt=0;
		log.error("down time arrived,migration stoped!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//wait for system restart to work
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertTrue("verify data failure!", getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 15");
	}
	
	@Test
	public void testFailover_16_recover_slave_cs_after_add_ds_and_rebuild_then_ds_close_again_generate_second_rebuild_after_rebuild()
	{
		log.error("start config test Failover case 16");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);		
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failed!");
		log.error("slave cs has been closed!");
		
		waitto(2);
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check firse migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");

		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failed!");
		log.error("Restart slave cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");

		//close ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("close ds failed!");
		log.error("ds has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild table finished on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no rebuild started or finished!");
		}
		waitcnt=0;
		log.error("down time arrived,rebuild table finished!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 16");
	}
	
	@Test
	public void testFailover_17_add_ds_shutdown_master_cs_ds_and_slave_cs_one_by_one_then_recover_one_by_one()
	{
		log.error("start config test Failover case 17");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_6);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successfull!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		
		
		//stop master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failed!");
		log.error("stop master cs successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration started on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//restart master cs
		if(!control_cs((String)csList.get(0),FailOverBaseCase.start,0))fail("restart master cs failed!");
		log.error("restart master cs successful!");
		if(!control_cs((String)csList.get(1),FailOverBaseCase.stop,0))fail("close slave cs failed!");
		log.error("stop slave cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		//restart slave cs
		if(!control_cs((String)csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failed!");
		log.error("restart slave cs successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration started on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//stop master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failed!");
		log.error("stop master cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//restart master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("restart master cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("end config test Failover case 17");
	}
	
	@Test
	public void testFailover_18_close_ds_close_master_cs_restart_it_after_add_a_ds_and_migration()
	{
		log.error("start config test Failover case 18!");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//start part cluster 
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))fail("modify configure file failed");
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
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("put data over!");
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//stop ds
		if(!control_ds((String)dsList_4.get(dsList_4.size()-1), FailOverBaseCase.stop, 0))fail("stop ds failed!");
		log.error("ds has been closeed!");
		//wait down time for migration
		waitto(FailOverBaseCase.down_time);
		
		//check rebuild stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			waitto(2);
			if(++waitcnt>10)fail("check rebuild start time out!");
		}
		waitcnt=0;
		log.error("check rebuild finished!");
		
		//close master cs
		if(!control_cs((String) csList.get(0),FailOverBaseCase.stop, 0))fail("stop master cs failed!");
		log.error("close master cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("restart master cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//close slave cs
		if(!control_cs((String) csList.get(1),FailOverBaseCase.stop, 0))fail("stop slave cs failed!");
		log.error("close slave cs successful!");
		waitto(FailOverBaseCase.down_time);
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failed!");
		log.error("restart slave cs successful!");
		waitto(FailOverBaseCase.down_time);
		
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
		
		//verify get result
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 18");
	}
	
	@Test
	public void testFailover_19_close_master_cs_restart_it_when_close_ds_and_begin_migration()
	{
		log.error("start config test Failover case 19!");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))fail("modify configure file failed");
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
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("put data over!");
		
		
		//close master cs
		if(!control_cs((String) csList.get(0),FailOverBaseCase.stop, 0))fail("stop master cs failed!");
		log.error("close master cs successful!");
		//record the version times
		int versionCount = check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");

		//stop ds
		if(!control_ds((String)dsList_4.get(dsList_4.size()-1), FailOverBaseCase.stop, 0))fail("start ds failed!");
		log.error("close ds successful!");
		//wait down time for migration
		waitto(FailOverBaseCase.down_time);
		
		//check rebuild stat of start
		while(check_keyword((String) csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			waitto(2);
			if(++waitcnt>10)fail("check rebuild finished time out!");
		}
		waitcnt=0;
		log.error("check rebuild finished!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failed!");
		log.error("restart master cs successful!");
		
		//wait down time for cs change
		waitto(FailOverBaseCase.down_time);
		
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
		
		//verify get result
		assertTrue("verify data failed!",getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 19");
	}
	
	@Test
	public void testFailover_20_close_slave_cs_restart_it_after_close_ds_and_migration()
	{
		log.error("start config test Failover case 20!");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))fail("modify configure file failed");
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
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("put data over!");
		
		//close slave cs
		if(!control_cs((String) csList.get(1),FailOverBaseCase.stop, 0))fail("stop slave cs failed!");
		log.error("close slave cs successful!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//stop ds
		if(!control_ds((String)dsList_4.get(dsList_4.size()-1), FailOverBaseCase.stop, 0))fail("start ds failed!");
		log.error("stop ds successfuly");
		
		//wait down time for rebuild
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			waitto(2);
			if(++waitcnt>10)fail("check rebuild finished time out!");
		}
		waitcnt=0;
		log.error("check rebuild finished!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failed!");
		log.error("restart slave cs successful!");
		waitto(FailOverBaseCase.down_time);
		
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
		
		//verify get result
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/FailOverBaseCase.put_count_float>0.73);
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 20");
	}
	
	@Test
	public void testFailover_21_shutdown_master_cs_and_slave_cs_one_by_one_then_recover_one_by_one()
	{
		log.error("start config test Failover case 21");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successfull!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("Write data over!");		

		//stop master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failed!");
		log.error("stop master cs successful!");
		
		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(FailOverBaseCase.down_time*2);
		
		//restart master cs
		if(!control_cs((String)csList.get(0),FailOverBaseCase.start,0))fail("restart master cs failed!");
		log.error("restart master cs successful!");
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		//stop slave cs
		if(!control_cs((String)csList.get(1), FailOverBaseCase.stop, 0))fail("stop slave cs failed!");
		log.error("stop slave cs successful!");
		
		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(FailOverBaseCase.down_time*2);
		
		//read data from cluster
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//restart slave cs
		if(!control_cs((String)csList.get(1),FailOverBaseCase.start,0))fail("restart slave cs failed!");
		log.error("restart slave cs successful!");
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
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
		
		//stop master cs again
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failed!");
		log.error("stop master cs successful!");
		
		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(FailOverBaseCase.down_time*2);
		
		//stop slave cs again
		if(!control_cs((String)csList.get(1), FailOverBaseCase.stop, 0))fail("stop slave cs failed!");
		log.error("stop slave cs successful!");
		
		//keep all cs down for down_time seconds
		waitto(FailOverBaseCase.down_time);
		
		//restart slave cs again
		if(!control_cs((String)csList.get(1),FailOverBaseCase.start,0))fail("restart slave cs failed!");
		log.error("restart slave cs successful!");
		
		//restart master cs again
		if(!control_cs((String)csList.get(0),FailOverBaseCase.start,0))fail("restart master cs failed!");
		log.error("restart master cs successful!");
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		log.error("end config test Failover case 21");
	}
	
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		clean_point_tool("local");
		reset_cluster(csList,dsList_6);
		execute_copy_tool("local", copy_kv4_cp1);
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
