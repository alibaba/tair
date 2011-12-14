package com.taobao.tairtest;
import org.junit.Test;

/**
 * @author ashu.cs
 *
 */

public class FailOverConfigServerTest2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_master_cs()
	{
		log.error("start config test Failover case 01");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");		
		

		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		
		log.error("master cs has been closed!");

		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
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
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("get data verify failure!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");
		//end test
		log.error("end config test Failover case 01");
	}
	@Test
	public void testFailover_02_restart_slave_cs()
	{
		log.error("start config test Failover case 02");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
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
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
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
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");
		//end test
		log.error("end config test Failover case 02");
	}
	@Test
	public void testFailover_03_restart_all_cs()
	{
		log.error("start config test Failover case 03");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");		

		
		//close all cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("close all cs failure!");
		
		log.error("all cs has been closed! wait for 2 seconds to restart ...");

		waitto(FailOverBaseCase.down_time);
		
		//restart all cs
		if(!batch_control_cs(csList, FailOverBaseCase.start, 0))fail("restart all cs failure!");
		log.error("Restart all cs successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");
		//end test
		log.error("end config test Failover case 03");
	}
	@Test
	public void testFailover_04_recover_master_cs_and_ds_before_rebuild()
	{
		log.error("start config test Failover case 04");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		
		log.error("Write data over!");		

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("finish put data!");
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		log.error("master cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		log.error("wait 5 seconds to restart before rebuild ...");
		
		waitto(10);
		
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)fail("Already migration!");
		//restart ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("Restart ds successful!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.error("Read data over!");
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 04");
	}
	@Test
	public void testFailover_05_recover_master_cs_when_ds_down_and_rebuild()
	{
		log.error("start config test Failover case 05");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");		

		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		log.error("master cs has been closed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("down time arrived,but no migration started!");
		log.error("migration start!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 05");
	}
	@Test
	public void testFailover_06_recover_master_cs_after_ds_down_and_rebuild()
	{
		log.error("start config test Failover case 06");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				log.error("sleep exception", e);
			}
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))
			fail("close master cs failure!");
		log.error("master cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(1)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt,getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 06");
	}
	@Test
	public void testFailover_07_recover_master_cs_when_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 07");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		log.error("master cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(1)+" log");
			waitto(3);
			if(++waitcnt>200)break;//add wait time from 5 minites to 7 minites
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
	
		int versionCount=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	
		//restart ds
		if(!control_ds((String)dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=versionCount+1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
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

		//end test
		log.error("end config test Failover case 07");
	}
	@Test
	public void testFailover_08_recover_master_cs_after_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 08");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);		
		log.error("Write data over!");		
		
		//close master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))fail("close master cs failure!");
		log.error("master cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		if(check_keyword((String)csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
			fail("version changed failed!");
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//restart ds
		if(!control_ds((String)dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration stop on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started or stoped!");
		waitcnt=0;
		log.error("down time arrived,migration stoped!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("Restart master cs successful!");
		
		//wait for system restart to work
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt,getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 08");
	}
	@Test
	public void testFailover_09_shutdown_master_cs_and_slave_cs_one_by_one_then_recover_one_by_one()
	{
		log.error("start config test Failover case 09");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successfull!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		

		//stop master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failure!");
		log.error("stop master cs successful!");
		
		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(FailOverBaseCase.down_time*2);
		
		//restart master cs
		if(!control_cs((String)csList.get(0),FailOverBaseCase.start,0))fail("restart master cs failure!");
		log.error("restart master cs successful!");
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt,getVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		//stop slave cs
		if(!control_cs((String)csList.get(1), FailOverBaseCase.stop, 0))fail("stop slave cs failure!");
		log.error("stop slave cs successful!");
		
		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(FailOverBaseCase.down_time*2);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//restart slave cs
		if(!control_cs((String)csList.get(1),FailOverBaseCase.start,0))fail("restart slave cs failure!");
		log.error("restart slave cs successful!");
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.error("Read data over!");
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//stop master cs again
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failure!");
		log.error("stop master cs successful!");
		
		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(FailOverBaseCase.down_time*2);
		
		//stop slave cs again
		if(!control_cs((String)csList.get(1), FailOverBaseCase.stop, 0))fail("stop slave cs failure!");
		log.error("stop slave cs successful!");
		
		//keep all cs down for down_time seconds
		waitto(FailOverBaseCase.down_time);
		
		//restart slave cs again
		if(!control_cs((String)csList.get(1),FailOverBaseCase.start,0))fail("restart slave cs failure!");
		log.error("restart slave cs successful!");
		
		//restart master cs again
		if(!control_cs((String)csList.get(0),FailOverBaseCase.start,0))fail("restart master cs failure!");
		log.error("restart master cs successful!");
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		log.error("end config test Failover case 09");
	}
	@Test
	public void testFailover_10_recover_slave_cs_and_ds_before_rebuild()
	{
		log.error("start config test Failover case 10");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		log.error("wait less than ds down time before rebuild ...");
		waitto(FailOverBaseCase.ds_down_time/2);
		
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==1)
			fail("Already migration!");
		//restart ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("Restart ds successful!");	
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 10");
	}
	@Test
	public void testFailover_11_recover_slave_cs_when_ds_down_and_rebuild()
	{
		log.error("start config test Failover case 11");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				log.error("sleep exception", e);
			}
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		

		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migration not started!");
		log.error("migration start!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.error("Read data over!");
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 11");
	}
	@Test
	public void testFailover_12_recover_slave_cs_after_ds_down_and_rebuild()
	{
		log.error("start config test Failover case 12");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//wait for system restart to work
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 12");
	}
	@Test
	public void testFailover_13_recover_slave_cs_when_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 13");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		

		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)
			fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

		int versionCount=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restart ds
		if(!control_ds((String)dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=versionCount+1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//wait for system restart to work
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
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

		//end test
		log.error("end config test Failover case 13");
	}
	@Test
	public void testFailover_14_recover_slave_cs_after_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.error("start config test Failover case 14");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		
		
		//close slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))fail("close slave cs failure!");
		log.error("slave cs has been closed!");
		
		waitto(2);
		
		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failure!");
		log.error("ds has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//restart ds
		if(!control_ds((String)dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check second migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration stop on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started or stoped!");
		waitcnt=0;
		log.error("down time arrived,migration stoped!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("Restart slave cs successful!");
		
		//wait for system restart to work
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt,getVerifySuccessful());
		log.error("Successfully Verified data!");	

		//end test
		log.error("end config test Failover case 14");
	}
	@Test
	public void testFailover_15_shutdown_master_cs_ds_and_slave_cs_one_by_one_then_recover_one_by_one()
	{
		log.error("start config test Failover case 15");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successfull!");
		
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("Write data over!");		
		
		//stop master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failure!");
		log.error("stop master cs successful!");
		//stop ds
		if(!control_ds((String)dsList.get(0), FailOverBaseCase.stop, 0))fail("stop ds failure!");
		log.error("stop ds sucessful!");
		
		//wait migration start
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("no migration started!");
		log.error("migration started!");
		
		//restart master cs
		if(!control_cs((String)csList.get(0),FailOverBaseCase.start,0))fail("restart master cs failure!");
		log.error("restart master cs successful!");
		
		//stop slave cs
		if(!control_cs((String)csList.get(1),FailOverBaseCase.stop,0))fail("close slave cs failure!");
		log.error("stop slave cs successful!");
		
		//wait down time for master cs work
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt,getVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		//restart slave cs
		if(!control_cs((String)csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("restart slave cs successful!");
		
		//restart ds
		if(!control_ds((String)dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failure!");
		log.error("restart ds successful!");
			
		//wait down time
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration started on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//stop master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.stop, 0))fail("stop master cs failure!");
		log.error("stop master cs successful!");
		//wait to down time
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat
		while(check_keyword((String)csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+(String)csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("down time arrived,but no migration started or finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//restart master cs
		if(!control_cs((String)csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("restart master cs successful!");
		//wait to down time
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("end config test Failover case 15");
	}
	@Test
	public void testFailover_16_close_master_cs_restart_it_when_add_a_ds_and_begin_migration()
	{
		log.error("start config test Failover case 16!");
		
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		log.error("group.conf has been changed!");
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("put data over!");
		
		//close master cs
		if(!control_cs((String) csList.get(0),FailOverBaseCase.stop, 0))fail("stop master cs failure!");
		log.error("close master cs successful!");
		
		waitto(FailOverBaseCase.down_time);
		int versionCount=check_keyword((String) csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		//start ds
		if(!control_ds((String)dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failure!");
		log.error("start the last ds successful!");

		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		//wait down time for migration
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=versionCount+1)
		{
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("check migrate start time out!");
		waitcnt=0;
		log.error("check migrate started!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("restart master cs successful!");
		
		//wait down time for cs change
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate finish time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failure!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 16");
	}
	@Test
	public void testFailover_17_close_master_cs_restart_it_after_add_a_ds_and_migration()
	{
		log.error("start config test Failover case 17!");
		
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		log.error("group.conf has been changed!");
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))fail("modify configure file failure!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))fail("modify configure file failure");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("put data over!");
		
		//close master cs
		if(!control_cs((String) csList.get(0),FailOverBaseCase.stop, 0))fail("stop master cs failure!");
		log.error("close master cs successful!");
		
		//start ds
		if(!control_ds((String)dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failure!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		//wait down time for migration
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(1), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//restart master cs
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))fail("restart master cs failure!");
		log.error("restart master cs successful!");
		
		//wait down time for cs change
		waitto(FailOverBaseCase.down_time);
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failure!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 17");
	}
	@Test
	public void testFailover_18_close_slave_cs_restart_it_when_add_a_ds_and_begin_migration()
	{
		log.error("start config test Failover case 18!");
		
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		log.error("group.conf has been changed!");
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))fail("modify configure file failure!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))fail("modify configure file failure");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("put data over!");
		
		//close slave cs
		if(!control_cs((String) csList.get(1),FailOverBaseCase.stop, 0))fail("stop slave cs failure!");
		log.error("close slave cs successful!");
		
		//start ds
		if(!control_ds((String)dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failure!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		//wait down time for migration
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("check migrate start out of time!");
		waitcnt=0;
		log.error("check migrate started!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))fail("restart slave cs failure!");
		log.error("restart slave cs successful!");
		
		//wait down time for cs change
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate finish time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failure!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 18");
	}
	@Test
	public void testFailover_19_close_slave_cs_restart_it_after_add_a_ds_and_migration()
	{
		log.error("start config test Failover case 19!");
		
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		log.error("group.conf has been changed!");
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))fail("modify configure file failure!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))fail("modify configure file failure");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("put data over!");
		
		//close slave cs
		if(!control_cs((String) csList.get(1),FailOverBaseCase.stop, 0))fail("stop slave cs failure!");
		log.error("close slave cs successful!");
		
		//start ds
		if(!control_ds((String)dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failure!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		//wait down time for migration
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate start time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//restart slave cs
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))
			fail("restart slave cs failure!");
		log.error("restart slave cs successful!");
		
		//wait down time for cs change
		waitto(FailOverBaseCase.down_time);
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failure!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		log.error("stop config test Failover case 19");
	}

	@Test
	public void testFailover_20_migrate_before_add_new_ds()
	{
		log.error("start config test Failover case 20");
		
		int waitcnt=0;
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		log.error("change group.conf successful!");
		
		//start part cluster for next migration step
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failure!");
		log.error("start cluster successful!");
		
		//wait down time for cluster to work
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))fail("modify configure file failure!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))fail("modify configure file failure");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//save put result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("put data over!");
		
		//close a ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop ds failure!");
		log.error("stop ds successful1");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate start time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify tool config file failure!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failure!");
		log.error("change group.conf successful!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate start time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failure!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");

		log.error("end config test Failover case 20");
	}
	
	////////////////////////////////////
	/////// cross different rack ///////
	////////////////////////////////////
	@Test
	public void testFailover_21_kill_master_cs_and_one_ds_on_master_rack()
	{ 
		log.error("start config test Failover case 21");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();

		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");

		//wait 10s for duplicate
		waitto(10);
		
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))
			fail("close master config server failed!");
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close data server failed!");
		log.error("master cs and one ds in master rack have been closed!");
		waitto(FailOverBaseCase.ds_down_time);

		//check migration stat of start
		if(check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migrate started!");
		
		int version_count=check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))
			fail("start data server failed!");
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))
			fail("start master config server failed!");
		log.error("master cs and one ds in master rack have been started!");
		waitto(FailOverBaseCase.down_time);
		
		if(check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1)
			fail("migrate not start!");
		log.error("check migrate started!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 21");
	}
	
	@Test
	public void testFailover_22_kill_slave_cs_and_one_ds_on_slave_rack()
	{ 
		log.error("start config test Failover case 22");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();

		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");

		//wait 5s for duplicate
		waitto(10);
		
		int versionCount=check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))
			fail("close data server failed!");
		waitto(FailOverBaseCase.ds_down_time);
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))
			fail("close slave config server failed!");
		waitto(FailOverBaseCase.down_time);
		log.error("slave cs and one ds in slave rack have been closed!");

		//check migration stat of start
		if(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=versionCount+1)
			fail("version not changed!");
		log.error("version changed!");
		
		int version_count=check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))
			fail("start slave config server failed!");
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))
			fail("start data server failed!");
		log.error("slave cs and one ds in slave rack have been started!");
		waitto(FailOverBaseCase.ds_down_time);
		
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1)
			fail("migrate not start!");
		log.error("check migrate started!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 22");
	}
	
	@Test
	public void testFailover_23_kill_master_rack()
	{
		log.error("start config test Failover case 23");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");

		waitto(10);
		
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))
			fail("close master config server failed!");
		for(int i=0; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.stop, 0))
				fail("close data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		log.error("master rack have been closed!");

		//check migration stat of start
		if(check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=dsList.size()/2)
			fail("migrate not start!");
		log.error("check migrate started!");
		
		//write data while migrating
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		waitto(10);
		
		int version_count=check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		for(int i=0; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.start, 0))
				fail("start data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))
			fail("start master config server failed!");
		log.error("master rack have been started!");
		waitto(FailOverBaseCase.down_time);
		
		if(check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+dsList.size()/2)
			fail("migrate not start!");
		log.error("check migrate started!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", getVerifySuccessful()>datacnt*1.9);
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 23");
	}
	
	@Test
	public void testFailover_24_kill_slave_rack()
	{ 
		log.error("start config test Failover case 24");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		execute_data_verify_tool();

		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");

		//wait 5s for duplicate
		waitto(10);
		
		for(int i=1; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.stop, 0))
				fail("close data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))
			fail("close slave config server failed!");
		log.error("slave rack have been closed!");

		//check migration stat of start
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=dsList.size()/2)
			fail("migrate not start!");
		log.error("check migrate started!");
		
		//write data while migrating
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		waitto(10);
		
		int version_count=check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		for(int i=1; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.start, 0))
				fail("start data server failed!");
			waitto(FailOverBaseCase.down_time);
		}
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))
			fail("start slave config server failed!");
		log.error("master rack have been started!");
		//wait 10s for data server start
		waitto(FailOverBaseCase.down_time);
		
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+dsList.size()/2)
			fail("migrate not start!");
		log.error("check migrate started!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", getVerifySuccessful()>datacnt*1.9);
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 24");
	}
	
/*	@Test
	public void testFailover_25_kill_master_rack_and_one_ds_on_slave_rack()
	{ 
		log.error("start config test Failover case 25");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		waitto(10);
		
		if(!control_cs((String) csList.get(0), FailOverBaseCase.stop, 0))
			fail("close master config server failed!");
		for(int i=0; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.stop, 0))
				fail("close data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		
		int version_count=check_keyword((String) csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrate_count=check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))
			fail("close data server in slave rack failed!");
		log.error("master rack and one ds in slave rack have been closed!");
		waitto(FailOverBaseCase.ds_down_time);

		//check migration stat of start
		if(check_keyword((String) csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1)
			fail("migrate not start!");
		if(check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+1)
			fail("migrate not start!");
		log.error("check migrate started!");
		
		//write data while migrating
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		waitto(10);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		//verify get result
		assertTrue("verify data failed!",datacnt/FailOverBaseCase.put_count_float>1.5);
		log.error("Successfully Verified data!");
		
		for(int i=0; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.start, 0))
				fail("start data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		if(!control_cs((String) csList.get(0), FailOverBaseCase.start, 0))
			fail("start master config server failed!");
		log.error("master rack have been started!");
		waitto(FailOverBaseCase.down_time);
		
		if(check_keyword((String) csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+1+dsList.size()/2)
			fail("migrate not start!");
		if(check_keyword((String) csList.get(1), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1+dsList.size()/2)
			fail("version not change!");
		log.error("check migrate started!");

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

		//verify get result
        log.error(getVerifySuccessful());
		assertTrue("verify data failed!",datacnt/FailOverBaseCase.put_count_float>1.5);
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 25");
	}
	
	@Test
	public void testFailover_26_kill_slave_rack_and_one_ds_on_master_rack()
	{ 
		log.error("start config test Failover case 26");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		execute_data_verify_tool();

		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");

		//wait 5s for duplicate
		waitto(10);
		
		for(int i=1; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.stop, 0))
				fail("close data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		if(!control_cs((String) csList.get(1), FailOverBaseCase.stop, 0))
			fail("close slave config server failed!");
		log.error("master rack and one ds in slave rack have been closed!");
		waitto(FailOverBaseCase.down_time);
		
		int version_count=check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrate_count=check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close data server in master rack failed!");
		waitto(FailOverBaseCase.ds_down_time);

		//check migration stat of start
		if(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1)
			fail("version not change!");
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+1)
			fail("migrate not start!");
		log.error("check migrate started!");
		
		//write data while migrating
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		waitto(10);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		//verify get result
		assertTrue("verify data failed!",datacnt/FailOverBaseCase.put_count_float>1.5);
		log.error("Successfully Verified data!");
		
		for(int i=1; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.start, 0))
				fail("start data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))
			fail("start slave config server failed!");
		log.error("slave rack have been started!");
		waitto(FailOverBaseCase.down_time);
		
		if(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1+dsList.size()/2)
			fail("version not change!");
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+1+dsList.size()/2)
			fail("migrate not start!");
		log.error("check migrate started!");

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

		//verify get result
        log.error(getVerifySuccessful());
		assertTrue("verify data failed!",datacnt/FailOverBaseCase.put_count_float>1.5);
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 26");
	}
	
	@Test
	public void testFailover_27_kill_one_ds_on_each_rack()
	{ 
		log.error("start config test Failover case 27");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");

		//wait for duplicate
		waitto(10);

		//check if version changed after one ds stoped on master rack
//		int version_count=check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrate_count=check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close data server on master rack failed!");
		waitto(FailOverBaseCase.ds_down_time);
		
//		if(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+1)
//			fail("version not changed after stop one ds on master rack!");
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+1)
			fail("migrate not start after stop one ds on master rack!");
		
		//check if version changed after one ds stoped on slave rack
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))
			fail("close data server on slave rack failed!");
		waitto(FailOverBaseCase.ds_down_time);
		
//		if(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+2)
//			fail("version not changed after stop one ds on slave rack!");
		if(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+2)
			fail("migrate not start after stop one ds on master rack!");
		log.error("migrate started!");
		
		//write data while migrating
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		waitto(10);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");	
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		datacnt=getVerifySuccessful();
		assertTrue("verify data failed!",datacnt/FailOverBaseCase.put_count_float>1.6);
		log.error("Successfully Verified data!");
		
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))
			fail("start ds on master rack failed!");
		waitto(FailOverBaseCase.down_time);
		if(!control_cs((String) csList.get(1), FailOverBaseCase.start, 0))
			fail("start ds on slave rack failed!");
		waitto(FailOverBaseCase.down_time);
		log.error("the stoped ds on each rack have been started!");
		
//		if(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=version_count+4)
//			fail("version not changed after restart one ds on each rack!");
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=migrate_count+4)
			fail("migrate not start after restart one ds on each rack!");
		log.error("migrate started!");

		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");

        log.error(getVerifySuccessful());
		assertTrue("verify data failed!",datacnt/FailOverBaseCase.put_count_float>1.6);
		log.error("Successfully Verified data!");

		//end test
		log.error("end config test Failover case 27");
	}*/
	
	@Test
	public void testFailover_28_accept_add_ds_without_touch()
	{
		log.error("start config test Failover case 28");
		int waitcnt=0;
		
		if(!modify_config_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", "_accept_strategy", "1"))
			fail("modify _accept_strategy failure");
		log.error("_accept_strategy has been changed!");
		
		//start cluster
		if(!control_cluster(csList, dsList.subList(0,dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failure!");

		log.error("Start Cluster Successful!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failure");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failure");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
		log.error("Write data over!");
		
		//close ds
		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))
			fail("start ds failure!");
		log.error("last data server has been started!");
		
		waitto(2);
		//check if strike migrate without touch configure file
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10) fail("down time arrived,but no migration start!");
	    waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		// check data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failure");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end config test Failover case 28");
	}

	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		reset_cluster(csList,dsList);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
            fail("modify configure file failure");
        if(!batch_modify(dsList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
            fail("modify configure file failure");
	}
	
	public void tearDown()
	{
		clean_tool("local");
		log.error("clean tool and cluster!");
		batch_uncomment(csList, FailOverBaseCase.tair_bin+"etc/group.conf", dsList, "#");
	}
}
