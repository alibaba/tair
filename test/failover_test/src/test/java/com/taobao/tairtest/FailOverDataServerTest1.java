package com.taobao.tairtest;

import java.util.*;

import org.junit.Test;

import com.ibm.staf.STAFResult;

import junit.framework.Assert;

/**
 * author fanggang
 * dataserver testcse
 **/
public class FailOverDataServerTest1 extends FailOverBaseCase{
	
	@Test
	public void testFailover_01_add_ds_stop_in_time()
	{
		log.error("start DataServer test Failover case 01");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
        log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		log.error("Start Cluster Successful!");
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		
		//save put result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);	
		log.error("finish put data!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been started!");
		log.error("wait 2 seconds to restart before rebuild ...");

		waitto(2);
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		
		//stop the data server again
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop data server failed!");
		log.error("stop ds successful!");
		
		//wait 10s for data server start
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
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		//end test
		log.error("end DataServer test Failover case 01");
	}
	
	@Test
    public void testFailover_02_add_one_ds()
    {   
		log.error("start DataServer test Failover case 02");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been added!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 02");
    }
    
	@Test
    public void testFailover_03_add_ds_close_in_migrate()
    { 
    	log.error("start DataServer test Failover case 03");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
        
		log.error("wait system initialize ...");
	    waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "500000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(6);
			if(++waitcnt>100)break;
		}

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/500000.0>0.9);
		log.error("Write data over!");
		
		//wait 30s for duplicate
		waitto(30);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been added!");
       
	  	//wait for migrate start
		waitto(FailOverBaseCase.down_time);
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		
	    if(waitcnt>150) fail("donw time arrived,but no migration start!");
	    waitcnt=0;
		log.error("donw time arrived,migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("restop ds failed!");
		log.error("restop ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no rebuild finished!");
    	waitcnt=0;
		log.error("donw time arrived,rebuild finished!");
		
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/500000.0>0.9);
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 03");
      
    }
    
	@Test
    public void testFailover_04_add_ds_reclose_after_migrate()
    {
    	log.error("start DataServer test Failover case 04");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");  
		log.error("wait system initialize ...");
		
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}	

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 10s for duplicate
		waitto(10);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
       
       
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		//restop ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("restart ds failed!");
		log.error("ds has been stoped");
		
		//wait 120s for data server stop
		waitto(FailOverBaseCase.down_time);
		

		// wait second rebuild finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin + "logs/config.log") != (versionCount+1)) {
			log.debug("check if rebuild finish on cs " + (String) csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.error("donw time arrived,the seccond rebuild finished!");
		
		// check data
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.79);
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 04");
    
    }
	@Test
    public void testFailover_05_add_two_ds()
    {
    	log.error("start DataServer test Failover case 05");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");  
		log.error("wait system initialize ...");
		
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}	

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 10s for duplicate
		waitto(10);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
       
       
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		// check data
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 05");
    
    }
    //first add one ,and then add another one in migrate
	@Test
    public void testFailover_06_add_one_inMigrate_add_another_ds()
    {
    	log.error("start DataServer test Failover case 06");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
       

		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
       
		waitto(FailOverBaseCase.down_time);

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
        if(waitcnt>210) fail("donw time arrived,but no migration finished!");
        waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		//migrate need check data 
		if (!modify_config_file("local", FailOverBaseCase.test_bin + "DataDebug.conf", "actiontype", "get"))
			fail("modify configure file failed");
		execute_data_verify_tool();
		
		// check verify
		while (check_process("local", "DataDebug")!=2)
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
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 06");
    }
    
  //first add one , then add another one and restart it before second migrate
	@Test
    public void testFailover_07_add_one_inMigrate_inTime()
    {
    	log.error("start DataServer test Failover case 07");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");

		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration started!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
		
		log.error("wait 3 seconds to reclose the secord ds  before rebuild ...");
		waitto(3);
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)fail("Already migration!");
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("start data server failed!");
		log.error("second data server has been close");
		
		waitto(FailOverBaseCase.down_time);
		
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration finised!");
        waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 07");
    }
    
    //first add one ,and then add another one, and reclose first one
	@Test
    public void testFailover_08_add_one_inMigrate_reCloseFirst()
    {
    	log.error("start DataServer test Failover case 08");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration started!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
        
		//reclose first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
        if(waitcnt>210) fail("donw time arrived,but no rebuild finished!");
        waitcnt=0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.8);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 08");
    }
    
    //add another data server ,and reclose both in migrate
	@Test
    public void testFailover_09_add_one_inMigrate_reCloseBoth()
    {
    	log.error("start DataServer test Failover case 09");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration started!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
        
		//reclose first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		
		//reclose second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
	
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no rebuild finished!");
        waitcnt = 0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.8);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 09");	
    }
    
    //add one data server ,and then migrate,add another， reclose first data server
	@Test
    public void testFailover_10_add_one_afterMigrate_recloseFirst()
    {
    	log.error("start DataServer test Failover case 10");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
        
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for second migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration started!");
		waitcnt = 0;
		log.error("donw time arrived,the second migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
	    if(waitcnt>150) fail("donw time arrived,but no second rebuild finished!");
	    waitcnt = 0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.75);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 10");
    	
    }
    
    //add another data server ,and after second migrate, restop second data server
	@Test
    public void testFailover_11_add_one_afterMigrate_restartSecond()
    {
    	log.error("start DataServer test Failover case 11");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
		   if(++waitcnt>150)break;
		}	
        if(waitcnt>150) fail("donw time arrived,but no migration started!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for second migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no second migration started!");
        waitcnt = 0;
		log.error("donw time arrived, the second migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no second rebuild finished!");
        waitcnt = 0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		log.error("Read data over");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.99);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 11");
    	
    }
    
    //add another data server ,and after second migrate, restop both
	@Test
    public void testFailover_12_add_one_afterMigrate_reStopBoth()
    {
    	log.error("start DataServer test Failover case 12");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("firse data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration started!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
        
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for second migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration started!");
		waitcnt = 0;
		log.error("donw time arrived,the second migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stop!");
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stop!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second rebuild finished!");
		waitcnt = 0;
		log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.74);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 12");
    	
    }
  //add another data server,and after migrate finish, restop second data server
	@Test
    public void testFailover_13_add_one_finishMigrate_reStopSecond()
    {
    	log.error("start DataServer test Failover case 13");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		log.error("first data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for first migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no second migration started!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("second data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no first migration finished!");
		waitcnt = 0;
		log.error("donw time arrived,migration finished!");
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait for  rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check fourth if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no fourth rebuild finished!");
		waitcnt = 0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.99);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 13");
    	
    }
    
    //kill another data server,and after migrate finish, restart both
	@Test
    public void testFailover_14_add_one_finishMigrate_restartBoth()
    {
    	log.error("start DataServer test Failover case 14");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		log.error("first data server has been start!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
	    if(waitcnt>150) fail("donw time arrived,but no migration started!");
	    waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		log.error("second data server has been start!");
        
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for first migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("donw time arrived,migration finished!");
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait for fourth rebuild finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin + "logs/config.log") != (versionCount+1)) {
			log.debug("check fourth rebuild finish on cs " + (String) csList.get(0) + " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no fourth rebuild finished!");
		waitcnt = 0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.6);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 14");
    }
    
  //在迁移过程中关闭另一台ds
	@Test
    public void testFailover_15_add_ds_close_another_in_migrate()
    { 
    	log.error("start DataServer test Failover case 15");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
        
		log.error("wait system initialize ...");
	    waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "500000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(6);
			if(++waitcnt>100)break;
		}

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 30s for duplicate
		waitto(30);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been added!");
       
	  	//wait for migrate start
		waitto(FailOverBaseCase.down_time);
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		
	    if(waitcnt>150) fail("donw time arrived,but no migration start!");
	    waitcnt=0;
		log.error("donw time arrived,migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop ds
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("restop another ds failed!");
		log.error("restop another ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no rebuild finished!");
    	waitcnt=0;
		log.error("donw time arrived,rebuild finished!");
		
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.74);
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 15");
      
    }
  //在迁移过程中关闭另一台ds,restart ontime
	@Test
    public void testFailover_16_add_ds_close_another_in_migrate_restart_ontime()
    { 
    	log.error("start DataServer test Failover case 16");
		int waitcnt=0;
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
        
		log.error("wait system initialize ...");
	    waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write 100k data to cluster
		execute_data_verify_tool();
		
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(6);
			if(++waitcnt>100)break;
		}

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 30s for duplicate
		waitto(30);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		log.error("first data server has been added!");
       
	  	//wait for migrate start
		waitto(FailOverBaseCase.down_time);
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		
	    if(waitcnt>150) fail("donw time arrived,but no migration start!");
	    waitcnt=0;
		log.error("donw time arrived,migration started!");
		
		//restop ds
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("restop another ds failed!");
		log.error("restop another ds successful!");
		
		waitto(2);
		
		//restart ds
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("restart another ds failed!");
		log.error("restart another ds successful!");
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no migration finished!");
    	waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		//migrate need check data 
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 16");
      
    }
  //first kill one ,restart it before build table
	@Test
    public void testFailover_17_kill_one_restart_inTime()
    {
    	log.error("start DataServer test Failover case 17");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");

		waitto(2);
		
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		
		//restart  data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("data server has been restarted!");
		
		waitto(FailOverBaseCase.down_time);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 17");
    }
  //first kill one 
	@Test
    public void testFailover_18_kill_one_ds()
    {
    	log.error("start DataServer test Failover case 18");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");

		waitto(FailOverBaseCase.down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no rebuild finished!");
    	waitcnt=0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.79);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 18");
    }
    //first kill one ,add ds before build table
	@Test
    public void testFailover_19_kill_one_add_ds_before_rebuild()
    {
    	log.error("start DataServer test Failover case 19");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(0), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		//wait 5s for duplicate
		waitto(5);
		
		//close one data server
		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");
		int versionCount=check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		waitto(2);
		
		if(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=versionCount)fail("Already rebuild table!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(0), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		log.error("first data server has been started!");
		
		waitto(FailOverBaseCase.down_time);
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no rebuild finished!");
    	waitcnt=0;
		log.error("donw time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.74);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 19");
    }
    //first kill one ,add ds after build table
	@Test
    public void testFailover_20_kill_one_add_ds_after_rebuild()
    {
    	log.error("start DataServer test Failover case 20");
		int waitcnt=0;
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");

		waitto(FailOverBaseCase.down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		log.error("first data server has been started!");
		
		waitto(FailOverBaseCase.down_time);
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no migration finished!");
    	
    	waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.74);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 20");
    }
    //kill all data servers, and restart in time
	@Test
    public void testFailover_21_kill_allDataServrs_restart_inTime()
    {
    	log.error("start DataServer test Failover case 21");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		//close all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0)) fail("close data servers failed!");
		log.error("data servers has been closed!");
		
		waitto(5);
		
		//restart all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.start, 0))fail("start data servers failed!");
		log.error("data servers has been started!");
				
		waitto(30);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 21");
    }
    
    //kill all data servers, and restart out time
	@Test
    public void testFailover_22_kill_allDataServrs_restart_outTime()
    {
    	log.error("start DataServer test Failover case 22");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		
		//wait 5s for duplicate
		waitto(5);
		
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//close all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0)) fail("close data servers failed!");
		log.error("data servers has been closed!");
		 
		//wait for rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//restart all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.start, 0))fail("start data servers failed!");
		log.error("data servers has been started!");
		
		waitto(30);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		waitto(30);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 22");
    }  
	//kill another data server,and after migrate finish, restart first data server
	@Test
    public void testFailover_23_kill_one_finishMigrate_restartFirst()
    {
    	log.error("start DataServer test Failover case 23");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
	    if(waitcnt>150) fail("donw time arrived,but no rebuild started!");
	    waitcnt = 0;
		log.error("donw time arrived,rebuild started!");
		
		//record the version times
		versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
	    if(waitcnt>150) fail("donw time arrived,but no rebuild started!");
	    waitcnt = 0;
		log.error("donw time arrived,rebuild started!");
		
		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for second  migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if second migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.6);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 23");
    	
    }
	@Test
    public void testFailover_24_add_ds_finishMigrate_add_ds_finishMigrate_and_kill_one_finishMigrate_kill_one_finishMigration()
	{
		log.error("start DataServer test Failover case 24");
		
		int waitcnt=0;
		
		//modify group configuration .delete two ds
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//write verify data to cluster
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))fail("modify configure file failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))fail("modify configure file failed");
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);	
		log.error("put data over!");
		
		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds1 successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate1 time out!");
		waitcnt=0;
		log.error("check migrate1 finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failed!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data1 time out!");
		waitcnt=0;
		log.error("get data1 over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data 1!");
		
		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds2 successful!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate2 time out!");
		waitcnt=0;
		log.error("check migrate2 finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failed!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data2 time out!");
		waitcnt=0;
		log.error("get data2 over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data2!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("first data server has been closed!");
		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//check rebuild stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check rebuild5 time out!");
		waitcnt=0;
		log.error("check rebuild5 finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failed!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data3 time out!");
		waitcnt=0;
		log.error("get data3 over!");
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.8);
		
		//record the version times
		versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close second data server failed!");
		log.error("second data server has been closed!");
		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//check rebuild stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check rebuild time out!");
		waitcnt=0;
		log.error("check second rebuild finished!");
		
		//verify data
		if (!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("modify tool config file failed!");
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data4 time out!");
		waitcnt=0;
		log.error("get data4 over!");
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.59);
		log.error("end DataServer test Failover case 24");
	}
  //first kill one ,close another ds after build table
	@Test
    public void testFailover_25_kill_one_close_ds_after_rebuild()
    {
    	log.error("start DataServer test Failover case 25");
		int waitcnt=0;
		
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");

		waitto(FailOverBaseCase.down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		//record the version times
		versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		
		log.error("second data server has been closed!");
		
		waitto(FailOverBaseCase.down_time);
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}
		
    	if(waitcnt>300) fail("donw time arrived,but no migration finished!");
    	
    	waitcnt=0;
		log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.59);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 25");
    }
    //kill 80% data servers,and after migrate finish, restart the servers
	@Test
    public void testFailover_26_kill_twoDataServers_finishMigrate_restart()
    {
    	log.error("start DataServer test Failover case 26");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("first data server has been closed!");
		
		//close second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close second data server failed!");
		log.error("second data server has been closed!");

		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if first rebuild finish on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.error("first rebuild has been finished");

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for first migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>210)break;
		}
       if(waitcnt>210) fail("donw time arrived,but no migration finished!");
       waitcnt = 0;
       log.error("donw time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.59);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 26");
    }
  //kill 80%数量dataserver,迁移之前恢复
	@Test
    public void testFailover_27_kill_more_servers()
    { 
    	log.error("start DataServer test Failover case 27");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
        	log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", "100000"))
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);
		log.error("Write data over!");
		
		//wait 10s for duplicate
		waitto(10);
		
		//close 3 data server
		List closeList = dsList.subList(0, 3);
		if(!batch_control_ds(closeList, FailOverBaseCase.stop, 0))
			fail("close 3 data server failed!");
		
		log.error("3 data server has been closed!");
		log.error("wait 2 seconds to restart before rebuild ...");
		waitto(2);
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		//start the  3 data server again
		if(!batch_control_ds(closeList, FailOverBaseCase.start, 0)) 
			fail("start data server failed!");
		log.error("restart ds successful!");
		//wait 10s for data server start
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
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//end test
		log.error("end DataServer test Failover case 27");
    }
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		reset_cluster(csList,dsList);
        
	}
	
	public void tearDown()
	{
		log.error("clean tool and cluster!");
//	    clean_tool("local");
//	    reset_cluster(csList,dsList);
		
	}
	
}
