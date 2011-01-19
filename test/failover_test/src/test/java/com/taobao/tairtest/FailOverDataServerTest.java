package com.taobao.tairtest;

import java.util.*;

import org.junit.Test;

import com.ibm.staf.STAFResult;

import junit.framework.Assert;

/**
 * author fanggang
 * dataserver testcse
 **/
public class FailOverDataServerTest extends FailOverBaseCase{
	
	//在恢复时间内启动dataserver
	@Test
	public void testFailover_01_restart_in_time()
	{
		log.error("start DataServer test Failover case 01");
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
		
		//save put result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);	
		log.error("finish put data!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");
		log.error("wait 2 seconds to restart before rebuild ...");

		waitto(2);
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		
		//start the data server again
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("Restart ds successful!");
		
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");	
		
		//end test
		log.error("end DataServer test Failover case 01");
	}
	
    //在恢复时间外恢复dataserver
	@Test
    public void testFailover_02_kill_out_time()
    {   
		log.error("start DataServer test Failover case 02");
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
			
		//change test tool's configuration
/*		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
//			fail("modify configure file failed");	
		//read data from cluster
//		execute_data_verify_tool();
		//check verify
		while(check_process("local", "DataDebug")!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(++waitcnt>150)break;
		}	
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		String verify="cd "+FailOverBaseCase.test_bin+" && ";
		verify+="tail -4 datadbg0.log|grep \"Successful\"|awk -F\" \" \'{print $3}\'";
		STAFResult result=executeShell(stafhandle, "local", verify);
		String stdout=getShellOutput(result);
		if((new Integer(stdout.trim())).intValue()!=100000)fail("Not All data were entered!");
		log.error("Successfully Verified data!");
		
*/		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 02");
    }
    
    //在迁移过程中恢复
	@Test
    public void testFailover_03_restart_in_migrate()
    { 
    	log.error("start DataServer test Failover case 03");
		int waitcnt=0;
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
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");
       
        
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
		
		//restart ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failed!");
		log.error("restart ds successful!");
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		log.error("touch group.conf");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 03");
      
    }
    
    //在迁移后恢复
	@Test
    public void testFailover_04_restart_after_migrate()
    {
    	log.error("start DataServer test Failover case 04");
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
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		
		log.error("first data server has been closed!");
       
       
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
		
		//restart ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failed!");
		
		//wait 3s for data server start
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		log.error("touch group.conf");
		waitto(FailOverBaseCase.down_time);

		// wait second migrate finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin + "logs/config.log") != 2) {
			log.debug("check if migration finish on cs " + (String) csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("donw time arrived,the seccond migration finished!");
		
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 04");
    
    }
    
    //kill 80%数量dataserver,迁移之前恢复
	@Test
    public void testFailover_05_kill_more_servers()
    { 
    	log.error("start DataServer test Failover case 05");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully Verified data!");
		
		//end test
		log.error("end DataServer test Failover case 05");
    }
    
    
    //first kill one ,and then kill another one in migrate
	@Test
    public void testFailover_06_kill_one_inMigrate_outtime()
    {
    	log.error("start DataServer test Failover case 06");
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
		
		//kill another data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
        
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 06");
    }
    
  //first kill one , then kill another one and restart it before second migrate
	@Test
    public void testFailover_07_kill_one_inMigrate_inTime()
    {
    	log.error("start DataServer test Failover case 07");
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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("second data server has been closed!");
		log.error("wait 3 seconds to restart the secord ds  before rebuild ...");
		waitto(3);
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)fail("Already migration!");
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("second data server has been started");
		
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 07");
    }
    
    //first kill one ,and then kill another one, and restart first one
	@Test
    public void testFailover_08_kill_one_inMigrate_restartFirst()
    {
    	log.error("start DataServer test Failover case 08");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 08");
    }
    
    //kill another data server ,and restart both in migrate
	@Test
    public void testFailover_09_kill_one_inMigrate_restartBoth()
    {
    	log.error("start DataServer test Failover case 09");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
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
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
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
		log.error("Read data over");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 09");	
    }
    
    //kill another data server ,and then migrate, restart first data server
	@Test
    public void testFailover_10_kill_one_afterMigrate_restartFirst()
    {
    	log.error("start DataServer test Failover case 10");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

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
        if(waitcnt>150) fail("donw time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("donw time arrived,migration started!");
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
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
		
		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
	    if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 10");
    	
    }
    
    //kill another data server ,and after second migrate, restart second data server
	@Test
    public void testFailover_11_kill_one_afterMigrate_restartSecond()
    {
    	log.error("start DataServer test Failover case 11");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
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
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 11");
    	
    }
    
    //kill another data server ,and after second migrate, restart both
	@Test
    public void testFailover_12_kill_one_afterMigrate_restartBoth()
    {
    	log.error("start DataServer test Failover case 12");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
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
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 12");
    	
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
		
		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for first migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("donw time arrived,migration finished!");
		
		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for second  migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if second migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 23");
    	
    }
    
  //kill another data server,and after migrate finish, restart second data server
	@Test
    public void testFailover_13_kill_one_finishMigrate_restartSecond()
    {
    	log.error("start DataServer test Failover case 13");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

		waitto(2);
		
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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
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
		
		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
		
		waitto(3);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check second if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 13");
    	
    }
    
    //kill another data server,and after migrate finish, restart both
	@Test
    public void testFailover_14_kill_one_finishMigrate_restartBoth()
    {
    	log.error("start DataServer test Failover case 14");
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
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close data server failed!");
		log.error("first data server has been closed!");

		waitto(2);
		
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
		
		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
        
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
		
		//wait for second migrate finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin + "logs/config.log") != 2) {
			log.debug("check second if migration finish on cs " + (String) csList.get(0) + " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no second migration finished!");
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 14");
    }
    
    //kill two data servers,and after migrate start, restart the servers
	@Test
    public void testFailover_15_kill_twoDataServers_afterMigrate_restart()
    {
    	log.error("start DataServer test Failover case 15");
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
		
		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("first data server has been closed!");
		
		//close second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close second data server failed!");
		log.error("second data server has been closed!");

		waitto(2);
		
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
		
		//wait for migrate finish
		waitcnt=0;
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
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
		waitcnt=0;
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
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 15");
    }
    
    //kill two data servers,and after migrate finish, restart the servers
	@Test
    public void testFailover_16_kill_twoDataServers_finishMigrate_restart()
    {
    	log.error("start DataServer test Failover case 16");
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
		
		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("first data server has been closed!");
		
		//close second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close second data server failed!");
		log.error("second data server has been closed!");

		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration finish on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}

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
		
		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if second migration finish on cs "+(String)csList.get(0)+" log");
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 16");
    }
    
    //kill all data servers, and restart in time
	@Test
    public void testFailover_17_kill_allDataServrs_restart_inTime()
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
		
		//close all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0)) fail("close data servers failed!");
		log.error("data servers has been closed!");
		
		waitto(5);
		
		//restart all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.start, 0))fail("start data servers failed!");
		log.error("data servers has been started!");
                
        //touch the group file
		//if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
		//		fail(" touch group file failed!");
		//waitto(FailOverBaseCase.down_time);
				
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 17");
    }
    
    //kill all data servers, and restart out time
	@Test
    public void testFailover_18_kill_allDataServrs_restart_outTime()
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
		
		//wait for migrate start
		//while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		//{
		//	log.error("check if migration start on cs "+(String)csList.get(0)+" log ");
		//	try{
		//		Thread.sleep(2000);
		//	}
		//	catch(Exception e){
		//		e.printStackTrace();
		//	}
		//}
		
		//restart all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.start, 0))fail("start data servers failed!");
		log.error("data servers has been started!");
		
		waitto(30);
		
		//touch the group file
		if (touch_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf")!= 0)
			fail(" touch group file failed!");
		waitto(FailOverBaseCase.down_time);
		
		waitto(30);
		
		//wait for migrate finish
		//while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		//{
		//	log.error("check if migration finish on cs "+(String)csList.get(0)+" log ");
		//	try{
		//		Thread.sleep(2000);
	        //		}
		//	catch(Exception e){
		//		e.printStackTrace();
	//		}
	//	}
		
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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 18");
    }
    
    //kill the data server after migrate start
	@Test
    public void testFailover_21_kill_one_afterMigate()
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
		
		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("first data server has been closed!");
		
		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
        if(waitcnt>150) fail("donw time arrived,but no migration started!");
        waitcnt = 0;
        log.error("donw time arrived,migration started!");
        
		//close second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close second data server failed!");
		log.error("second data server has been closed!");
	
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log ");
			waitto(2);
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
		log.error("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 21");
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
		
		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
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
		
		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("first data server has been closed!");
		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=3)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate3 time out!");
		waitcnt=0;
		log.error("check migrate3 finished!");
		
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
		
		//close second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
		log.error("second data server has been closed!");
		waitto(2);
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=4)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate4 time out!");
		waitcnt=0;
		log.error("check migrate4 finished!");
		
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
		
		log.error("end DataServer test Failover case 24");
	}
	@Test
    public  void testFailover_25_kill_oneDataServer_no_rebuild_table()
    {
    	//start cluster 
    	log.error("start DataServer test Failover case 25");
    	int waitcnt=0;
		// modify tair config
		try {
			if (!modify_config_file((String) csList.get(0), FailOverBaseCase.tair_bin + "etc/group.conf", "_min_data_server_count", "5"))
				fail("modify configure file failed");
			if (!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
				fail("start cluster failed!");
			log.error("wait system initialize ...");
			waitto(FailOverBaseCase.down_time);
			log.error("Start Cluster Successful!");

			// modify test config
			if (!modify_config_file("local", FailOverBaseCase.test_bin + "DataDebug.conf", "actiontype", "put"))
				fail("modify configure file failed");
			if (!modify_config_file("local", FailOverBaseCase.test_bin + "DataDebug.conf", "datasize", "100000"))
				fail("modify configure file failed");
			if (!modify_config_file("local", FailOverBaseCase.test_bin + "DataDebug.conf", "filename", "read.kv"))
				fail("modify configure file failed");
			// write 100k data to cluster
			execute_data_verify_tool();
			// check verify
			while (check_process("local", "DataDebug") != 2) {
				waitto(2);
				if (++waitcnt > 150)
					break;
			}
			if (waitcnt > 150)
				fail("put data time out!");
			waitcnt = 0;
			// verify get result
			int datacnt = getVerifySuccessful();
			log.error("put successful=" + datacnt);
			assertTrue("put successful rate samll than 90%!", datacnt / 100000.0 > 0.9);
			log.error("Write data over!");

			// close a DS
			if (!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
				fail("close 1 data server failed!");
			waitto(FailOverBaseCase.down_time);
			// modify test config
			if (!modify_config_file("local", FailOverBaseCase.test_bin + "DataDebug.conf", "actiontype", "get"))
				fail("modify configure file failed");
			execute_data_verify_tool();
			while (check_process("local", "DataDebug") != 2) {
				waitto(2);
				if (++waitcnt > 150)
					break;
			}
			if (waitcnt > 150)
				fail("Read data time out!");
			waitcnt = 0;
			log.error("Read data over!");
			// verify get result
			log.error(getVerifySuccessful());
			assertEquals("verify data failed!", datacnt, getVerifySuccessful());
			log.error("Successfully Verified data!");
			
		} finally {
			// resume tair config
			if (!modify_config_file((String) csList.get(0), FailOverBaseCase.tair_bin + "etc/group.conf", "_min_data_server_count", "1"))
				fail("modify configure file failed");
		}
		log.error("end DataServer test Failover case 25");
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
