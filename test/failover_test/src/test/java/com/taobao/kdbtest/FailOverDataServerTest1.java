package com.taobao.kdbtest;
import org.junit.Test;

/**
 * author ashu.cs
 * 
 **/

public class FailOverDataServerTest1 extends FailOverBaseCase{
	
	@Test
	public void testFailover_01_add_ds_stop_in_time()
	{
		log.error("start DataServer test Failover case 01");
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
        log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		
		log.error("Start Cluster Successful!");
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
		
		//save put result
		int datacnt=getVerifySuccessful();
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);	
		log.error("finish put data!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been started!");
		
//		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		//stop the data server again
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("stop data server failed!");
		log.error("stop ds successful!");
		waitto(FailOverBaseCase.ds_down_time);
		
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
		
		//end test
		log.error("end DataServer test Failover case 01");
	}
	
	@Test
    public void testFailover_02_add_one_ds()
    {   
		log.error("start DataServer test Failover case 02");
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		//wait 5s for duplicate
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been added!");
		
		//wait rebuild table
		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 02");
    }
    
	@Test
    public void testFailover_03_add_ds_close_in_migrate()
    { 
    	log.error("start DataServer test Failover case 03");
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
        
		log.error("wait system initialize ...");
	    waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");

		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been added!");
       
	  	//wait for migrate start
		waitto(FailOverBaseCase.down_time);
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
	    if(waitcnt>10) fail("down time arrived,but no migration start!");
	    waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("restop ds failed!");
		log.error("restop ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
    	if(waitcnt>10) fail("down time arrived,but no rebuild finished!");
    	waitcnt=0;
		log.error("down time arrived,rebuild finished!");
		
		//migrate need check data 
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
		assertEquals("verify data failed!",datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 03");
    }
    
	@Test
    public void testFailover_04_add_ds_reclose_after_migrate()
    {
    	log.error("start DataServer test Failover case 04");
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");  
		log.error("wait system initialize ...");
		
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
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
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 10s for duplicate
		waitto(10);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
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
		if(waitcnt>150) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		//restop ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("restart ds failed!");
		log.error("ds has been stoped");
		
		//wait for data server stop
		waitto(FailOverBaseCase.down_time);

		// wait second rebuild finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin + "logs/config.log") != (versionCount+1))
		{
			log.debug("check if rebuild finish on cs " + (String) csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 10)break;
		}
		if (waitcnt > 10)fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.error("down time arrived,the seccond rebuild finished!");
		
		// check data
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
		log.error("Successfully Verified data!");
	
		//end test
		log.error("end DataServer test Failover case 04");
    }
	
	@Test
    public void testFailover_05_add_two_ds()
    {
    	log.error("start DataServer test Failover case 05");
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");  
		log.error("wait system initialize ...");
		
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 10s for duplicate
		waitto(10);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		waitto(FailOverBaseCase.down_time);
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		// check data
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
		log.error("end DataServer test Failover case 05");
    }
	
    //first add one ,and then add another one in migrate
	@Test
    public void testFailover_06_add_one_inMigrate_add_another_ds()
    {
    	log.error("start DataServer test Failover case 06");
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());	
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
		waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
        if(waitcnt>200) fail("down time arrived,but no migration finished!");
        waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//migrate need check data 
		if (!modify_config_file("local", FailOverBaseCase.test_bin + "kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		// check verify
		while (check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    	log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");

		waitto(FailOverBaseCase.down_time);
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
		waitto(3);
		
		//restart second data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), FailOverBaseCase.stop, 0))fail("start data server failed!");
		log.error("second data server has been close");
		
		waitto(FailOverBaseCase.down_time);
		
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
        if(waitcnt>200) fail("down time arrived,but no migration finised!");
        waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		//check verify
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}		
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
		waitto(FailOverBaseCase.down_time);

		//reclose first data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)fail("down time arrived,but no rebuild finished!");
		}
        waitcnt=0;
		log.error("down time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");
		//migrate need check data 
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
        waitto(FailOverBaseCase.down_time);
		
        int versionCount=check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
        
		//reclose first data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		//reclose second data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")==versionCount)
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no rebuild finished!");
		}
        waitcnt = 0;
		log.error("down time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");   
		//migrate need check data 
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
		log.error("Read data over");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 09");	
    }
    
    //add one data server ,and then migrate,add another£¬ reclose first data server
	@Test
    public void testFailover_10_add_one_afterMigrate_recloseFirst()
    {
    	log.error("start DataServer test Failover case 10");
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration finished!");
		}
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");

		//wait for second migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no second migration started!");
		}
		waitcnt = 0;
		log.error("down time arrived,the second migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop first data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no second rebuild finished!");
		}
	    waitcnt = 0;
		log.error("down time arrived,rebuild finished!");
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
		   if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}	
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
		waitto(FailOverBaseCase.down_time);
		
		//wait for second migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no second migration started!");
		}
        waitcnt = 0;
		log.error("down time arrived, the second migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop second data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no second rebuild finished!");
		}
        waitcnt = 0;
		log.error("down time arrived,rebuild finished!");
		
		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify configure file failed");   
		//migrate need check data 
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
		log.error("Read data over");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("firse data server has been start!");
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)fail("down time arrived,but no migration started!");
		}
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
		
		//wait for second migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no second migration started!");
		}
		waitcnt = 0;
		log.error("down time arrived,the second migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop first data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stop!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stop!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//wait for rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")==versionCount)
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>10)fail("down time arrived,but no second rebuild finished!");
		}
		waitcnt = 0;
		log.error("down time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for first migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no second migration started!");
		}
        waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
		
		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no first migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop second data server
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		
		//wait for  rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check fourth if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no fourth rebuild finished!");
		}
		waitcnt = 0;
		log.error("down time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
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
		
		if(!control_cluster(csList, dsList_6, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		waitto(5);
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been start!");
		
		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration started!");
		}
	    waitcnt = 0;
		log.error("down time arrived,migration started!");
		
		///uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		log.error("second data server has been start!");
        
		//wait for first migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
        if(waitcnt>200) fail("down time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("down time arrived,migration finished!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), FailOverBaseCase.stop, 0))fail("stop first data server failed!");
		log.error("first data server has been stoped!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), FailOverBaseCase.stop, 0))fail("stop second data server failed!");
		log.error("second data server has been stoped!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//wait for fourth rebuild finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin + "logs/config.log")==versionCount)
		{
			log.debug("check fourth rebuild finish on cs " + (String) csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 10)fail("down time arrived,but no fourth rebuild finished!");
		}
		waitcnt = 0;
		log.error("down time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.48);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.48);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 14");
    }
    
	@Test
    public void testFailover_15_add_ds_close_another_in_migrate()
    { 
    	log.error("start DataServer test Failover case 15");
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
        
		log.error("wait system initialize ...");
	    waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been added!");
       
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration start!");
		}
	    waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//restop ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("restop another ds failed!");
		log.error("restop another ds successful!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no rebuild finished!");
		}
    	waitcnt=0;
		log.error("down time arrived,rebuild finished!");
		
		//migrate need check data
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
		log.error("end DataServer test Failover case 15");
    }
	
	@Test
    public void testFailover_16_add_ds_close_another_in_migrate_restart_ontime()
    {
    	log.error("start DataServer test Failover case 16");
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
        
		log.error("wait system initialize ...");
	    waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		waitto(5);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been added!");

		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no migration start!");
		}
	    waitcnt=0;
		log.error("down time arrived,migration started!");
		
		//restop ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.stop, 0))fail("restop another ds failed!");
		log.error("restop another ds successful!");
		//restart ds
		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.start, 0))fail("restart another ds failed!");
		log.error("restart another ds successful!");
		
		waitto(FailOverBaseCase.down_time);
		//delete all dat file on 18ds
		
//		touch_file((String)csList.get(0), FailOverBaseCase.tair_bin + "etc/group.conf");
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish  on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>300)break;
		}
    	if(waitcnt>300) fail("down time arrived,but no migration finished!");
    	waitcnt=0;
		log.error("down time arrived,migration finished!");
		
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.73);
		assertEquals("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
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
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!modify_config_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", "_min_data_server_count", "4"))
            fail("modify configure file failure");
		
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
		//change test tool's configuration
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))
			fail("modify configure file failed");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		//write data to cluster
		execute_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		waitto(5);
		
		//close one data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");

		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");
		
		//restart  data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("data server has been restarted!");
		waitto(FailOverBaseCase.down_time);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify configure file failed");   
		//migrate need check data 
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
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		
		//record the version times
		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//wait rebuild finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
		{
			log.debug("check if rebuild finish  on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)fail("down time arrived,but no rebuild finished!");
		}
    	waitcnt=0;
		log.error("down time arrived,rebuild finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed"); 
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
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
		
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!modify_config_file((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", "_min_data_server_count", "4"))
            fail("modify configure file failure");
		
		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    	log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		//wait 5s for duplicate
		waitto(5);

        int versionCount=check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
		//close one data server
		if(!control_ds((String) dsList_5.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("a new data server has been started!");
		waitto(FailOverBaseCase.down_time);
		
		if(check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")==versionCount)fail("not rebuild table!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
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
		
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    	log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds((String) dsList_5.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("first data server has been started!");
		
		waitto(FailOverBaseCase.down_time);
		
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>20)fail("time out! but no migrate started!");
		}
		waitcnt=0;
		log.error("check migrate started!");
		
		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if rebuild finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)fail("down time arrived,but no migrate finished!");
		}
    	waitcnt=0;
		log.error("down time arrived,migrate finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.73);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.73);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 20");
    }
	
	//kill another data server,and after migrate finish, restart first data server
	@Test
    public void testFailover_23_kill_one_finishMigrate_restartFirst()
    {
    	log.error("start DataServer test Failover case 23");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	   	log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		//close one data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		
		//record the version times
		int migrateCount = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//kill second data server
		if(!control_ds((String) dsList_4.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//restart first data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		
		waitto(3);
		waitto(FailOverBaseCase.down_time);
		
		//wait for rebuild start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=(migrateCount+1))
		{
			log.debug("check if rebuild start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>10)break;
		}
	    if(waitcnt>10) fail("down time arrived,but no rebuild started!");
	    waitcnt = 0;
		log.error("down time arrived,rebuild started!");
		
		//wait for second  migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if second migration finish on cs "+(String)csList.get(0)+" log");
			waitto(5);
			if(++waitcnt>200)break;
		}
        if(waitcnt>200) fail("down time arrived,but no migration finished!");
        waitcnt = 0;
		log.error("down time arrived,migration finished!");
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.48);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.48);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 23");
    }
	
//	@Test
//    public void testFailover_24_add_ds_finishMigrate_add_ds_finishMigrate_and_kill_one_finishMigrate_kill_one_finishMigration()
//	{
//		log.error("start DataServer test Failover case 24");
//		
//		int waitcnt=0;
//		
//		//modify group configuration .delete two ds
//		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
//		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
//		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
//		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
//		log.error("group.conf has been changed!");
//		
//		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
//		log.error("start cluster successful!");
//		
//		waitto(FailOverBaseCase.down_time);
//		
//		//write verify data to cluster
//		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))fail("modify configure file failed!");
//		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", "100000"))fail("modify configure file failed");
//		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))fail("modify configure file failed");
//		execute_data_verify_tool();
//		
//		while(check_process("local", "kdb_tool")!=2)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("put data time out!");
//		waitcnt=0;
//		//verify get result
//		int datacnt=getVerifySuccessful();
//		log.error(getVerifySuccessful());
//		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);	
//		log.error("put data over!");
//		
//		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
//		log.error("start ds1 successful!");
//		
//		//uncomment cs group.conf
//		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
//		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
//		//touch group.conf
////		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
////		log.error("change group.conf and touch it");
//		
//		waitto(FailOverBaseCase.down_time);
//		
//		//check migration stat of finish
//		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("check migrate1 time out!");
//		waitcnt=0;
//		log.error("check migrate1 finished!");
//		
//		//verify data
//		if (!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify tool config file failed!");
//		execute_data_verify_tool();
//		
//		while(check_process("local", "kdb_tool")!=2)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("get data1 time out!");
//		waitcnt=0;
//		log.error("get data1 over!");
//		
//		//verify get result
//		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
//		log.error("Successfully Verified data 1!");
//		
//		if(!control_ds((String) dsList.get(dsList.size()-2), FailOverBaseCase.start, 0))fail("start ds failed!");
//		log.error("start ds2 successful!");
//		
//		//uncomment cs group.conf
//		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
//		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
//		//touch group.conf
//		touch_file((String) csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf");
//		log.error("change group.conf and touch it");
//		
//		waitto(FailOverBaseCase.down_time);
//		
//		//check migration stat of finish
//		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("check migrate2 time out!");
//		waitcnt=0;
//		log.error("check migrate2 finished!");
//		
//		//verify data
//		if (!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify tool config file failed!");
//		execute_data_verify_tool();
//		
//		while(check_process("local", "kdb_tool")!=2)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("get data2 time out!");
//		waitcnt=0;
//		log.error("get data2 over!");
//		
//		//verify get result
//		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
//		log.error("Successfully Verified data2!");
//		
//		//record the version times
//		int versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
//		
//		//close first data server
//		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0)) fail("close first data server failed!");
//		log.error("first data server has been closed!");
//		waitto(2);
//		
//		//wait rebuild table
//		waitto(FailOverBaseCase.down_time);
//		
//		//check rebuild stat of finish
//		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("check rebuild5 time out!");
//		waitcnt=0;
//		log.error("check rebuild5 finished!");
//		
//		//verify data
//		if (!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify tool config file failed!");
//		execute_data_verify_tool();
//		
//		while(check_process("local", "kdb_tool")!=2)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("get data3 time out!");
//		waitcnt=0;
//		log.error("get data3 over!");
//		log.error(getVerifySuccessful());
//		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.79);
//		
//		//record the version times
//		versionCount = check_keyword((String)csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log");
//		
//		//close second data server
//		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("close second data server failed!");
//		log.error("second data server has been closed!");
//		waitto(2);
//		
//		//wait rebuild table
//		waitto(FailOverBaseCase.down_time);
//		
//		//check rebuild stat of finish
//		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_rebuild, FailOverBaseCase.tair_bin+"logs/config.log")!=(versionCount+1))
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("check rebuild time out!");
//		waitcnt=0;
//		log.error("check second rebuild finished!");
//		
//		//verify data
//		if (!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("modify tool config file failed!");
//		execute_data_verify_tool();
//		
//		while(check_process("local", "kdb_tool")!=2)
//		{
//			waitto(2);
//			if(++waitcnt>150)break;
//		}
//		if(waitcnt>150)fail("get data4 time out!");
//		waitcnt=0;
//		log.error("get data4 over!");
//		log.error(getVerifySuccessful());
//		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.58);
//		log.error("end DataServer test Failover case 24");
//	}
	
	//first kill one ,close another ds after build table
	@Test
    public void testFailover_25_kill_one_close_ds_after_rebuild()
    {
    	log.error("start DataServer test Failover case 25");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 5s for duplicate
		waitto(5);
		//record the version times
		int migrateCount = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		//record the version times
		migrateCount = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close one data server
		if(!control_ds((String) dsList_4.get(1), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))                                     fail("modify configure file failed");   
		//migrate need check data 
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
		assertTrue("verify data failed!",getVerifySuccessful()/100000.0>0.48);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.48);
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
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
                
	    log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");	
		
		//wait 5s for duplicate
		waitto(5);
		
		//record the version times
		int migrateCount = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		
		//close data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.stop, 0))fail("close first data server failed!");
		log.error("first data server has been closed!");
		if(!control_ds((String) dsList_4.get(1), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		
		//restart first data server
		if(!control_ds((String) dsList_4.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		//restart second data server
		if(!control_ds((String) dsList_4.get(1), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
		waitto(FailOverBaseCase.down_time);
		
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))
			fail("modify configure file failed");   
		//migrate need check data 
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
		assertEquals("verify data failed!", getVerifySuccessful()/(float)datacnt>0.48);
		assertEquals("verify point data failed!", getPointVerifySuccessful()/100000.0>0.48);
		log.error("Successfully verified data!");

		//end test
		log.error("end DataServer test Failover case 26");
    }
	
	//kill 80% dataserver, restart them before rebuild
	@Test
    public void testFailover_27_kill_more_servers()
    { 
    	log.error("start DataServer test Failover case 27");
		int waitcnt=0;
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("start cluster failed!");
        	log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");
		
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
		log.error(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/100000.0>0.9995);
		log.error("Write data over!");
		
		//wait 10s for duplicate
		waitto(10);
		
		//close 3 data server
		if(!batch_control_ds(dsList_4.subList(0, 3), FailOverBaseCase.stop, 0))
			fail("close 3 data server failed!");	
		log.error("3 data server has been closed!");
		
        //start the  3 data server again
		if(!batch_control_ds(dsList_4.subList(0, 3), FailOverBaseCase.start, 0)) 
			fail("start data server failed!");
		log.error("3 data server has been started!");
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
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.error("Read data over!");
		
		//verify get result
		log.error(getVerifySuccessful());
		assertTrue("verify data failed!", getVerifySuccessful()/(float)datacnt>0.36);
		assertTrue("verify point data failed!", getPointVerifySuccessful()/100000.0>0.36 );
		log.error("Successfully Verified data!");
		
		//end test
		log.error("end DataServer test Failover case 27");
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
