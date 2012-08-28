package com.taobao.ldbtest;

import static org.junit.Assert.*;
import org.junit.Test;

public class FailoverClusterTest2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_all_server()
	{
		log.error("start cluster test failover case 01");
		
		int waitcnt=0;
		
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(FailOverBaseCase.down_time);
		
		//write data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("change conf failed!");
		
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data out time!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("finish put data!");
		
		//shut down ds and cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0))fail("stop ds failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//restart cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("restart cluster failed!");
		log.error("restart cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		//verify data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("change conf failed!");
		execute_data_verify_tool();
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data out time!");
		waitcnt=0;
		log.error("finish get data!");
		
		//verify get result 
        log.error("datacnt:" + datacnt + ";and getVerifySuccessful:" + getVerifySuccessful());
        assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end cluster test failover case 01");
	}
	
	@Test
	public void testFailover_02_restart_all_server_after_join_ds_and_migration()
	{
		log.error("start cluster test failover case 02");
		
		int waitcnt=0;
		
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(FailOverBaseCase.down_time);
		
		//write data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
			fail("change conf failed!");
		
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data out time!");
		waitcnt=0;
		//save put result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);	
		log.error("finish put data!");

		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//add ds and migration
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");

		//shut down ds and cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0))fail("stop ds failed!");
		log.equals("cluster shut down!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//restart cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("restart cluster failed!");
		log.error("restart cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		//verify data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))fail("change conf failed!");
		
		execute_data_verify_tool();
		
		while(check_process("local", "DataDebug")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data out time!");
		waitcnt=0;
		log.error("finish get data!");
		
		//verify get result
		log.error(getVerifySuccessful());
        log.error("datacnt:" + datacnt + ";and getVerifySuccessful:" + getVerifySuccessful());
        assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end cluster test failover case 02");
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
