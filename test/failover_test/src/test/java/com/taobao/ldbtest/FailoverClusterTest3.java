/**
 * 
 */
package com.taobao.ldbtest;

import static org.junit.Assert.*;
import org.junit.Test;
import com.taobao.tairtest.FailOverBaseCase;

public class FailoverClusterTest3 extends FailOverBaseCase {
	@Test
	public void testFailover_01_restart_all_server()
	{
		log.error("start cluster test failover case 01");
		
		int waitcnt=0;
		
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.error("start cluster successful!");
		
		waitto(down_time);
		
		//write data
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "actiontype", "put"))fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "datasize", "100000"))fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "filename", "read.kv"))fail("change conf failed!");
		
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);	
		log.error("finish put data!");
		
		//shut down ds and cs
		if(!batch_control_cs(csList, stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList, stop, 0))fail("stop ds failed!");
		
		waitto(down_time);
		
		//restart cluster
		controlCluster(csList, dsList, start, 0);
		log.error("restart cluster successful!");
		
		waitto(down_time);
		//verify data
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "actiontype", "get"))fail("change conf failed!");
		
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
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end cluster test failover case 01");
	}
	@Test
	public void testFailover_02_restart_all_server_after_join_ds_and_migration()
	{
		log.error("start cluster test failover case 02");
		
		int waitcnt=0;
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		controlCluster(csList, dsList.subList(0, dsList.size()-1), start, 0);
		log.error("start cluster successful!");
		waitto(down_time);
		
		//write data
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "actiontype", "put"))fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "datasize", "100000"))fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "filename", "read.kv"))fail("change conf failed!");
		
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
		assertTrue("put successful rate samll than 90%!",datacnt/100000.0>0.9);	
		log.error("finish put data!");

		if(!control_ds((String) dsList.get(dsList.size()-1), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//add ds and migration
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		
		touch_file((String) csList.get(0), tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		
		waitto(down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");

		
		//shut down ds and cs
		if(!batch_control_cs(csList, stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList, stop, 0))fail("stop ds failed!");
		log.equals("cluster shut down!");
		
		waitto(down_time);
		
		//restart cluster
		controlCluster(csList, dsList, start, 0);
		log.error("restart cluster successful!");
		
		waitto(down_time);
		//verify data
		if(!modify_config_file("local", test_bin+"DataDebug.conf", "actiontype", "get"))fail("change conf failed!");
		
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
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end cluster test failover case 02");
	}
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		resetCluster(csList,dsList);
		batch_uncomment(csList, tair_bin+"etc/group.conf", dsList, "#");
		if(!batch_modify(csList, tair_bin+"etc/group.conf", "_copy_count", "3"))
                        fail("modify configure file failed");
                if(!batch_modify(dsList, tair_bin+"etc/group.conf", "_copy_count", "3"))
                        fail("modify configure file failed");
	}
	public void tearDown()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		resetCluster(csList,dsList);
		batch_uncomment(csList, tair_bin+"etc/group.conf", dsList, "#");
	}
}
