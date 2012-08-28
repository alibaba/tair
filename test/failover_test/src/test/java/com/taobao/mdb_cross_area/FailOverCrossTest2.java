package com.taobao.mdb_cross_area;

import static org.junit.Assert.*;
import org.junit.Test;
import com.taobao.mdb_cross_area.FailOverBaseCase;

public class FailOverCrossTest2 extends FailOverBaseCase {

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
