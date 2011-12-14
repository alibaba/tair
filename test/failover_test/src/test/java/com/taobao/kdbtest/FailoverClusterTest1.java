package com.taobao.kdbtest;
import org.junit.Test;

/**
 * @author ashu.cs
 *
 */

public class FailoverClusterTest1 extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_all_server()
	{
		log.error("start cluster test failover case 01");
		
		execute_copy_tool("local", conf_4);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		int waitcnt=0;
		
		//start cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(FailOverBaseCase.down_time);
		
		//start ycsb
		control_kdb_ycsb(ycsb_client, start);
		
		//write data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("change conf failed!");
		
		execute_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data out time!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("finish put data!");
		
		control_kdb_ycsb(ycsb_client, stop);
		
		//shut down ds and cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList_4, FailOverBaseCase.stop, 0))fail("stop ds failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//restart cluster
		if(!control_cluster(csList, dsList_4, FailOverBaseCase.start, 0))fail("restart cluster failed!");
		log.error("restart cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		
		control_kdb_ycsb(ycsb_client, start);
		
		//verify data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("change conf failed!");
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data out time!");
		waitcnt=0;
		log.error("finish get data!");
		
		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end cluster test failover case 01");
	}
	
	@Test
	public void testFailover_02_restart_all_server_after_join_ds_and_migration()
	{
		log.error("start cluster test failover case 02");
		execute_copy_tool("local", conf_5);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
		execute_copy_tool("local", dat_4_cp1);
		
		int waitcnt=0;
		
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_5.subList(0, dsList_5.size()-1), FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(FailOverBaseCase.down_time);
		
		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);
		
		//write data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "datasize", FailOverBaseCase.put_count))
			fail("change conf failed!");
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "filename", "read.kv"))
			fail("change conf failed!");
		
		execute_data_verify_tool();
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data out time!");
		waitcnt=0;
		//save put result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9995);	
		log.error("finish put data!");

		if(!control_ds((String) dsList_5.get(dsList_5.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		
		//add ds and migration
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList_5.get(dsList_5.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(FailOverBaseCase.down_time);
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
//		control_kdb_ycsb(ycsb_client, stop);

		//shut down ds and cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList_5, FailOverBaseCase.stop, 0))fail("stop ds failed!");
		log.equals("cluster shut down!");
		
		waitto(FailOverBaseCase.ds_down_time);
		
		//restart cluster
		if(!control_cluster(csList, dsList_5, FailOverBaseCase.start, 0))fail("restart cluster failed!");
		log.error("restart cluster successful!");
		
		waitto(FailOverBaseCase.down_time);
		
//		control_kdb_ycsb(ycsb_client, start);
		//verify data
		if(!modify_config_file("local", FailOverBaseCase.test_bin+"kdb_tool.conf", "actiontype", "get"))fail("change conf failed!");
		
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data out time!");
		waitcnt=0;
		log.error("finish get data!");
		
		//verify get result
        assertEquals("verify data failed!", datacnt,getVerifySuccessful());
        assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end cluster test failover case 02");
	}
	
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		clean_point_tool("local");
		execute_copy_tool("local", copy_kv4_cp1);
		reset_cluster(csList,dsList_6);
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
