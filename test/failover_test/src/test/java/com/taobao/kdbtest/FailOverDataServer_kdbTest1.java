package com.taobao.kdbtest;

import static org.junit.Assert.*;
import org.junit.Test;

public class FailOverDataServer_kdbTest1 extends FailOverBaseCase {
	@Test
	public void testFailover_01_add_1ds_then_write_while_migrate()
	{
		log.error("start kdb_1 dataserver test failover case 01");
		int waitcnt=0;
		execute_copy_tool("local", dat_3_cp1);
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_4.subList(0, dsList_4.size()-1), start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(down_time);
		
		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);
		
		//write data
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "datasize", put_count))
			fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "filename", "read.kv"))
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.995);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_4.get(dsList_4.size()-1), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
			fail("check migrate stat failed!");
		
		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("put data out time!");
		waitcnt=0;
		int datacnt_new=getVerifySuccessful();
		log.error("finish write new data and read point data while migrating!");
		
		//check migration stat of start
		while(check_keyword((String) csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(5);
			if(++waitcnt>300)break;
		}
		if(waitcnt>300)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");
		
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "actiontype", "get"))fail("change conf failed!");
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
        assertEquals("verify data failed!", datacnt+datacnt_new,getVerifySuccessful());
        assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("verify data successful!");
		
		log.error("end kdb_1 dataserver test failover case 01");
	}
	
	@Test
	public void testFailover_02_add_1ds_then_write_then_kill_the_new_ds_while_migrate()
	{
		log.error("start kdb_1 dataserver test failover case 02");
		int waitcnt=0;
		execute_copy_tool("local", dat_3_cp1);
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_4.subList(0, dsList_4.size()-1), start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(down_time);
		
		//start ycsb
//		control_kdb_ycsb(ycsb_client, start);
		
		//write data
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "actiontype", "put"))
			fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "datasize", put_count))
			fail("change conf failed!");
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "filename", "read.kv"))
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.995);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_4.get(dsList_4.size()-1), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
		{
			waitto(2);
			if(++waitcnt>10)fail("check migrate stat failed!");
		}
		waitcnt=0;
		log.error("migrate count is right!");
		
		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		waitto(3);
		
		if(!control_ds((String) dsList_4.get(dsList_4.size()-1), stop, 0))fail("stop ds failed!");
		waitto(ds_down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
			fail("already migrate!");
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("put data out time!");
		waitcnt=0;
		int datacnt_new=getVerifySuccessful();
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("finish write new data and read point data while migrating!");
		
		if(!modify_config_file("local", test_bin+"kdb_tool.conf", "actiontype", "get"))fail("change conf failed!");
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
        assertEquals("verify data failed!", datacnt+datacnt_new,getVerifySuccessful());
        assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
		log.error("verify data successful!");
		
		//check if bucket status is recovered
		execute_copy_tool("local", check_3_cp1);
		
		log.error("end kdb_1 dataserver test failover case 02");
	}
	
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		clean_point_tool("local");
		reset_cluster(csList,dsList_6);
		execute_copy_tool("local", conf_4);
		execute_copy_tool("local", copy_kv3_cp1);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
            fail("modify configure file failure");
        if(!batch_modify(dsList_6, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "1"))
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
