package com.taobao.kdbtest;

import static org.junit.Assert.*;
import org.junit.Test;

public class FailOverDataServer_kdbTest2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_add_1ds_then_write_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 01");
		int waitcnt=0;
		execute_copy_tool("local", conf_4);
		execute_copy_tool("local", dat_3_cp2);
		
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_4.get(dsList_4.size()-1), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		
		waitto(down_time);
		
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
		{
			waitto(2);
			if(++waitcnt>10)
				fail("check migrate stat failed!");
		}
		waitcnt=0;
		
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
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
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
		
		log.error("end kdb_2 dataserver test failover case 01");
	}
	
	@Test
	public void testFailover_02_add_1ds_then_write_then_kill_the_new_ds_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 02");
		int waitcnt=0;
		execute_copy_tool("local", conf_4);
		execute_copy_tool("local", dat_3_cp2);
		
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
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
		
		waitto(5);
		
		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		waitto(3);
		
		if(!control_ds((String) dsList_4.get(dsList_4.size()-1), stop, 0))fail("stop ds failed!");
		waitto(ds_down_time);
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("put data out time!");
		waitcnt=0;
		int datacnt_new=getVerifySuccessful();
		getPointVerifySuccessful();
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
		execute_copy_tool("local", check_3_cp2);
		
		log.error("end kdb_2 dataserver test failover case 02");
	}
	
	@Test
	public void testFailover_03_add_1ds_then_write_then_kill_one_old_ds_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 03");
		int waitcnt=0;
		execute_copy_tool("local", conf_4);
		execute_copy_tool("local", dat_3_cp2);
		
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_4.get(dsList_4.size()-1), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_4.get(dsList_4.size()-1), "#"))fail("change group.conf failed!");
		
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
		{
			waitto(2);
			if(++waitcnt>200)fail("check migrate stat failed!");
		}
		waitcnt=0;
		log.error("migrate count is right!");
		
		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		waitto(3);
		
		if(!control_ds((String) dsList_4.get(0), stop, 0))fail("stop ds failed!");
		waitto(ds_down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+2)
			fail("check migrate stat failed!");
		
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
		
		log.error("end kdb_2 dataserver test failover case 03");
	}
	
	@Test
	public void testFailover_04_stop_1ds_then_write_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 04");
		int waitcnt=0;
		execute_copy_tool("local", conf_3);
		execute_copy_tool("local", dat_3_cp2);
		
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_4.get(dsList_4.size()-2), stop, 0))fail("stop ds failed!");
		log.error("stop ds successful!");
		
		waitto(ds_down_time);
		
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
		{
			waitto(2);
			if(++waitcnt>200)fail("check migrate stat failed!");
		}
		waitcnt=0;
		log.error("migrate count is right!");
		
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
		assertEquals("verify point data failed!", 100000, getPointVerifySuccessful());
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
		
		log.error("end kdb_2 dataserver test failover case 04");
	}
	
	@Test
	public void testFailover_05_stop_1ds_then_write_then_restart_the_ds_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 05");
		int waitcnt=0;
		execute_copy_tool("local", conf_3);
		execute_copy_tool("local", dat_3_cp2);
		
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_4.get(dsList_4.size()-2), stop, 0))fail("stop ds failed!");
		log.error("stop ds successful!");
		
		waitto(ds_down_time);
		
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
		{
			waitto(2);
			if(++waitcnt>200)fail("check migrate stat failed!");
		}
		waitcnt=0;
		log.error("migrate count is right!");
		
		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		waitto(3);
		
		if(!control_ds((String) dsList_4.get(dsList_4.size()-2), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		waitto(down_time);
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+2)
		{
			waitto(2);
			if(++waitcnt>200)fail("check migrate stat failed!");
		}
		waitcnt=0;
		log.error("migrate count is right!");
		
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
		
		execute_copy_tool("local", check_3_cp2);
		
		log.error("end kdb_2 dataserver test failover case 05");
	}
	
	@Test
	public void testFailover_06_add_3ds_then_write_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 06");
		int waitcnt=0;
		execute_copy_tool("local", conf_6);
		execute_copy_tool("local", dat_3_cp2);
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_6.subList(0, dsList_6.size()-3), start, 0))
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-3), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		waitto(down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")<migrate_count+1)
			fail("check migrate stat failed!");
		migrate_count = check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");

		//write new data and read point data while migrating
		execute_data_verify_tool();
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
		
		log.error("end kdb_2 dataserver test failover case 06");
	}
	
	@Test
	public void testFailover_07_add_3ds_then_write_then_kill_the_new_ds_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 07");
		int waitcnt=0;
		execute_copy_tool("local", conf_6);
		execute_copy_tool("local", dat_3_cp2);
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_6.subList(0, dsList_6.size()-3), start, 0))
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-3), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		waitto(down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")<migrate_count+1)
			fail("check migrate stat failed!");
		migrate_count = check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");

		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		waitto(3);
		
		//kill 1 new ds while migrating
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), stop, 0))fail("stop ds failed!");
		waitto(ds_down_time);
		
		while(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
		{
			waitto(2);
			if(++waitcnt>10)fail("migrate count wrong!");
		}
		waitcnt=0;
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("put data out time!");
		waitcnt=0;
		int datacnt_new=getVerifySuccessful();
		getPointVerifySuccessful();
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
		
		log.error("end kdb_2 dataserver test failover case 07");
	}
	
	@Test
	public void testFailover_08_add_3ds_then_write_then_kill_one_old_ds_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 08");
		int waitcnt=0;
		execute_copy_tool("local", conf_6);
		execute_copy_tool("local", dat_3_cp2);
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_6.subList(0, dsList_6.size()-3), start, 0))
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-3), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		waitto(down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")<migrate_count+1)
			fail("check migrate stat failed!");
		migrate_count = check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");

		//write new data and read point data while migrating
		execute_data_verify_tool();
		execute_point_data_verify_tool();
		waitto(3);
		
		//kill 1 old ds while migrating
		if(!control_ds((String) dsList_6.get(0), stop, 0))fail("stop ds failed!");
		waitto(ds_down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")!=migrate_count+1)
			fail("migrate count wrong!");
		
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
		
		log.error("end kdb_2 dataserver test failover case 08");
	}
	
	@Test
	public void testFailover_09_add_3ds_then_write_then_kill_all_new_ds_while_migrate()
	{
		log.error("start kdb_2 dataserver test failover case 09");
		int waitcnt=0;
		execute_copy_tool("local", conf_6);
		execute_copy_tool("local", dat_3_cp2);
		
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		log.error("change group.conf successful!");
		
		//start cluster
		if(!control_cluster(csList, dsList_6.subList(0, dsList_6.size()-3), start, 0))
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
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float==1);	
		log.error("finish put data!");

		int migrate_count=check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), start, 0))fail("start ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-3), start, 0))fail("start ds failed!");
		log.error("start ds successful!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(0), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), tair_bin+"etc/group.conf", (String)dsList_6.get(dsList_6.size()-3), "#"))fail("change group.conf failed!");
		waitto(down_time);
		
		if(check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log")==migrate_count)
			fail("check migrate stat failed!");
		migrate_count = check_keyword((String)csList.get(0),start_migrate,tair_bin+"logs/config.log");

		int fisishRebuildCount=check_keyword((String)csList.get(0),finish_rebuild,tair_bin+"logs/config.log");
		//write new data and read point data while migrating
		execute_data_verify_tool();
		waitto(3);
		
		//kill all old ds while migrating
		if(!control_ds((String) dsList_6.get(dsList_6.size()-1), stop, 0))fail("stop ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-2), stop, 0))fail("stop ds failed!");
		if(!control_ds((String) dsList_6.get(dsList_6.size()-3), stop, 0))fail("stop ds failed!");
		waitto(ds_down_time);
		
		if(check_keyword((String)csList.get(0),finish_rebuild,tair_bin+"logs/config.log")==fisishRebuildCount)
			fail("rebuild count wrong!");
		
		while(check_process("local", "kdb_tool")!=2)
		{
			waitto(5);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("put data out time!");
		waitcnt=0;
		int datacnt_new=getVerifySuccessful();
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
		
		execute_copy_tool("local", check_3_cp2);
		
		log.error("end kdb_2 dataserver test failover case 09");
	}
	
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		clean_point_tool("local");
		reset_cluster(csList,dsList_6);
		execute_copy_tool("local", copy_kv3_cp2);
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
