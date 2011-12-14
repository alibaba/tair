package com.taobao.javaclient;
import org.junit.Test;

/**
 * @author ashu.cs
 *
 */

public class FunctionDataServerTest2 extends FailOverBaseCase {

	@Test
	public void testFunction_01_add_ds_and_migration()
	{
		log.error("start function test case 01");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");

		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");

		waitto(FailOverBaseCase.down_time);

		//start java client
		if(!control_client(java_client, start, 0))fail("start javaclient failed!");
		log.error("start javaclient successful!");
		waitto(down_time/2);
		assertTrue(get_keyword(java_client, logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//start ycsb client
		execute_data_verify_tool(ycsb_client);
		while(check_ycsb(ycsb_client)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("put data out time!");
		waitcnt=0;
		log.error("finish put data!");

		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");

		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");

		waitto(FailOverBaseCase.down_time);

		//wait reconnect to server
		int fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");

		//wait reconnect to server
		fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		log.error("end function test case 01");
	}

	@Test
	public void testFunction_02_add_ds_and_migration_then_write_20w_data()
	{
		log.error("start function test case 02");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");

		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");

		waitto(FailOverBaseCase.down_time);

		//start java client
		if(!control_client(java_client, start, 0))fail("start javaclient failed!");
		log.error("start javaclient successful!");
		waitto(down_time/2);
		assertTrue(get_keyword(java_client, logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//start ycsb client
		execute_data_verify_tool(ycsb_client);
		while(check_ycsb(ycsb_client)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("put data out time!");
		waitcnt=0;
		log.error("finish put data!");

		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");

		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");

		waitto(FailOverBaseCase.down_time);

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate started!");

		//wait reconnect to server
		int fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		//write data while migration
		//start ycsb client
		execute_data_verify_tool(ycsb_client);
		while(check_ycsb(ycsb_client)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("put data out time!");
		waitcnt=0;
		log.error("finish put data!");

		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");

		//wait reconnect to server
		fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		log.error("end function test case 02");
	}

	@Test
	public void testFunction_03_add_ds_and_migration_then_get_data()
	{
		log.error("start function test case 03");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");

		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");

		waitto(FailOverBaseCase.down_time);

		//start java client
		if(!control_client(java_client, start, 0))fail("start javaclient failed!");
		log.error("start javaclient successful!");
		waitto(down_time/2);
		assertTrue(get_keyword(java_client, logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//start ycsb client
		execute_data_verify_tool(ycsb_client);
		while(check_ycsb(ycsb_client)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("put data out time!");
		waitcnt=0;
		log.error("finish put data!");

		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");

		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");

		waitto(FailOverBaseCase.down_time);

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate started!");

		//wait reconnect to server
		int fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		log.error("end function test case 03");
	}

	@Test
	public void testFunction_04_add_ds_and_migration_then_remove_data()
	{
		log.error("start function test case 04");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");

		if(!control_cluster(csList, dsList.subList(0, dsList.size()-1), FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(FailOverBaseCase.down_time);

		//start java client
		if(!control_client(java_client, start, 0))fail("start javaclient failed!");
		log.error("start javaclient successful!");
		waitto(down_time/2);
		assertTrue(get_keyword(java_client, logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//start ycsb client
		execute_data_verify_tool(ycsb_client);
		while(check_ycsb(ycsb_client)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("put data out time!");
		waitcnt=0;
		log.error("finish put data!");

		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))fail("start ds failed!");
		log.error("start ds successful!");

		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");

		waitto(FailOverBaseCase.down_time);

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate started!");

		//wait reconnect to server
		int fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate time out!");
		waitcnt=0;
		log.error("check migrate finished!");

		//wait reconnect to server
		fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		log.error("end function test case 04");
	}

	@Test
	public void testFunction_06_recover__ds_before_rebuild_120s_noRebuild()
	{
		log.error("start function test case 06");
		int waitcnt=0;
		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("Start Cluster Successful!");

		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		//start java client
		if(!control_client(java_client, start, 0))fail("start javaclient failed!");
		log.error("start javaclient successful!");
		waitto(down_time/2);
		assertTrue(get_keyword(java_client, logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//start ycsb client
		execute_data_verify_tool(ycsb_client);
		while(check_ycsb(ycsb_client)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}
		if(waitcnt>100)fail("put data out time!");
		waitcnt=0;
		log.error("finish put data!");

		//close ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close ds failed!");
		log.error("ds has been closed!");
		log.error("wait to restart before rebuild ...");

		waitto(FailOverBaseCase.ds_down_time);

		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)fail("Already migration!");

		//wait reconnect to server
		int fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		//restart ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failed!");
		log.error("Restart ds successful!");	

		//wait reconnect to server
		fail_count = get_keyword(java_client, logfile, "fail_count");
		while(batch_get_keyword(java_client, logfile, "code")!=0)
		{
			if(get_keyword(java_client, logfile, "fail_count")>fail_count+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client reconnect to server successful!");

		//verify no migration
		if(check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=0)
			fail("Already migration!");

		//end test
		log.error("end function test case 06");
	}

	public void setUp()
	{   
		log.error("clean tool and cluster!");
		if(!control_client(java_client, FailOverBaseCase.stop, 1)) 
			fail("stop client failed!");
		clean_tool(java_client);
		log.error("stop client successful!");
		reset_cluster(csList,dsList);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
		if(!batch_modify(dsList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
	}

	public void tearDown()
	{   
		log.error("clean tool and cluster!");
		if(!control_client(java_client, FailOverBaseCase.stop, 1)) 
			fail("stop client failed!");
		clean_tool(java_client);
		log.error("stop client successful!");
		reset_cluster(csList,dsList);
		batch_uncomment(csList, FailOverBaseCase.tair_bin+"etc/group.conf", dsList, "#");
	}
}
