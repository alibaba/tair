package com.taobao.javaclient;
import org.junit.Test;

/**
 * @author ashu.cs
 *
 */

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

		//shut down ds and cs
		if(!batch_control_cs(csList, FailOverBaseCase.stop, 0))fail("stop cs failed!");
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0))fail("stop ds failed!");

		waitto(FailOverBaseCase.down_time);

		//restart cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("restart cluster failed!");
		log.error("restart cluster successful!");

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

		//add ds and migration
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");

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

		log.error("end cluster test failover case 02");
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
