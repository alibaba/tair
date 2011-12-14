package com.taobao.javaclient;
import org.junit.Test;

/**
 * @author ashu.cs
 * 
 */

public class FailOverDataServerTest2 extends FailOverBaseCase{

	@Test
	public void testFailover_02_kill_out_time()
	{   
		log.error("start DataServer test Failover case 02");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

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

		//end test
		log.error("end DataServer test Failover case 02");
	}

	@Test
	public void testFailover_03_restart_in_migrate()
	{ 
		log.error("start DataServer test Failover case 03");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");

		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait for migrate start
		waitto(FailOverBaseCase.ds_down_time);
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("down time arrived,but no migration start!");
		waitcnt=0;
		log.error("down time arrived,migration started!");

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
		log.error("restart ds successful!");
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

		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish  on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>300)break;
		}

		if(waitcnt>300) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

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

		//end test
		log.error("end DataServer test Failover case 03");
	}

	@Test
	public void testFailover_04_restart_after_migrate()
	{
		log.error("start DataServer test Failover case 04");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");  
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))
			fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

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

		//restart ds
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("restart ds failed!");
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

		// wait second migrate finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin + "logs/config.log") != 2) {
			log.debug("check if migration finish on cs " + (String) csList.get(0) + " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,the seccond migration finished!");

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

		//end test
		log.error("end DataServer test Failover case 04");
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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate finish
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill another data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("second data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time/2);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==2)
			fail("Already migration!");
		log.error("check migration not started!");

		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("second data server has been started");

		waitto(FailOverBaseCase.down_time);

		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("down time arrived,but no migration finised!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);

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

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt=0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		waitto(FailOverBaseCase.ds_down_time);

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

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
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

		//restart second data server
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for second migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
			fail("migrate not started!");
		log.error("check migration started!");

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

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for second migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for second migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		if(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
			fail("migrate not start!");
		log.error("check migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for first migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if first migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		int finish_migrate_count=check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
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

		//wait for second  migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=finish_migrate_count+1)
		{
			log.debug("check if second migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for first migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10) fail("down time arrived,but no second migration started!");
		waitcnt = 0;
		log.error("down time arrived,migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no first migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		int finish_migrate_count=check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=finish_migrate_count+1)
		{
			log.debug("check second if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close one data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10) fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.error("down time arrived,migration started!");

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

		//kill second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close seconde data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for first migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		int finish_migrate_count=check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for second migrate finish
		while (check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin + "logs/config.log")!=finish_migrate_count+1)
		{
			log.debug("check second if migration finish on cs " + (String) csList.get(0) + " log");
			waitto(3);
			if (++waitcnt > 200)break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close first data server failed!");
		log.error("first data server has been closed!");
		//close second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10) fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.error("down time arrived,migration started!");

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

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check first if migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close first data server failed!");
		log.error("first data server has been closed!");
		//close second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if first migration finish on cs "+(String)csList.get(0)+" log ");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt = 0; 
		log.error("down time arrived,migration finished!");

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

		int finish_migrate_count=check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//restart first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start first data server failed!");
		log.error("first data server has been started!");
		//restart second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start second data server failed!");
		log.error("second data server has been started!");
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

		//wait for second migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=finish_migrate_count+1)
		{
			log.debug("check if second migration finish on cs "+(String)csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//end test
		log.error("end DataServer test Failover case 16");
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

		//close all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.stop, 0))fail("close data servers failed!");
		log.error("data servers has been closed!");
		//wait for rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//restart all data servers
		if(!batch_control_ds(dsList, FailOverBaseCase.start, 0))fail("start data servers failed!");
		log.error("data servers has been started!");
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

		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close first data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

		//wait for migrate start
		while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration start on cs "+(String)csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10) fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.error("down time arrived,migration started!");

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

		//close second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//wait for migrate finish
		while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			log.debug("check if migration finish on cs "+(String)csList.get(0)+" log ");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200) fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.error("down time arrived,migration finished!");

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

		//end test
		log.error("end DataServer test Failover case 21");
	}

	@Test
	public void testFailover_24_add_ds_finishMigrate_add_ds_finishMigrate_and_kill_one_finishMigrate_kill_one_finishMigration()
	{
		log.error("start DataServer test Failover case 24");

		int waitcnt=0;

		//modify group configuration delete two ds
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-3), "#"))fail("change group.conf failed!");
		if(!comment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String)dsList.get(dsList.size()-3), "#"))fail("change group.conf failed!");
		log.error("group.conf has been changed!");

		if(!control_ds((String)dsList.get(0), FailOverBaseCase.start, 0))fail("start ds failed");
		if(!control_ds((String)dsList.get(2), FailOverBaseCase.start, 0))fail("start ds failed");
		if(!control_ds((String)dsList.get(4), FailOverBaseCase.start, 0))fail("start ds failed");
		if(!control_ds((String)dsList.get(5), FailOverBaseCase.start, 0))fail("start ds failed");
		waitto(FailOverBaseCase.down_time);
		if(!batch_control_cs(csList, FailOverBaseCase.start, 0))fail("batch start cs failed!");	
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

		if(!control_ds((String) dsList.get(dsList.size()-1), FailOverBaseCase.start, 0))
			fail("start ds failed!");
		log.error("start ds1 successful!");

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
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate1 time out!");
		waitcnt=0;
		log.error("check migrate1 finished!");

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

		if(!control_ds((String) dsList.get(dsList.size()-3), FailOverBaseCase.start, 0))
			fail("start ds failed!");
		log.error("start ds2 successful!");

		//uncomment cs group.conf
		if(!uncomment_line((String)csList.get(0), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-3), "#"))fail("change group.conf failed!");
		if(!uncomment_line((String)csList.get(1), FailOverBaseCase.tair_bin+"etc/group.conf", (String) dsList.get(dsList.size()-3), "#"))fail("change group.conf failed!");

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

		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=2)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate2 time out!");
		waitcnt=0;
		log.error("check migrate2 finished!");

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

		//close first data server
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close first data server failed!");
		log.error("first data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=3)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate3 time out!");
		waitcnt=0;
		log.error("check migrate3 finished!");

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

		//close second data server
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close second data server failed!");
		log.error("second data server has been closed!");
		//wait rebuild table
		waitto(FailOverBaseCase.ds_down_time);

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

		//check migration stat of finish
		while(check_keyword((String) csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=4)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate4 time out!");
		waitcnt=0;
		log.error("check migrate4 finished!");

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

		log.error("end DataServer test Failover case 24");
	}

	////////////////////////////////////
	/////// cross different rack ///////
	////////////////////////////////////
	@Test
	public void testFailover_05_kill_more_servers_on_master_rack()
	{ 
		log.error("start DataServer test Failover case 05");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close more data server in master rack 
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.stop, 0))fail("close data server failed!");
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("all data server in master rack have been closed!");
		waitto(FailOverBaseCase.ds_down_time);

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("check migrate time out!");
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

		//start the 2 data server again
		if(!control_ds((String) dsList.get(0), FailOverBaseCase.start, 0))fail("start data server failed!");
		if(!control_ds((String) dsList.get(2), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("all data server in master rack have been started!");
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

		//end test
		log.error("end DataServer test Failover case 05");
	}

	@Test
	public void testFailover_25_kill_more_servers_on_slave_rack()
	{ 
		log.error("start DataServer test Failover case 25");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close more data server in master rack 
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0))fail("close data server failed!");
		if(!control_ds((String) dsList.get(3), FailOverBaseCase.stop, 0))fail("close data server failed!");
		log.error("all data server in slave rack have been closed!");
		waitto(FailOverBaseCase.ds_down_time);

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")==0)
		{
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("check migrate time out!");
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

		//start the 2 data server again
		if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0))fail("start data server failed!");
		if(!control_ds((String) dsList.get(3), FailOverBaseCase.start, 0))fail("start data server failed!");
		log.error("all data server in slave rack have been started!");
		//wait for data server start
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

		//end test
		log.error("end DataServer test Failover case 25");
	}

	@Test
	public void testFailover_26_kill_all_servers_on_master_rack()
	{ 
		log.error("start DataServer test Failover case 26");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close all data server in master rack 
		for(int i=0; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.stop, 0))
				fail("close data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		log.error("all data server in master rack have been closed!");

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=dsList.size()/2)
		{
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("check migrate time out!");
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

		//start the 2 data server again
		for(int j=0; j<dsList.size(); j+=2)
		{
			if(!control_ds((String) dsList.get(j), FailOverBaseCase.start, 0))
				fail("start data server failed!");
		}
		log.error("all data server in master rack have been started!");
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

		//end test
		log.error("end DataServer test Failover case 26");
	}

	@Test
	public void testFailover_27_kill_all_servers_on_slave_rack()
	{ 
		log.error("start DataServer test Failover case 27");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

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

		//close all data server in master rack 
		for(int i=1; i<dsList.size(); i+=2)
		{
			if(!control_ds((String) dsList.get(i), FailOverBaseCase.stop, 0))
				fail("close data server failed!");
			waitto(FailOverBaseCase.ds_down_time);
		}
		log.error("all data server in slave rack have been closed!");

		//check migration stat of start
		while(check_keyword((String) csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")!=dsList.size()/2)
		{
			waitto(2);
			if(++waitcnt>10)break;
		}
		if(waitcnt>10)fail("check migrate time out!");
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

		//start the  2 data server again
		for(int j=1; j<dsList.size(); j+=2)
		{
			if(!control_ds((String) dsList.get(j), FailOverBaseCase.start, 0))
				fail("start data server failed!");
		}
		log.error("all data server in slave rack have been started!");
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

		//end test
		log.error("end DataServer test Failover case 27");
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
