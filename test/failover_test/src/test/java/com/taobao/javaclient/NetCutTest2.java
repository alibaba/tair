package com.taobao.javaclient;
import org.junit.Test;

/**
 * @author ashu.cs
 * 
 */

public class NetCutTest2 extends FailOverBaseCase{

	@Test
	public void testFailover_01_cutoff_a1b1_a1b2_b1a2()
	{
		log.error("start netcut test Failover case 01");
		int waitcnt=0;

		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		//end test
		log.error("end netcut test Failover case 01");
	}

	@Test
	public void testFailover_02_cutoff_a1b1_b1a2_a1b2()
	{
		log.error("start netcut test Failover case 02");
		int waitcnt=0;

		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_equal((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		//end test
		log.error("end netcut test Failover case 02");
	}

	@Test
	public void testFailover_03_cutoff_b1a2_a1b1_a1b2()
	{
		log.error("start netcut test Failover case 03");
		int waitcnt=0;

		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		//end test
		log.error("end netcut test Failover case 03");
	}

	@Test
	public void testFailover_04_cutoff_a1b2_b1a2_a1b1()
	{
		log.error("start netcut test Failover case 04");
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		///////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a2b1
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));		
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 04");
	}

	@Test
	public void testFailover_05_cutoff_b1a2_a1b1_a1b2()
	{
		log.error("start netcut test Failover case 05");
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		///////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: all
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));		
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 05");
	}

	@Test
	public void testFailover_06_cutoff_b1a2_a1b2_a1b1()
	{
		log.error("start netcut test Failover case 06");
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		///////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: all
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));		
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 06");
	}

	@Test
	public void testFailover_07_cutoff_a1b1_b1a2_a1b2_recover_a1b1_a1b2_b1a2()
	{
		log.error("start netcut test Failover case 07");
		int waitcnt=0;

		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_equal((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("////////////////////////////////////");
		log.error("/////////// recover net ////////////");
		log.error("////////////////////////////////////");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b1
		recover_net((String)csList.get(0));
		recover_net((String)csList.get(1));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b2
		recover_net((String)dsList.get(3));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: b1a2
		recover_net((String)dsList.get(2));
		recover_net((String)dsList.get(4));
		recover_net((String)dsList.get(5));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		//end test
		log.error("end netcut test Failover case 07");
	}

	@Test
	public void testFailover_08_cutoff_b1a2_a1b1_a1b2_recover_a1b1_b1a2_a1b2()
	{
		log.error("start netcut test Failover case 08");
		int waitcnt=0;

		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("////////////////////////////////////");
		log.error("/////////// recover net ////////////");
		log.error("////////////////////////////////////");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b1
		recover_net((String)csList.get(0));
		recover_net((String)csList.get(1));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: b1a2
		recover_net((String)dsList.get(2));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_equal((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b2
		recover_net((String)dsList.get(3));
		recover_net((String)dsList.get(4));
		recover_net((String)dsList.get(5));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		//end test
		log.error("end netcut test Failover case 08");
	}

	@Test
	public void testFailover_09_cutoff_a1b2_b1a2_a1b1_recover_a1b2_a1b1_b1a2()
	{
		log.error("start netcut test Failover case 09");

		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		///////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a2b1
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));		
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("////////////////////////////////////");
		log.error("/////////// recover net ////////////");
		log.error("////////////////////////////////////");

		/////////////////////////////////////////////////////////////////////////////
		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		//recover net: a1b2
		recover_net((String)csList.get(0));
		recover_net((String)dsList.get(3));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		//slave cs will receive a few package from master cs
		//		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b1
		recover_net((String)csList.get(0));
		recover_net((String)csList.get(1));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		//slave cs may be linked to master cs, then will not rebuild
		//		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: b1a2
		recover_net((String)csList.get(1));
		recover_net((String)dsList.get(2));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_equal((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: all
		recover_net((String)dsList.get(4));
		recover_net((String)dsList.get(5));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	

		//wait reconnect to server
		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 09");
	}

	@Test
	public void testFailover_10_cutoff_b1a2_a1b1_a1b2_recover_a1b2_b1a2_a1b1()
	{
		log.error("start netcut test Failover case 10");

		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		///////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: all
		shutoff_net((String)csList.get(0), (String)dsList.get(5));		
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("////////////////////////////////////");
		log.error("/////////// recover net ////////////");
		log.error("////////////////////////////////////");

		/////////////////////////////////////////////////////////////////////////////
		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		//recover net: a1b2
		recover_net((String)csList.get(0));
		recover_net((String)dsList.get(3));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		//		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		//recover net: b1a2
		recover_net((String)csList.get(1));
		recover_net((String)dsList.get(2));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		//		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		//recover all
		recover_net((String)dsList.get(4));
		recover_net((String)dsList.get(5));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	

		//wait reconnect to server
		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 10");
	}

	@Test
	public void testFailover_11_cutoff_b1a2_a1b1_a1b2_recover_b1a2_a1b1_a1b2()
	{
		log.error("start netcut test Failover case 11");

		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		///////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		///////////////////////////////////////////////////////////////////
		//shut off: all
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));		
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);	
		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("////////////////////////////////////");
		log.error("/////////// recover net ////////////");
		log.error("////////////////////////////////////");

		/////////////////////////////////////////////////////////////////////
		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		//recover net: b1a2
		recover_net((String)csList.get(1));
		recover_net((String)dsList.get(2));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_equal((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////
		//recover net: a1b1
		recover_net((String)csList.get(0));
		recover_net((String)csList.get(1));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////
		//recover net: a1b2
		recover_net((String)csList.get(0));
		recover_net((String)dsList.get(3));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");           

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		//recover all
		recover_net((String)dsList.get(4));
		recover_net((String)dsList.get(5));
		waitto(FailOverBaseCase.down_time);
		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");	

		//wait reconnect to server
		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 11");
	}

	@Test
	public void testFailover_12_cutoff_a1b1_a1b2_b1a2_recover_b1a2_a1b2_a1b1()
	{
		log.error("start netcut test Failover case 12");
		int waitcnt=0;

		//start cluster
		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		int migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		int migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b1
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//shut off: b1a2
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));
		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("////////////////////////////////////");
		log.error("/////////// recover net ////////////");
		log.error("////////////////////////////////////");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: b1a2
		recover_net((String)csList.get(1));
		recover_net((String)dsList.get(2));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_equal((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b2
		recover_net((String)csList.get(0));
		recover_net((String)dsList.get(3));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_change((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

//		//wait reconnect to server
//		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
//		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
//		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on master cs reconnect to server successful!");
//		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
//		{
//			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
//				fail("fail count is more than 200 times since last fail!");
//			waitto(4);
//			waitcnt++;
//			if(waitcnt>100)
//				fail("wait for client reconnect time out!");
//		}
//		waitcnt=0;
//		log.error("client on slave cs reconnect to server successful!");

		/////////////////////////////////////////////////////////////////////////////
		//recover net: a1b1 and all
		recover_net((String)dsList.get(4));
		recover_net((String)dsList.get(5));
		waitto(FailOverBaseCase.down_time);

		wait_keyword_change((String)csList.get(0), FailOverBaseCase.start_migrate, migrateCount1);
		wait_keyword_equal((String)csList.get(1), FailOverBaseCase.start_migrate, migrateCount2);
		migrateCount1=check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		migrateCount2=check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//wait reconnect to server
		fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		//end test
		log.error("end netcut test Failover case 12");
	}

	@Test
	public void testFailover_13_cutoff_a1b2_recover_before_rebuild()
	{
		log.error("start netcut test Failover case 13");

		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		//shut off: a1b2
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		//		waitto(FailOverBaseCase.ds_down_time/4);

		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1);
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2);

		//recover_net:a1b2
		recover_net((String)csList.get(0));
		recover_net((String)dsList.get(3));
		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );
		waitto(FailOverBaseCase.down_time);

		wait_keyword_equal((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_equal((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 13");
	}

	@Test
	public void testFailover_14_cut_all_flash()
	{
		log.error("start netcut test Failover case 14");

		if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failure!");
		log.error("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);
		log.error("Start Cluster Successful!");

		int waitcnt=0;

		int init_migrateCount1,init_migrateCount2;
		init_migrateCount1 = check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");
		init_migrateCount2 = check_keyword((String)csList.get(1), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log");

		//start java client on master cs
		if(!control_client((String)csList.get(0), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on master cs successful!");
		//start java client on slave cs
		if(!control_client((String)csList.get(1), start, 0))fail("start javaclient failed!");
		log.error("start javaclient on slave cs successful!");
		waitto(down_time/2);
		assertTrue(get_keyword((String)csList.get(0), logfile, "code")==0);
		assertTrue(get_keyword((String)csList.get(1), logfile, "code")==0);
		log.error("assert javaclient connection successful!");
		//write data on client
		execute_data_verify_tool(ycsb_client);
		wait_ycsb_on_mac(ycsb_client);

		//shut off: all
		shutoff_net((String)csList.get(0), (String)csList.get(1));
		shutoff_net((String)csList.get(0), (String)dsList.get(3));
		shutoff_net((String)csList.get(0), (String)dsList.get(5));
		shutoff_net((String)csList.get(1), (String)dsList.get(2));
		shutoff_net((String)csList.get(1), (String)dsList.get(4));

		waitto(FailOverBaseCase.ds_down_time);

		wait_keyword_change((String)csList.get(0),FailOverBaseCase.start_migrate, init_migrateCount1 );
		wait_keyword_change((String)csList.get(1),FailOverBaseCase.start_migrate, init_migrateCount2 );

		//wait reconnect to server
		int fail_count1 = get_keyword((String)csList.get(0), logfile, "fail_count");
		int fail_count2 = get_keyword((String)csList.get(1), logfile, "fail_count");
		while(batch_get_keyword((String)csList.get(0), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(0), logfile, "fail_count")>fail_count1+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on master cs reconnect to server successful!");
		while(batch_get_keyword((String)csList.get(1), logfile, "code")!=0)
		{
			if(get_keyword((String)csList.get(1), logfile, "fail_count")>fail_count2+200)
				fail("fail count is more than 200 times since last fail!");
			waitto(4);
			waitcnt++;
			if(waitcnt>100)
				fail("wait for client reconnect time out!");
		}
		waitcnt=0;
		log.error("client on slave cs reconnect to server successful!");

		log.error("end netcut test Failover case 14");
	}


	public void setUp()
	{   
		log.error("clean tool and cluster!");
		if(!control_client((String)csList.get(0), FailOverBaseCase.stop, 1))fail("stop client failed!");
		if(!control_client((String)csList.get(1), FailOverBaseCase.stop, 1))fail("stop client failed!");
		clean_tool((String)csList.get(0));
		clean_tool((String)csList.get(1));
		log.error("stop client successful!");
		reset_cluster(csList,dsList);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
		if(!batch_modify(dsList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
		if(!batch_recover_net(dsList))fail("recover net failure");
	}

	public void tearDown()
	{
		log.error("clean tool and cluster!");
		if(!control_client((String)csList.get(0), FailOverBaseCase.stop, 1))fail("stop client failed!");
		if(!control_client((String)csList.get(1), FailOverBaseCase.stop, 1))fail("stop client failed!");
		clean_tool((String)csList.get(0));
		clean_tool((String)csList.get(1));
		log.error("stop client successful!");
		reset_cluster(csList,dsList);
		if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
		if(!batch_modify(dsList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
		if(!batch_recover_net(dsList))fail("recover net failure");
	}
}
