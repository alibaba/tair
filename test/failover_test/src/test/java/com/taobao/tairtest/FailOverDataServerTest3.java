/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailOverDataServerTest3 extends FailOverBaseCase{

	//�ڻָ�ʱ����ָ�dataserver
	@Test
	public void testFailover_02_kill_out_time()
	{   
		log.info("start DataServer test Failover case 02");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");
		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0))fail("close data server failed!");

		log.info("first data server has been closed!");

		//change test tool's configuration
		/*		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
//			fail("modify configure file failed");	
		//read data from cluster
//		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(++waitcnt>150)break;
		}	
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		String verify="cd "+test_bin+" && ";
		verify+="tail -4 datadbg0.log|grep \"Successful\"|awk -F\" \" \'{print $3}\'";
		STAFResult result=executeShell(stafhandle, local, verify);
		String stdout=getShellOutput(result);
		if((new Integer(stdout.trim())).intValue()!=100000)fail("Not All data were entered!");
		log.info("Successfully Verified data!");

		 */		
		//wait rebuild table
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 02");
	}

	//��Ǩ�ƹ���лָ�
	@Test
	public void testFailover_03_restart_in_migrate()
	{ 
		log.info("start DataServer test Failover case 03");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, "500000"))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(6);
			if(++waitcnt>100)break;
		}

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");

		//wait 30s for duplicate
		waitto(30);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0))fail("close data server failed!");

		log.info("first data server has been closed!");


		//wait for migrate start
		waitto(down_time);
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150) fail("donw time arrived,but no migration start!");
		waitcnt=0;
		log.info("donw time arrived,migration started!");

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("restart ds successful!");

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		log.info("touch group.conf");
		waitto(down_time);

		//wait migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish  on cs "+csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>300)break;
		}

		if(waitcnt>300) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//migrate need check data 
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		//end test
		log.info("end DataServer test Failover case 03");

	}

	//��Ǩ�ƺ�ָ�
	@Test
	public void testFailover_04_restart_after_migrate()
	{
		log.info("start DataServer test Failover case 04");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");  
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");
		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>100)break;
		}	

		if(waitcnt>100)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");

		//wait 10s for duplicate
		waitto(10);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0))fail("close data server failed!");

		log.info("first data server has been closed!");


		//wait rebuild table
		waitto(down_time);

		//wait migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");

		//wait 3s for data server start
		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		log.info("touch group.conf");
		waitto(down_time);

		// wait second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin + "logs/config.log") != 2) {
			log.debug("check if migration finish on cs " + csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,the seccond migration finished!");

		// check data
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		//end test
		log.info("end DataServer test Failover case 04");

	}

	//kill 40% dataserver
	@Test
	public void testFailover_05_kill_more_servers()
	{ 
		log.info("start DataServer test Failover case 05");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");

		//wait 10s for duplicate
		waitto(10);

		//close 3 data server
		if(!batch_control_ds(dsList.subList(0, 2), stop, 0))
			fail("close 2 data server failed!");

		log.info("2 data server has been closed!");
		log.info("wait 2 seconds to restart before rebuild ...");
		waitto(5);
		if(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")==0)fail("Already migration!");
		//start the  2 data server again
		if(!batch_control_ds(dsList.subList(0, 2), start, 0)) 
			fail("start data server failed!");
		log.info("restart ds successful!");
		//wait 10s for data server start
		waitto(10);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		//end test
		log.info("end DataServer test Failover case 05");
	}


	//first kill one ,and then kill another one in migrate
	@Test
	public void testFailover_06_kill_one_inMigrate_outtime()
	{
		log.info("start DataServer test Failover case 06");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0))fail("close data server failed!");

		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill another data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close second data server failed!");
		log.info("second data server has been closed!");

		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//migrate need check data 
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 06");
	}

	//first kill one , then kill another one and restart it before second migrate
	@Test
	public void testFailover_07_kill_one_inMigrate_inTime()
	{
		log.info("start DataServer test Failover case 07");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		log.info(getVerifySuccessful());
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0))fail("close data server failed!");

		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close data server failed!");
		log.info("second data server has been closed!");
		log.info("wait 3 seconds to restart the secord ds  before rebuild ...");
		waitto(3);
		//		if(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)fail("Already migration!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start data server failed!");
		log.info("second data server has been started");
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(3);
			if(++waitcnt>150)break;
		}
		if(waitcnt>450) fail("donw time arrived,but no migration finised!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		log.info(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 07");
	}

	//first kill one ,and then kill another one, and restart first one
	@Test
	public void testFailover_08_kill_one_inMigrate_restartFirst()
	{
		log.info("start DataServer test Failover case 08");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}		

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210) fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 08");
	}

	//kill another data server ,and restart both in migrate
	@Test
	public void testFailover_09_kill_one_inMigrate_restartBoth()
	{
		log.info("start DataServer test Failover case 09");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");


		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 09");	
	}

	//kill another data server ,and then migrate, restart first data server
	@Test
	public void testFailover_10_kill_one_afterMigrate_restartFirst()
	{
		log.info("start DataServer test Failover case 10");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for second migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("donw time arrived,the second migration started!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 10");

	}

	//kill another data server ,and after second migrate, restart second data server
	@Test
	public void testFailover_11_kill_one_afterMigrate_restartSecond()
	{
		log.info("start DataServer test Failover case 11");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for second migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("donw time arrived, the second migration started!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 11");

	}

	//kill another data server ,and after second migrate, restart both
	@Test
	public void testFailover_12_kill_one_afterMigrate_restartBoth()
	{
		log.info("start DataServer test Failover case 12");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for second migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("donw time arrived,the second migration started!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 12");

	}


	//kill another data server,and after migrate finish, restart first data server
	@Test
	public void testFailover_23_kill_one_finishMigrate_restartFirst()
	{
		log.info("start DataServer test Failover case 23");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for first migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for second  migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if second migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 23");

	}

	//kill another data server,and after migrate finish, restart second data server
	@Test
	public void testFailover_13_kill_one_finishMigrate_restartSecond()
	{
		log.info("start DataServer test Failover case 13");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for first migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for second migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no first migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for second migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check second if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 13");

	}

	//kill another data server,and after migrate finish, restart both
	@Test
	public void testFailover_14_kill_one_finishMigrate_restartBoth()
	{
		log.info("start DataServer test Failover case 14");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close one data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//kill second data server
		if(!control_ds(dsList.get(1), stop, 0))fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for first migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin + "logs/config.log") != 2) {
			log.debug("check second if migration finish on cs " + csList.get(0) + " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("donw time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 14");
	}

	//kill two data servers,and after migrate start, restart the servers
	@Test
	public void testFailover_15_kill_twoDataServers_afterMigrate_restart()
	{
		log.info("start DataServer test Failover case 15");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	                   

		//wait 5s for duplicate
		waitto(5);

		//close first data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close first data server failed!");
		log.info("first data server has been closed!");

		//close second data server
		if(!control_ds(dsList.get(1), stop, 0)) fail("close second data server failed!");
		log.info("second data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")==0)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for migrate finish
		waitcnt=0;
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check first if migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		waitcnt=0;
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 15");
	}

	//kill two data servers,and after migrate finish, restart the servers
	@Test
	public void testFailover_16_kill_twoDataServers_finishMigrate_restart()
	{
		log.info("start DataServer test Failover case 16");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}		

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close first data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close first data server failed!");
		log.info("first data server has been closed!");

		//close second data server
		if(!control_ds(dsList.get(1), stop, 0)) fail("close second data server failed!");
		log.info("second data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration finish on cs "+csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}

		//restart first data server
		if(!control_ds(dsList.get(0), start, 0))fail("start first data server failed!");
		log.info("first data server has been started!");

		//restart second data server
		if(!control_ds(dsList.get(1), start, 0))fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		//wait for second migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if second migration finish on cs "+csList.get(0)+" log");
			waitto(10);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");
		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 16");
	}
	//kill all data servers, and restart out time
	@Test
	public void testFailover_18_kill_allDataServrs_restart_outTime()
	{
		log.info("start DataServer test Failover case 18");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//wait 5s for duplicate
		waitto(5);

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//close all data servers
		if(!batch_control_ds(dsList, stop, 0)) fail("close data servers failed!");
		log.info("data servers has been closed!");

		//wait for rebuild table
		waitto(down_time);

		//wait for migrate start
		//while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		//{
		//	log.info("check if migration start on cs "+csList.get(0)+" log ");
		//	try{
		//		Thread.sleep(2000);
		//	}
		//	catch(Exception e){
		//		e.printStackTrace();
		//	}
		//}

		//restart all data servers
		if(!batch_control_ds(dsList, start, 0))fail("start data servers failed!");
		log.info("data servers has been started!");

		waitto(30);

		//touch the group file
		if (touch_file(csList.get(0), tair_bin+groupconf)!= 0)
			fail(" touch group file failed!");
		waitto(down_time);

		waitto(30);

		//wait for migrate finish
		//while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		//{
		//	log.info("check if migration finish on cs "+csList.get(0)+" log ");
		//	try{
		//		Thread.sleep(2000);
		//		}
		//	catch(Exception e){
		//		e.printStackTrace();
		//		}
		//	}

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 18");
	}

	//kill the data server after migrate start
	@Test
	public void testFailover_21_kill_one_afterMigate()
	{
		log.info("start DataServer test Failover case 21");
		int waitcnt=0;
		if(!control_cluster(csList, dsList, start, 0))fail("start cluster failed!");

		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");	

		//wait 5s for duplicate
		waitto(5);

		//close first data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close first data server failed!");
		log.info("first data server has been closed!");

		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//wait for migrate start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if first migration start on cs "+csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150) fail("donw time arrived,but no migration started!");
		waitcnt = 0;
		log.info("donw time arrived,migration started!");

		//close second data server
		if(!control_ds(dsList.get(1), stop, 0)) fail("close second data server failed!");
		log.info("second data server has been closed!");

		//wait rebuild table
		waitto(down_time);

		//wait for migrate finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log ");
			waitto(2);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210) fail("donw time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("donw time arrived,migration finished!");

		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))                                     fail("modify configure file failed");   
		//migrate need check data 
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		//end test
		log.info("end DataServer test Failover case 21");
	}
	@Test
	public void testFailover_24_add_ds_finishMigrate_add_ds_finishMigrate_and_kill_one_finishMigrate_kill_one_finishMigration()
	{
		log.info("start DataServer test Failover case 24");

		int waitcnt=0;

		//modify group configuration .delete two ds
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		if(!control_cluster(csList, dsList.subList(0, dsList.size()-2), start, 0))fail("start cluster failed!");
		log.info("start cluster successful!");

		waitto(down_time);

		//write verify data to cluster
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))fail("modify configure file failed!");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))fail("modify configure file failed");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate small than 90%!",datacnt/put_count_float>0.9);	
		log.info("put data over!");

		if(!control_ds(dsList.get(dsList.size()-1), start, 0))fail("start ds failed!");
		log.info("start ds1 successful!");

		//uncomment cs group.conf
		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file(csList.get(0), tair_bin+groupconf);
		log.info("change group.conf and touch it");

		waitto(down_time);

		//check migration stat of finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate1 time out!");
		waitcnt=0;
		log.info("check migrate1 finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data1 time out!");
		waitcnt=0;
		log.info("get data1 over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data 1!");

		if(!control_ds(dsList.get(dsList.size()-2), start, 0))fail("start ds failed!");
		log.info("start ds2 successful!");

		//uncomment cs group.conf
		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-2), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file(csList.get(0), tair_bin+groupconf);
		log.info("change group.conf and touch it");

		waitto(down_time);

		//check migration stat of finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate2 time out!");
		waitcnt=0;
		log.info("check migrate2 finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data2 time out!");
		waitcnt=0;
		log.info("get data2 over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data2!");

		//close first data server
		if(!control_ds(dsList.get(0), stop, 0)) fail("close first data server failed!");
		log.info("first data server has been closed!");
		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//check migration stat of finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=3)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate3 time out!");
		waitcnt=0;
		log.info("check migrate3 finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data3 time out!");
		waitcnt=0;
		log.info("get data3 over!");

		//close second data server
		if(!control_ds(dsList.get(1), stop, 0)) fail("close first data server failed!");
		log.info("second data server has been closed!");
		waitto(2);

		//wait rebuild table
		waitto(down_time);

		//check migration stat of finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=4)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate4 time out!");
		waitcnt=0;
		log.info("check migrate4 finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data4 time out!");
		waitcnt=0;
		log.info("get data4 over!");

		log.info("end DataServer test Failover case 24");
	}

	@Before
	public void setUp()
	{
        log.info("clean tool and cluster while setUp!");
		clean_tool(local);
		reset_cluster(csList,dsList);
		execute_shift_tool(local, "conf5");
		batch_uncomment(csList, tair_bin+groupconf, dsList, "#");
		if(!batch_modify(csList, tair_bin+groupconf, copycount, "3"))
			fail("modify configure file failed");
		if(!batch_modify(dsList, tair_bin+groupconf, copycount, "3"))
			fail("modify configure file failed");
	}

	@After
	public void tearDown()
	{
		log.info("clean tool and cluster while tearDown!");
		clean_tool(local);
		reset_cluster(csList,dsList);
		batch_uncomment(csList, tair_bin+groupconf, dsList, "#");		
	}

}
