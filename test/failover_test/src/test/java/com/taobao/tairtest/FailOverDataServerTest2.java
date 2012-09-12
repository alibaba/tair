/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailOverDataServerTest2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_kill_out_time() {
		log.info("start DataServer test Failover case 01");
		int waitcnt = 0;
		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");
		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		// write data to cluster
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 5s for duplicate
		waitto(5);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 01");
	}

	@Test
	public void testFailover_02_restart_in_migrate() {
		log.info("start DataServer test Failover case 02");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		// write data to cluster
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(6);
			if (++waitcnt > 100)
				break;
		}
		if (waitcnt > 100)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		// wait for migrate start
		waitto(ds_down_time);
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration start!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// restart ds
		if (!control_ds(dsList.get(0), start, 0))
			fail("restart ds failed!");
		log.info("restart ds successful!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish  on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 300)
				break;
		}

		if (waitcnt > 300)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		// migrate need check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// read data from cluster
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		// end test
		log.info("end DataServer test Failover case 02");
	}

	@Test
	public void testFailover_03_restart_after_migrate() {
		log.info("start DataServer test Failover case 03");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		// write data to cluster
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 100)
				break;
		}
		if (waitcnt > 100)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 10s for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		// wait rebuild table
		waitto(ds_down_time);

		// wait migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		// restart ds
		if (!control_ds(dsList.get(0), start, 0))
			fail("restart ds failed!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 2) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,the seccond migration finished!");

		// check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// read data from cluster
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		// end test
		log.info("end DataServer test Failover case 03");
	}

	@Test
	public void testFailover_04_kill_one_inMigrate_outtime() {
		log.info("start DataServer test Failover case 06");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 10s for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");

		log.info("first data server has been closed!");
		waitto(down_time);

		// wait for migrate finish
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill another data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");

		waitto(ds_down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		// migrate need check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 04");
	}

	@Test
	public void testFailover_05_kill_one_inMigrate_inTime() {
		log.info("start DataServer test Failover case 05");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		waitto(ds_down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close data server failed!");
		log.info("second data server has been closed!");

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") == 0)
			fail("Already migration!");
		log.info("check migration not started!");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start data server failed!");
		log.info("second data server has been started");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration finised!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		log.info(getVerifySuccessful());
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 05");
	}

	@Test
	public void testFailover_06_kill_one_inMigrate_restartFirst() {
		log.info("start DataServer test Failover case 06");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 10s for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");
		waitto(ds_down_time);

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 06");
	}

	@Test
	public void testFailover_07_kill_one_inMigrate_restartBoth() {
		log.info("start DataServer test Failover case 07");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		waitto(ds_down_time);

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// restart second data server
		if (!control_ds(dsList.get(1), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 07");
	}

	@Test
	public void testFailover_08_kill_one_afterMigrate_restartFirst() {
		log.info("start DataServer test Failover case 08");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for second migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 2)
			fail("migrate not started!");
		log.info("check migration started!");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 08");
	}

	@Test
	public void testFailover_09_kill_one_afterMigrate_restartSecond() {
		log.info("start DataServer test Failover case 09");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for second migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 2)
			fail("migrate not start!");
		log.info("check migration started!");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		log.info("Read data over!");
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 09");
	}

	@Test
	public void testFailover_10_kill_one_afterMigrate_restartBoth() {
		log.info("start DataServer test Failover case 10");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for second migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 2)
			fail("migrate not start!");
		log.info("check migration started!");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 10");
	}

	@Test
	public void testFailover_11_kill_one_finishMigrate_restartFirst() {
		log.info("start DataServer test Failover case 11");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for first migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if first migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int finish_migrate_count = check_keyword(csList.get(0), finish_migrate,
				tair_bin + "logs/config.log");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != finish_migrate_count + 1) {
			log.debug("check if second migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 11");
	}

	@Test
	public void testFailover_12_kill_one_finishMigrate_restartSecond() {
		log.info("start DataServer test Failover case 12");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for first migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 10)
				break;
		}
		if (waitcnt > 10)
			fail("down time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check first if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no first migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int finish_migrate_count = check_keyword(csList.get(0), finish_migrate,
				tair_bin + "logs/config.log");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != finish_migrate_count + 1) {
			log.debug("check second if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 12");
	}

	@Test
	public void testFailover_13_kill_one_finishMigrate_restartBoth() {
		log.info("start DataServer test Failover case 13");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 10)
				break;
		}
		if (waitcnt > 10)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// kill second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for first migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check first if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int finish_migrate_count = check_keyword(csList.get(0), finish_migrate,
				tair_bin + "logs/config.log");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != finish_migrate_count + 1) {
			log.debug("check second if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no second migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		log.info("Read data over!");
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 13");
	}

	@Test
	public void testFailover_14_kill_twoDataServers_afterMigrate_restart() {
		log.info("start DataServer test Failover case 14");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close first data server failed!");
		log.info("first data server has been closed!");

		// close second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 10)
				break;
		}
		if (waitcnt > 10)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for migrate finish
		waitcnt = 0;
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check first if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		waitcnt = 0;
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 14");
	}

	@Test
	public void testFailover_15_kill_twoDataServers_finishMigrate_restart() {
		log.info("start DataServer test Failover case 15");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close first data server failed!");
		log.info("first data server has been closed!");

		// close second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if first migration finish on cs " + csList.get(0)
					+ " log ");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int finish_migrate_count = check_keyword(csList.get(0), finish_migrate,
				tair_bin + "logs/config.log");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");

		// restart second data server
		if (!control_ds(dsList.get(2), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != finish_migrate_count + 1) {
			log.debug("check if second migration finish on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 15");
	}

	@Test
	public void testFailover_16_kill_allDataServrs_restart_outTime() {
		log.info("start DataServer test Failover case 16");
		int waitcnt = 0;
		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// wait for duplicate
		waitto(10);

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// close all data servers
		if (!batch_control_ds(dsList, stop, 0))
			fail("close data servers failed!");
		log.info("data servers has been closed!");

		// wait for rebuild table
		waitto(ds_down_time);

		// restart all data servers
		if (!batch_control_ds(dsList, start, 0))
			fail("start data servers failed!");
		log.info("data servers has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		log.info("Read data over!");
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 16");
	}

	@Test
	public void testFailover_17_kill_one_afterMigate() {
		log.info("start DataServer test Failover case 17");
		int waitcnt = 0;

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// close first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close first data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if first migration start on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 10)
				break;
		}
		if (waitcnt > 10)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// close second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") == 0) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log ");
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		// migrate need check data
		execute_data_verify_tool();
		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		log.info("Read data over!");
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 17");
	}

	@Test
	public void testFailover_18_add_ds_finishMigrate_add_ds_finishMigrate_and_kill_one_finishMigrate_kill_one_finishMigration() {
		log.info("start DataServer test Failover case 18");
		int waitcnt = 0;

		// modify group configuration delete two ds
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 3), "#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		if (!control_ds(dsList.get(0), start, 0))
			fail("start ds failed");
		if (!control_ds(dsList.get(1), start, 0))
			fail("start ds failed");
		if (!control_ds(dsList.get(3), start, 0))
			fail("start ds failed");
		waitto(down_time);
		if (!batch_control_cs(csList, start, 0))
			fail("batch start cs failed!");
		log.info("start cluster successful!");

		waitto(down_time);

		// write verify data to cluster
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed!");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("put data over!");

		if (!control_ds(dsList.get(dsList.size() - 1), start, 0))
			fail("start ds failed!");
		log.info("start ds1 successful!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("check migrate1 time out!");
		waitcnt = 0;
		log.info("check migrate1 finished!");

		// verify data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify tool config file failed!");
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data1 time out!");
		waitcnt = 0;
		log.info("get data1 over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data 1!");

		if (!control_ds(dsList.get(dsList.size() - 3), start, 0))
			fail("start ds failed!");
		log.info("start ds2 successful!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 3), "#"))
			fail("change group.conf failed!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 2) {
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("check migrate2 time out!");
		waitcnt = 0;
		log.info("check migrate2 finished!");

		// verify data
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data2 time out!");
		waitcnt = 0;
		log.info("get data2 over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data2!");

		// close first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close first data server failed!");
		log.info("first data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 3) {
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("check migrate3 time out!");
		waitcnt = 0;
		log.info("check migrate3 finished!");

		// verify data
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data3 time out!");
		waitcnt = 0;
		log.info("get data3 over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data3!");

		// close second data server
		if (!control_ds(dsList.get(2), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");
		// wait rebuild table
		waitto(ds_down_time);

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 4) {
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("check migrate4 time out!");
		waitcnt = 0;
		log.info("check migrate4 finished!");

		// verify data
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data4 time out!");
		waitcnt = 0;
		log.info("get data4 over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data4!");

		log.info("end DataServer test Failover case 18");
	}

	@Test
	public void testFailover_19_kill_more_ds_util_one_left_then_restart() {
		log.info("start DataServer test Failover case 19");
		int waitcnt = 0;

		// start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failure");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("modify configure file failure");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failure");

		execute_data_verify_tool();

		// check verify
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// close some ds
		if (!batch_control_ds(dsList.subList(0, dsList.size() - 1), stop, 0))
			fail("close master cs failure!");
		log.info("some ds has been closed, only one alive!");
		waitto(ds_down_time);

		// restart some ds
		if (!batch_control_ds(dsList.subList(0, dsList.size() - 1), start, 0))
			fail("close master cs failure!");
		log.info("closed ds has been started!");
		waitto(5);
		if (touch_flag != 0) {
			touch_file(csList.get(0), tair_bin + groupconf);
			touch_file(csList.get(1), tair_bin + groupconf);
		}
		waitto(down_time);

		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		int datacnt1 = getVerifySuccessful();
		assertTrue("put successful rate small than migSucRate!", datacnt1
				/ put_count_float > migSucRate);
		log.info("Write data over!");

		// read data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failure");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
			fail("modify configure file failure");
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		log.info("Read data over!");
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;

		// verify get result
		assertEquals("get data verify failure!", datacnt + datacnt1,
				getVerifySuccessful());
		log.info("Successfully verified data!");

		// end test
		log.info("end DataServer test Failover case 19");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		clean_tool(local);
		resetCluster(csList, dsList);
		batch_uncomment(csList, tair_bin + groupconf, dsList, "#");
		// execute_shift_tool(local, "conf5");//for kdb
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "0"))
			fail("modify configure file failed!");
		if (!batch_modify(csList, tair_bin + groupconf, copycount, "2"))
			fail("modify configure file failure");
		if (!batch_modify(dsList, tair_bin + groupconf, copycount, "2"))
			fail("modify configure file failure");
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
		clean_tool(local);
		resetCluster(csList, dsList);
		batch_uncomment(csList, tair_bin + groupconf, dsList, "#");
	}
}
