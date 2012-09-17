/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailOverDataServerTest1 extends FailOverBaseCase {

	@Test
	public void testFailover_01_add_ds_stop_in_time() {
		log.info("start DataServer test Failover case 01");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// save put result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("finish put data!");

		// wait 5s for duplicate
		waitto(5);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");

		log.info("first data server has been started!");
		waitto(2);

		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 0)
			fail("Already migration!");

		// stop the data server again
		if (!control_ds(dsList.get(0), stop, 0))
			fail("stop data server failed!");
		log.info("stop ds successful!");

		// wait 10s for data server start
		waitto(10);

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
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

		log.info("end DataServer test Failover case 01");
	}

	@Test
	public void testFailover_02_add_one_ds() {
		log.info("start DataServer test Failover case 02");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been added!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 210)
				break;
		}
		if (waitcnt > 210)
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

		log.info("end DataServer test Failover case 02");
	}

	@Test
	public void testFailover_03_add_ds_close_in_migrate() {
		log.info("start DataServer test Failover case 03");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, "500000"))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

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
		assertTrue("put successful rate small than normSucRate!",
				datacnt / 500000.0 > normSucRate);
		log.info("Write data over!");

		// wait 30s for duplicate
		waitto(30);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been added!");

		// wait for migrate start
		waitto(down_time);
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

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop ds
		if (!control_ds(dsList.get(0), stop, 0))
			fail("restop ds failed!");
		log.info("restop ds successful!");

		waitto(down_time);

		// wait rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}
		if (waitcnt > 300)
			fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

		// migrate need check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
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
		assertTrue("verify data failed!",
				getVerifySuccessful() / 500000.0 > 0.79);
		log.info("Successfully Verified data!");

		log.info("end DataServer test Failover case 03");
	}

	@Test
	public void testFailover_04_add_ds_reclose_after_migrate() {
		log.info("start DataServer test Failover case 04");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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
			if (++waitcnt > 100)
				break;
		}
		if (waitcnt > 100)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		log.info(getVerifySuccessful());
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 10s for duplicate
		waitto(10);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait migrate finish
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

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");
		// restop ds
		if (!control_ds(dsList.get(0), stop, 0))
			fail("restart ds failed!");
		log.info("ds has been stoped");

		// wait for data server stop
		waitto(down_time);

		// wait second rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish on cs " + csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,the seccond rebuild finished!");

		// check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);
		log.info("Successfully Verified data!");

		log.info("end DataServer test Failover case 04");
	}

	@Test
	public void testFailover_05_add_two_ds() {
		log.info("start DataServer test Failover case 05");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait migrate finish
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

		log.info("end DataServer test Failover case 05");
	}

	// first add one ,and then add another one in migrate
	@Test
	public void testFailover_06_add_one_inMigrate_add_another_ds() {
		log.info("start DataServer test Failover case 06");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		waitto(down_time);

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 210)
				break;
		}
		if (waitcnt > 210)
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

		log.info("end DataServer test Failover case 06");
	}

	// first add one , then add another one and restart it before second migrate
	@Test
	public void testFailover_07_add_one_inMigrate_inTime() {
		log.info("start DataServer test Failover case 07");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		log.info("wait 3 seconds to reclose the secord ds  before rebuild ...");
		waitto(3);
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("Already migration!");

		// restart second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("start data server failed!");
		log.info("second data server has been close");

		waitto(ds_down_time);

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
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 07");
	}

	@Test
	public void testFailover_08_add_one_inMigrate_reCloseFirst() {
		log.info("start DataServer test Failover case 08");
		int waitcnt = 0;
		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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
		log.info(getVerifySuccessful());
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");
		waitto(down_time);

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// reclose first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("stop first data server failed!");
		log.info("first data server has been stoped!");

		// wait for migrate finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish on cs " + csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 210)
				break;
		}
		if (waitcnt > 210)
			fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 08");
	}

	@Test
	public void testFailover_09_add_one_inMigrate_reCloseBoth() {
		log.info("start DataServer test Failover case 09");
		int waitcnt = 0;
		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");
		waitto(down_time);

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// reclose first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("stop first data server failed!");
		log.info("first data server has been stoped!");

		// reclose second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("stop second data server failed!");
		log.info("second data server has been stoped!");

		waitto(down_time);

		// wait for rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish on cs " + csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.59);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 09");
	}

	@Test
	public void testFailover_10_add_one_afterMigrate_recloseFirst() {
		log.info("start DataServer test Failover case 10");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for second migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 2) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("down time arrived,the second migration started!");

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("stop first data server failed!");
		log.info("first data server has been stoped!");

		waitto(down_time);

		// wait for rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish on cs " + csList.get(0) + " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.75);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 10");
	}

	@Test
	public void testFailover_11_add_one_afterMigrate_restartSecond() {
		log.info("start DataServer test Failover case 11");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
		"#"))
	fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for second migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 2) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("down time arrived, the second migration started!");

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("stop second data server failed!");
		log.info("second data server has been stoped!");

		waitto(down_time);

		// wait for rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish on cs " + csList.get(0) + " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		log.info("Read data over");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 11");
	}

	@Test
	public void testFailover_12_add_one_afterMigrate_reStopBoth() {
		log.info("start DataServer test Failover case 12");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);
		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("firse data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for second migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 2) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("down time arrived,the second migration started!");

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("stop first data server failed!");
		log.info("first data server has been stop!");
		waitto(5);

		// restart second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("stop second data server failed!");
		log.info("second data server has been stop!");

		waitto(ds_down_time);

		// wait for rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish on cs " + csList.get(0) + " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.59);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 12");
	}

	@Test
	public void testFailover_13_add_one_finishMigrate_reStopSecond() {
		log.info("start DataServer test Failover case 13");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for first migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no second migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check first if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no first migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");
		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("stop second data server failed!");
		log.info("second data server has been stoped!");

		waitto(down_time);

		// wait for rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check fourth if rebuild finish on cs " + csList.get(0)
					+ " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no fourth rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 13");
	}

	@Test
	public void testFailover_14_add_one_finishMigrate_restartBoth() {
		log.info("start DataServer test Failover case 14");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 5s for duplicate
		waitto(5);

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for migrate start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration started!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

		// /uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("second data server has been start!");

		// wait rebuild table
		waitto(down_time);

		// wait for first migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check first if migration finish on cs " + csList.get(0)
					+ " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("stop first data server failed!");
		log.info("first data server has been stoped!");
		waitto(5);

		// restart second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("stop second data server failed!");
		log.info("second data server has been stoped!");

		waitto(down_time);

		// wait for fourth rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check fourth rebuild finish on cs " + csList.get(0)
					+ " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no fourth rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.59);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 14");
	}

	@Test
	public void testFailover_15_add_ds_close_another_in_migrate() {
		log.info("start DataServer test Failover case 15");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, datasize, "500000"))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("modify configure file failed");

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
		log.info(getVerifySuccessful());
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("Write data over!");

		// wait 30s for duplicate
		waitto(30);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been added!");

		// wait for migrate start
		waitto(down_time);
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

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// restop ds
		if (!control_ds(dsList.get(1), stop, 0))
			fail("restop another ds failed!");
		log.info("restop another ds successful!");

		waitto(down_time);

		// wait rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}
		if (waitcnt > 300)
			fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

		// migrate need check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.74);
		log.info("Successfully Verified data!");

		log.info("end DataServer test Failover case 15");
	}

	@Test
	public void testFailover_16_add_ds_close_another_in_migrate_restart_ontime() {
		log.info("start DataServer test Failover case 16");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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

		// wait 30s for duplicate
		waitto(30);

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been added!");

		// wait for migrate start
		waitto(down_time);
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

		// restop ds
		if (!control_ds(dsList.get(1), stop, 0))
			fail("restop another ds failed!");
		log.info("restop another ds successful!");

		waitto(2);

		// restart ds
		if (!control_ds(dsList.get(1), start, 0))
			fail("restart another ds failed!");
		log.info("restart another ds successful!");
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);

		// wait migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.78);
		log.info("Successfully Verified data!");

		log.info("end DataServer test Failover case 16");
	}

	@Test
	public void testFailover_17_kill_one_restart_inTime() {
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

		// wait 5s for duplicate
		waitto(5);

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");

		log.info("first data server has been closed!");

		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != 0)
			fail("Already migration!");

		// restart data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start data server failed!");
		log.info("data server has been restarted!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 17");
	}

	@Test
	public void testFailover_18_kill_one_ds() {
		log.info("start DataServer test Failover case 18");
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

		// wait 5s for duplicate
		waitto(5);

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(down_time);

		// wait rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}
		if (waitcnt > 300)
			fail("down time arrived,but no rebuild finished!");
		waitcnt = 0;
		log.info("down time arrived,rebuild finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 18");
	}

	@Test
	public void testFailover_19_kill_one_add_ds_before_rebuild() {
		log.info("start DataServer test Failover case 19");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

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
		// wait 5s for duplicate
		waitto(5);

		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");
		// close one data server
		if (!control_ds(dsList.get(dsList.size() - 1), stop, 0))
			fail("close data server failed!");

		log.info("first data server has been closed!");
		waitto(5);

		if (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld)
			fail("Already rebuild table!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(0),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been started!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.74);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 19");
	}

	@Test
	public void testFailover_20_kill_one_add_ds_after_rebuild() {
		log.info("start DataServer test Failover case 20");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");
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

		// wait 5s for duplicate
		waitto(5);
		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(down_time);

		// wait rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf, dsList.get(1),
				"#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		log.info("first data server has been started!");

		waitto(down_time);
		// wait migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}
		if (waitcnt > 300)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int versionNow = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");
		assertTrue("check version count not correct!", versionNow > versionOld);
		waitto(down_time);
		assertTrue(
				"check version count not correct!",
				check_keyword(csList.get(0), finish_rebuild, tair_bin
						+ "logs/config.log") == versionNow);

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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.74);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 20");
	}

	@Test
	public void testFailover_21_kill_one_finishMigrate_restartFirst() {
		log.info("start DataServer test Failover case 21");
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

		// wait 5s for duplicate
		waitto(5);

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		// wait rebuild table
		waitto(down_time);

		// wait for rebuild start
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild start on cs " + csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no rebuild started!");
		waitcnt = 0;
		log.info("down time arrived,rebuild started!");

		// record the version times
		versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// kill second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("close seconde data server failed!");
		log.info("second data server has been closed!");

		// wait rebuild table
		waitto(down_time);

		// wait for rebuild start
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild start on cs " + csList.get(0) + " log");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no rebuild started!");
		waitcnt = 0;
		log.info("down time arrived,rebuild started!");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");

		waitto(3);

		// touch the group file
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		waitto(down_time);

		// wait for second migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if second migration finish on cs " + csList.get(0)
					+ " log");
			waitto(10);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int versionNow = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");
		assertTrue("check version count not correct!", versionNow > versionOld);
		waitto(down_time);
		assertTrue(
				"check version count not correct!",
				check_keyword(csList.get(0), finish_rebuild, tair_bin
						+ "logs/config.log") == versionNow);

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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.58);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 21");
	}

	@Test
	public void testFailover_22_add_ds_finishMigrate_add_ds_finishMigrate_and_kill_one_finishMigrate_kill_one_finishMigration() {
		log.info("start DataServer test Failover case 22");
		int waitcnt = 0;

		// modify group configuration .delete two ds
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 2), "#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList, start, 0);
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
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");
		waitto(down_time);

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
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

		if (!control_ds(dsList.get(dsList.size() - 2), start, 0))
			fail("start ds failed!");
		log.info("start ds2 successful!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 2), "#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");

		waitto(down_time);

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check migrate2 time out!");
		waitcnt = 0;
		log.info("check migrate2 finished!");

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

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close first data server failed!");
		log.info("first data server has been closed!");
		waitto(2);

		// wait rebuild table
		waitto(down_time);

		// check rebuild stat of finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check rebuild5 time out!");
		waitcnt = 0;
		log.info("check rebuild5 finished!");

		// verify data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify tool config file failed!");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
			fail("modify configure file failed");
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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.79);

		// record the version times
		versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");
		waitto(2);

		// wait rebuild table
		waitto(down_time);

		// check rebuild stat of finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check rebuild time out!");
		waitcnt = 0;
		log.info("check second rebuild finished!");

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
			fail("get data4 time out!");
		waitcnt = 0;
		log.info("get data4 over!");
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.58);
		log.info("end DataServer test Failover case 24");
	}

	@Test
	public void testFailover_23_kill_one_close_ds_after_rebuild() {
		log.info("start DataServer test Failover case 23");
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

		// wait 5s for duplicate
		waitto(5);
		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close one data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("first data server has been closed!");

		waitto(down_time);

		// wait rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}
		// record the version times
		versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close one data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("close second data server failed!");

		log.info("second data server has been closed!");

		waitto(down_time);
		// wait rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if rebuild finish  on cs " + csList.get(0)
					+ " log");
			waitto(3);
			if (++waitcnt > 300)
				break;
		}
		if (waitcnt > 300)
			fail("down time arrived,but no migration finished!");

		waitcnt = 0;
		log.info("down time arrived,migration finished!");

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
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		// verify get result
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.58);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 23");
	}

	@Test
	public void testFailover_24_kill_twoDataServers_finishMigrate_restart() {
		log.info("start DataServer test Failover case 24");
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

		// wait 5s for duplicate
		waitto(5);

		// record the version times
		int versionOld = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");

		// close first data server
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close first data server failed!");
		log.info("first data server has been closed!");

		// close second data server
		if (!control_ds(dsList.get(1), stop, 0))
			fail("close second data server failed!");
		log.info("second data server has been closed!");

		waitto(2);

		// wait rebuild table
		waitto(down_time);

		// wait for rebuild finish
		while (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") == versionOld) {
			log.debug("check if first rebuild finish on cs " + csList.get(0)
					+ " log ");
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		log.info("first rebuild has been finished");

		// restart first data server
		if (!control_ds(dsList.get(0), start, 0))
			fail("start first data server failed!");
		log.info("first data server has been started!");

		// restart second data server
		if (!control_ds(dsList.get(1), start, 0))
			fail("start second data server failed!");
		log.info("second data server has been started!");

		waitto(3);

		// touch the group file
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		waitto(down_time);

		// wait for first migrate finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if first migration finish on cs " + csList.get(0)
					+ " log");
			waitto(10);
			if (++waitcnt > 210)
				break;
		}
		if (waitcnt > 210)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		int versionNow = check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log");
		assertTrue("check version count not correct!", versionNow > versionOld);
		waitto(down_time);
		assertTrue(
				"check version count not correct!",
				check_keyword(csList.get(0), finish_rebuild, tair_bin
						+ "logs/config.log") == versionNow);

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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.58);
		log.info("Successfully verified data!");

		log.info("end DataServer test Failover case 24");
	}

	@Test
	public void testFailover_25_kill_more_servers() {
		log.info("start DataServer test Failover case 25");
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

		// close 3 data server
		if (!batch_control_ds(dsList.subList(0, 3), stop, 0))
			fail("close 3 data server failed!");

		log.info("3 data server has been closed!");
		log.info("wait 2 seconds to restart before rebuild ...");
		waitto(2);

		// start the 3 data server again
		if (!batch_control_ds(dsList.subList(0, 3), start, 0))
			fail("start data server failed!");
		log.info("restart ds successful!");
		// wait 10s for data server start
		waitto(10);

		// change test tool's configuration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
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
		assertTrue("verify data failed!", getVerifySuccessful()
				/ put_count_float > 0.39);
		log.info("Successfully Verified data!");

		log.info("end DataServer test Failover case 25");
	}

	@BeforeClass
	public static void subBeforeClass() {
		log.info("reset copycount while subBeforeClass!");
		FailOverBaseCase fb = new FailOverBaseCase();
		if (!fb.batch_modify(csList, tair_bin + groupconf, copycount, "1"))
			fb.fail("modify configure file failure");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		if (before.equals(clean_option)) {
			clean_tool(local);
			resetCluster(csList, dsList);
			// execute_shift_tool(local, "conf5");//for kdb
			if(!batch_uncomment(csList, tair_bin + groupconf, dsList, "#"))
				fail("batch uncomment ds in group.conf failed!");
			if (!modify_config_file(local, test_bin + toolconf, proxyflag, "0"))
				fail("modify configure file failed!");
		}
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
		if (after.equals(clean_option)) {
			clean_tool(local);
			resetCluster(csList, dsList);
			if(!batch_uncomment(csList, tair_bin + groupconf, dsList, "#"))
				fail("batch uncomment ds in group.conf failed!");
			if (!modify_config_file(local, test_bin + toolconf, proxyflag, "0"))
				fail("modify configure file failed!");
		}
	}

}
