/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailOverConfigServerDoubleRack2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_kill_master_cs_and_one_ds_on_master_rack() {
		log.info("start config test Failover case 01");
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

		//
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
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		// wait 10s for duplicate
		waitto(10);

		if (!control_cs(csList.get(0), stop, 0))
			fail("close master config server failed!");
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server failed!");
		log.info("master cs and one ds in master rack have been closed!");
		waitto(ds_down_time);

		// check migration stat of start
		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != 1)
			fail("migrate not start!");
		log.info("check migrate started!");

		int version_count = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		if (!control_ds(dsList.get(0), start, 0))
			fail("start data server failed!");
		if (!control_cs(csList.get(0), start, 0))
			fail("start master config server failed!");
		log.info("master cs and one ds in master rack have been started!");
		waitto(down_time);

		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != version_count + 1)
			fail("migrate not start!");
		log.info("check migrate started!");

		// change test tool's configuration
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
		log.info("end config test Failover case 01");
	}

	@Test
	public void testFailover_02_kill_slave_cs_and_one_ds_on_slave_rack() {
		log.info("start config test Failover case 02");
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

		//
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
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		// wait 5s for duplicate
		waitto(10);

		int versionCount = check_keyword(csList.get(0), finish_rebuild,
				tair_bin + "logs/config.log");

		if (!control_ds(dsList.get(1), stop, 0))
			fail("close data server failed!");
		waitto(ds_down_time);
		if (!control_cs(csList.get(1), stop, 0))
			fail("close slave config server failed!");
		waitto(down_time);
		log.info("slave cs and one ds in slave rack have been closed!");

		// check migration stat of start
		if (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") != versionCount + 1)
			fail("version not changed!");
		log.info("version changed!");

		int version_count = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");

		if (!control_cs(csList.get(1), start, 0))
			fail("start slave config server failed!");
		if (!control_ds(dsList.get(1), start, 0))
			fail("start data server failed!");
		log.info("slave cs and one ds in slave rack have been started!");
		waitto(ds_down_time);

		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != version_count + 1)
			fail("migrate not start!");
		log.info("check migrate started!");

		// change test tool's configuration
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
		log.info("end config test Failover case 02");
	}

	@Test
	public void testFailover_03_kill_master_rack() {
		log.info("start config test Failover case 03");
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
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		waitto(10);

		if (!control_cs(csList.get(0), stop, 0))
			fail("close master config server failed!");
		for (int i = 0; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), stop, 0))
				fail("close data server failed!");
			waitto(ds_down_time);
		}
		log.info("master rack have been closed!");

		// check migration stat of start
		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != dsList.size() / 2)
			fail("migrate not start!");
		log.info("check migrate started!");

		// write data while migrating
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");
		waitto(10);

		int version_count = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		for (int i = 0; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), start, 0))
				fail("start data server failed!");
			waitto(ds_down_time);
		}
		if (!control_cs(csList.get(0), start, 0))
			fail("start master config server failed!");
		log.info("master rack have been started!");
		waitto(down_time);

		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != version_count + dsList.size() / 2)
			fail("migrate not start!");
		log.info("check migrate started!");

		// change test tool's configuration
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
		log.info(getVerifySuccessful());
		assertEquals("verify data failed!",
				getVerifySuccessful() > datacnt * 1.9);
		log.info("Successfully Verified data!");

		// end test
		log.info("end config test Failover case 03");
	}

	@Test
	public void testFailover_04_kill_slave_rack() {
		log.info("start config test Failover case 04");
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
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		// wait 5s for duplicate
		waitto(10);

		for (int i = 1; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), stop, 0))
				fail("close data server failed!");
			waitto(ds_down_time);
		}
		if (!control_cs(csList.get(1), stop, 0))
			fail("close slave config server failed!");
		log.info("slave rack have been closed!");

		// check migration stat of start
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != dsList.size() / 2)
			fail("migrate not start!");
		log.info("check migrate started!");

		// write data while migrating
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");
		waitto(10);

		int version_count = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");

		for (int i = 1; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), start, 0))
				fail("start data server failed!");
			waitto(down_time);
		}
		if (!control_cs(csList.get(1), start, 0))
			fail("start slave config server failed!");
		log.info("master rack have been started!");
		// wait 10s for data server start
		waitto(down_time);

		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != version_count + dsList.size() / 2)
			fail("migrate not start!");
		log.info("check migrate started!");

		// change test tool's configuration
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
		log.info(getVerifySuccessful());
		assertEquals("verify data failed!",
				getVerifySuccessful() > datacnt * 1.9);
		log.info("Successfully Verified data!");

		// end test
		log.info("end config test Failover case 04");
	}

	@Test
	public void testFailover_05_kill_master_rack_and_one_ds_on_slave_rack() {
		log.info("start config test Failover case 05");
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
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");
		waitto(10);

		if (!control_cs(csList.get(0), stop, 0))
			fail("close master config server failed!");
		for (int i = 0; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), stop, 0))
				fail("close data server failed!");
			waitto(ds_down_time);
		}

		int version_count = check_keyword(csList.get(1), finish_rebuild,
				tair_bin + "logs/config.log");
		int migrate_count = check_keyword(csList.get(1), start_migrate,
				tair_bin + "logs/config.log");

		if (!control_ds(dsList.get(1), stop, 0))
			fail("close data server in slave rack failed!");
		log.info("master rack and one ds in slave rack have been closed!");
		waitto(ds_down_time);

		// check migration stat of start
		if (check_keyword(csList.get(1), finish_rebuild, tair_bin
				+ "logs/config.log") != version_count + 1)
			fail("migrate not start!");
		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 1)
			fail("migrate not start!");
		log.info("check migrate started!");

		// write data while migrating
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");
		waitto(10);

		// change test tool's configuration
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
		assertTrue("verify data failed!", datacnt / put_count_float > 1.5);
		log.info("Successfully Verified data!");

		for (int i = 0; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), start, 0))
				fail("start data server failed!");
			waitto(ds_down_time);
		}
		if (!control_cs(csList.get(0), start, 0))
			fail("start master config server failed!");
		log.info("master rack have been started!");
		waitto(down_time);

		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 1 + dsList.size() / 2)
			fail("migrate not start!");
		if (check_keyword(csList.get(1), finish_rebuild, tair_bin
				+ "logs/config.log") != version_count + 1 + dsList.size() / 2)
			fail("version not change!");
		log.info("check migrate started!");

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
		log.info(getVerifySuccessful());
		assertTrue("verify data failed!", datacnt / put_count_float > 1.5);
		log.info("Successfully Verified data!");

		// end test
		log.info("end config test Failover case 05");
	}

	@Test
	public void testFailover_06_kill_slave_rack_and_one_ds_on_master_rack() {
		log.info("start config test Failover case 06");
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
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		// wait 5s for duplicate
		waitto(10);

		for (int i = 1; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), stop, 0))
				fail("close data server failed!");
			waitto(ds_down_time);
		}
		if (!control_cs(csList.get(1), stop, 0))
			fail("close slave config server failed!");
		log.info("master rack and one ds in slave rack have been closed!");
		waitto(down_time);

		int version_count = check_keyword(csList.get(0), finish_rebuild,
				tair_bin + "logs/config.log");
		int migrate_count = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");

		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server in master rack failed!");
		waitto(ds_down_time);

		// check migration stat of start
		if (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") != version_count + 1)
			fail("version not change!");
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 1)
			fail("migrate not start!");
		log.info("check migrate started!");

		// write data while migrating
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");
		waitto(10);

		// change test tool's configuration
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
		assertTrue("verify data failed!", datacnt / put_count_float > 1.5);
		log.info("Successfully Verified data!");

		for (int i = 1; i < dsList.size(); i += 2) {
			if (!control_ds(dsList.get(i), start, 0))
				fail("start data server failed!");
			waitto(ds_down_time);
		}
		if (!control_cs(csList.get(1), start, 0))
			fail("start slave config server failed!");
		log.info("slave rack have been started!");
		waitto(down_time);

		if (check_keyword(csList.get(0), finish_rebuild, tair_bin
				+ "logs/config.log") != version_count + 1 + dsList.size() / 2)
			fail("version not change!");
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 1 + dsList.size() / 2)
			fail("migrate not start!");
		log.info("check migrate started!");

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
		log.info(getVerifySuccessful());
		assertTrue("verify data failed!", datacnt / put_count_float > 1.5);
		log.info("Successfully Verified data!");

		// end test
		log.info("end config test Failover case 06");
	}

	@Test
	public void testFailover_07_kill_one_ds_on_each_rack() {
		log.info("start config test Failover case 07");
		int waitcnt = 0;
		controlCluster(csList, dsList, start, 0);
		log.info("wait system initialize ...");
		waitto(down_time);
		log.info("Start Cluster Successful!");

		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("modify configure file failed");
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
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		// wait for duplicate
		waitto(10);

		// check if version changed after one ds stoped on master rack
		// int version_count=check_keyword(csList.get(0),
		// finish_rebuild, tair_bin+"logs/config.log");
		int migrate_count = check_keyword(csList.get(0), start_migrate,
				tair_bin + "logs/config.log");

		if (!control_ds(dsList.get(0), stop, 0))
			fail("close data server on master rack failed!");
		waitto(ds_down_time);

		// if(check_keyword(csList.get(0), finish_rebuild,
		// tair_bin+"logs/config.log")!=version_count+1)
		// fail("version not changed after stop one ds on master rack!");
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 1)
			fail("migrate not start after stop one ds on master rack!");

		// check if version changed after one ds stoped on slave rack
		if (!control_ds(dsList.get(1), stop, 0))
			fail("close data server on slave rack failed!");
		waitto(ds_down_time);

		// if(check_keyword(csList.get(0), finish_rebuild,
		// tair_bin+"logs/config.log")!=version_count+2)
		// fail("version not changed after stop one ds on slave rack!");
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 2)
			fail("migrate not start after stop one ds on master rack!");
		log.info("migrate started!");

		// write data while migrating
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");
		waitto(10);

		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed");
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");
		datacnt = getVerifySuccessful();
		assertTrue("verify data failed!", datacnt / put_count_float > 1.6);
		log.info("Successfully Verified data!");

		if (!control_ds(dsList.get(0), start, 0))
			fail("start ds on master rack failed!");
		waitto(down_time);
		if (!control_cs(csList.get(1), start, 0))
			fail("start ds on slave rack failed!");
		waitto(down_time);
		log.info("the stoped ds on each rack have been started!");

		// if(check_keyword(csList.get(0), finish_rebuild,
		// tair_bin+"logs/config.log")!=version_count+4)
		// fail("version not changed after restart one ds on each rack!");
		if (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != migrate_count + 4)
			fail("migrate not start after restart one ds on each rack!");
		log.info("migrate started!");

		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("Read data time out!");
		waitcnt = 0;
		log.info("Read data over!");

		log.info(getVerifySuccessful());
		assertTrue("verify data failed!", datacnt / put_count_float > 1.6);
		log.info("Successfully Verified data!");

		// end test
		log.info("end config test Failover case 07");
	}

	@Test
	public void testFailover_08_accept_add_ds_without_touch() {
		log.info("start config test Failover case 08");
		int waitcnt = 0;

		if (!modify_config_file(csList.get(0), tair_bin + groupconf,
				"_accept_strategy", "1"))
			fail("modify _accept_strategy failure");
		log.info("_accept_strategy has been changed!");

		// start cluster
		controlCluster(csList, dsList.subList(0, dsList.size() - 1), start, 0);
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

		//
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
		assertTrue("put successful rate small than 90%!", datacnt
				/ put_count_float > 0.9);
		log.info("Write data over!");

		// close ds
		if (!control_ds(dsList.get(dsList.size() - 1), start, 0))
			fail("start ds failure!");
		log.info("last data server has been started!");

		waitto(2);
		// check if strike migrate without touch configure file
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration start on cs " + csList.get(0)
					+ " log");
			waitto(2);
			if (++waitcnt > 10)
				break;
		}
		if (waitcnt > 10)
			fail("down time arrived,but no migration start!");
		waitcnt = 0;
		log.info("down time arrived,migration started!");

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

		// check data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failure");
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
		log.info(getVerifySuccessful());
		assertEquals("verify data failure!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		// end test
		log.info("end config test Failover case 08");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		clean_tool(local);
		resetCluster(csList, dsList);
		batch_uncomment(csList, tair_bin + groupconf, dsList, "#");
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
