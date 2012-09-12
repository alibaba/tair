/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FunctionDataServerTest1 extends FailOverBaseCase {

	@Test
	public void testFunction_01_add_ds_and_migration() {
		log.info("start function test case 01");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList.subList(0, dsList.size() - 1), start, 0);
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
		log.info("start ds successful!");

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
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate finished!");

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
			fail("get data time out!");
		waitcnt = 0;
		log.info("get data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end function test case 01");
	}

	@Test
	public void testFunction_02_add_ds_and_migration_then_write_20w_data() {
		log.info("start function test case 02");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList.subList(0, dsList.size() - 1), start, 0);
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
		log.info("start ds successful!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");

		waitto(down_time);

		// check migration stat of start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate started!");

		// write data while migration
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(4);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;
		// verify get result
		int datacnt1 = getVerifySuccessful();
		assertTrue("put successful rate small than migSucRate!", datacnt1
				/ put_count_float > migSucRate);
		datacnt += datacnt1;
		log.info("put data over!");

		// check migration stat of finish
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate finished!");

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
			fail("get data time out!");
		waitcnt = 0;
		log.info("get data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end function test case 02");
	}

	@Test
	public void testFunction_03_add_ds_and_migration_then_get_data() {
		log.info("start function test case 03");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList.subList(0, dsList.size() - 1), start, 0);
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
		log.info("start ds successful!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");

		waitto(down_time);

		// check migration stat of start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate started!");

		// get data while migration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed!");
		if (!modify_config_file(local, test_bin + toolconf, proxyflag, "1"))
			fail("modify configure file failed!");
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data time out!");
		waitcnt = 0;
		log.info("get data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end function test case 03");
	}

	@Test
	public void testFunction_04_add_ds_and_migration_then_remove_data() {
		log.info("start function test case 04");
		int waitcnt = 0;

		// modify group configuration
		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		controlCluster(csList, dsList.subList(0, dsList.size() - 1), start, 0);
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
		log.info("start ds successful!");

		// uncomment cs group.conf
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		// touch group.conf
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		log.info("change group.conf and touch it");

		waitto(down_time);

		// check migration stat of start
		while (check_keyword(csList.get(0), start_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate started!");

		// remove data while migration
		if (!modify_config_file(local, test_bin + toolconf, actiontype, rem))
			fail("modify configure file failed!");
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 300)
				break;
		}
		if (waitcnt > 300)
			fail("rem data time out!");
		waitcnt = 0;
		// verify get result
		int remcnt = getVerifySuccessful();
		assertTrue("rem successful rate small than 90%!", remcnt
				/ put_count_float > 0.9);
		datacnt -= remcnt;
		log.info("rem data over!");

		// check migration stat of start
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate finished!");

		// verify data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("modify configure file failed!");
		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data time out!");
		waitcnt = 0;
		log.info("get data over!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end function test case 04");
	}

	@Test
	public void testFunction_05_recover_ds_before_rebuild_120s_noRebuild() {
		log.info("start function test case 05");
		int waitcnt = 0;

		// start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

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
		log.info("Write data over!");
		if (waitcnt > 150)
			fail("put data time out!");
		waitcnt = 0;

		// verify get result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);

		// close ds
		if (!control_ds(dsList.get(0), stop, 0))
			fail("close ds failed!");
		log.info("ds has been closed!");
		log.info("wait 5 seconds to restart before rebuild ...");

		waitto(5);

		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != 0)
			fail("Already migration!");
		// restart ds
		if (!control_ds(dsList.get(0), start, 0))
			fail("restart ds failed!");
		log.info("Restart ds successful!");

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
				/ put_count_float > 0.79);
		log.info("Successfully Verified data!");

		// wait downtime
		waitto(down_time);

		// verify no migration
		if (check_keyword(csList.get(1), start_migrate, tair_bin
				+ "logs/config.log") != 0)
			fail("Already migration!");

		// end test
		log.info("end function test case 05");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		clean_tool(local);
		resetCluster(csList, dsList);
		// execute_shift_tool(local, "conf5");// for kdb
		batch_uncomment(csList, tair_bin + groupconf, dsList, "#");
		if (!batch_modify(csList, tair_bin + groupconf, copycount, "1"))
			fail("modify configure file failure");
		if (!batch_modify(dsList, tair_bin + groupconf, copycount, "1"))
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
