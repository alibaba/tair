/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailoverClusterTest2 extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_all_server() {
		log.info("start cluster test failover case 01");
		int waitcnt = 0;

		// start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("start cluster successful!");
		waitto(down_time);

		// write data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("change conf failed!");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("change conf failed!");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("change conf failed!");

		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data out time!");
		waitcnt = 0;
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("finish put data!");

		// shut down ds and cs
		if (!batch_control_cs(csList, stop, 0))
			fail("stop cs failed!");
		if (!batch_control_ds(dsList, stop, 0))
			fail("stop ds failed!");

		waitto(ds_down_time);

		// restart cluster
		controlCluster(csList, dsList, start, 0);
		log.info("restart cluster successful!");

		waitto(down_time);
		// verify data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("change conf failed!");
		execute_data_verify_tool();
		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data out time!");
		waitcnt = 0;
		log.info("finish get data!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("verify data successful!");

		log.info("end cluster test failover case 01");
	}

	@Test
	public void testFailover_02_restart_all_server_after_join_ds_and_migration() {
		log.info("start cluster test failover case 02");
		int waitcnt = 0;

		if (!comment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		log.info("change group.conf successful!");

		// start cluster
		controlCluster(csList, dsList.subList(0, dsList.size() - 1), start, 0);
		log.info("start cluster successful!");
		waitto(down_time);

		// write data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, put))
			fail("change conf failed!");
		if (!modify_config_file(local, test_bin + toolconf, datasize, put_count))
			fail("change conf failed!");
		if (!modify_config_file(local, test_bin + toolconf, filename, kv_name))
			fail("change conf failed!");

		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("put data out time!");
		waitcnt = 0;
		// save put result
		int datacnt = getVerifySuccessful();
		assertTrue("put successful rate small than normSucRate!", datacnt
				/ put_count_float > normSucRate);
		log.info("finish put data!");

		if (!control_ds(dsList.get(dsList.size() - 1), start, 0))
			fail("start ds failed!");
		log.info("start ds successful!");

		// add ds and migration
		if (!uncomment_line(csList.get(0), tair_bin + groupconf,
				dsList.get(dsList.size() - 1), "#"))
			fail("change group.conf failed!");
		if (touch_flag != 0)
			touch_file(csList.get(0), tair_bin + groupconf);
		waitto(down_time);

		// check migration stat of start
		while (check_keyword(csList.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			waitto(3);
			if (++waitcnt > 200)
				break;
		}
		if (waitcnt > 200)
			fail("check migrate time out!");
		waitcnt = 0;
		log.info("check migrate finished!");

		// shut down ds and cs
		if (!batch_control_cs(csList, stop, 0))
			fail("stop cs failed!");
		if (!batch_control_ds(dsList, stop, 0))
			fail("stop ds failed!");
		log.equals("cluster shut down!");

		waitto(ds_down_time);

		// restart cluster
		controlCluster(csList, dsList, start, 0);
		log.info("restart cluster successful!");

		waitto(down_time);
		// verify data
		if (!modify_config_file(local, test_bin + toolconf, actiontype, get))
			fail("change conf failed!");

		execute_data_verify_tool();

		while (check_process(local, toolname) != 2) {
			waitto(2);
			if (++waitcnt > 150)
				break;
		}
		if (waitcnt > 150)
			fail("get data out time!");
		waitcnt = 0;
		log.info("finish get data!");

		// verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("verify data successful!");

		log.info("end cluster test failover case 02");
	}

	@BeforeClass
	public static void subBeforeClass() {
		log.info("reset copycount while subBeforeClass!");
		FailOverBaseCase fb = new FailOverBaseCase();
		if (!fb.batch_modify(csList, tair_bin + groupconf, copycount, "2"))
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
