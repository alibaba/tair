package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailOverDetail extends FailOverBaseCase {

	public void changeToolConf(String machine, String path, String key,
			String value) {
		if (!modify_config_file(machine, path + "TairTool.conf", key, value))
			fail("modify configure file failure");
	}

	public void waitForFinish(String machine) {
		int waitcnt = 0;
		while (check_process(machine, "TairTool") != 2) {
			log.debug("wait for TairTool finish!");
			waitto(9);
			if (++waitcnt > 10)
				break;
		}
		if (waitcnt > 10)
			assertTrue("wait for TairTool finish timeout!", false);
	}

	@Test
	public void test_Failover_01_get_from_other_ds() {
		log.info("start detail failover test case 01");

		if (!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.info("Start Cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "get");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		// change kv file
		if (!scp_file(dsList.get(1), test_bin, "read.kv", dsList.get(2),
				test_bin))
			fail("scp kv file from " + dsList.get(1) + " to " + dsList.get(2)
					+ " failed!");
		if (!scp_file(dsList.get(0), test_bin, "read.kv", dsList.get(1),
				test_bin))
			fail("scp kv file from " + dsList.get(0) + " to " + dsList.get(1)
					+ " failed!");

		// get other data
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue("get other data successful!",
				getVerifySuccessful(dsList.get(1), test_bin) == 0);
		assertTrue("get other data successful!",
				getVerifySuccessful(dsList.get(2), test_bin) == 0);
		log.info("Successfully verified other data!");

		log.info("end detail failover test case 01");
	}

	@Test
	public void test_Failover_02_add_one_ds() {
		log.info("start detail failover test case 02");

		if (!comment_line((String) csList.get(0), FailOverBaseCase.tair_bin
				+ "etc/group.conf", (String) dsList.get(0), "#"))
			fail("change group.conf failed!");
		log.info("comment one ds successful!");
		if (!control_cluster(csList, dsList.subList(1, dsList.size()),
				FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.info("Start Cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);

		// add one new ds while putting data
		if (!control_ds(dsList.get(0), FailOverBaseCase.start, 0))
			fail("start the last ds failed!");
		log.info("start the last ds successful!");
		waitto(FailOverBaseCase.down_time);
		if (!uncomment_line((String) csList.get(0), FailOverBaseCase.tair_bin
				+ "etc/group.conf", (String) dsList.get(0), "#"))
			fail("change group.conf failed!");
		log.info("uncomment the last ds successful!");
		execute_tair_tool(dsList.get(0), test_bin);

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "get");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		log.info("end detail failover test case 02");
	}

	@Test
	public void test_Failover_03_restart_one_ds() {
		log.info("start detail failover test case 03");

		if (!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.info("Start Cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		waitto(10);

		// stop one ds while getting data
		if (!control_ds(dsList.get(dsList.size() - 1), FailOverBaseCase.stop, 1))
			fail("kill one ds failed!");
		log.info("kill one ds successful!");

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		// restart the killed ds
		if (!control_ds(dsList.get(dsList.size() - 1), FailOverBaseCase.start,
				0))
			fail("restart the killed ds failed!");
		log.info("restart the killed ds successful!");

		batch_clean_test_data(dsList, test_bin);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		log.info("end detail failover test case 03");
	}

	@Test
	public void test_Failover_04_restart_all_ds() {
		log.info("start detail failover test case 04");

		if (!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.info("Start Cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// stop all ds
		if (!batch_control_ds(dsList, FailOverBaseCase.stop, 1))
			fail("kill all ds failed!");
		log.info("kill all ds successful!");
		waitto(FailOverBaseCase.ds_down_time);

		// restart all ds
		if (!batch_control_ds(dsList, FailOverBaseCase.start, 0))
			fail("restart all ds failed!");
		log.info("restart all ds successful!");
		waitto(FailOverBaseCase.down_time);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "get");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		log.info("end detail failover test case 04");
	}

	@Test
	public void test_Failover_05_no_cs() {
		log.info("start detail failover test case 05");

		if (!batch_control_ds(dsList, FailOverBaseCase.start, 0))
			fail("start ds cluster failed!");
		log.info("Start ds cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "get");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		// start cs list
		if (!batch_control_cs(csList, FailOverBaseCase.start, 0))
			fail("start cs cluster failed!");
		log.info("Start cs cluster Successful!");

		// get data again
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		log.info("end detail failover test case 05");
	}

	@Test
	public void test_Failover_06_add_one_ds_while_open_datamove() {
		log.info("start detail failover test case 06");

		if (!comment_line((String) csList.get(0), FailOverBaseCase.tair_bin
				+ "etc/group.conf", (String) dsList.get(0), "#"))
			fail("change group.conf failed!");
		log.info("comment one ds successful!");
		if (!modify_config_file(csList.get(0), FailOverBaseCase.tair_bin
				+ "etc/group.conf", "_data_move", "1"))
			fail("change datamove to 1 failed!");
		log.info("change datamove to 1 successful!");

		if (!control_cluster(csList, dsList.subList(1, dsList.size()),
				FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.info("Start Cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);

		// add one new ds while putting data
		if (!control_ds(dsList.get(0), FailOverBaseCase.start, 0))
			fail("start the last ds failed!");
		log.info("start the last ds successful!");
		waitto(FailOverBaseCase.down_time);
		if (!uncomment_line((String) csList.get(0), FailOverBaseCase.tair_bin
				+ "etc/group.conf", (String) dsList.get(0), "#"))
			fail("change group.conf failed!");
		log.info("uncomment the last ds successful!");
		execute_tair_tool(dsList.get(0), test_bin);

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "get");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		log.info("Successfully verified local data!");

		log.info("end detail failover test case 06");
	}

	@Test
	public void test_Failover_07_restart_master_cs() {
		log.info("start detail failover test case 07");

		if (!control_cluster(csList, dsList, FailOverBaseCase.start, 0))
			fail("start cluster failed!");
		log.info("Start Cluster Successful!");
		log.debug("wait system initialize ...");
		waitto(FailOverBaseCase.down_time);

		// put data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		log.info("put data over!");
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"put successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);

		// get local data
		changeToolConf(dsList.get(0), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(1), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(2), test_bin, "actiontype", "get");
		changeToolConf(dsList.get(0), test_bin2, "actiontype", "put");
		changeToolConf(dsList.get(1), test_bin2, "actiontype", "put");
		changeToolConf(dsList.get(2), test_bin2, "actiontype", "put");
		execute_tair_tool(dsList.get(0), test_bin);
		execute_tair_tool(dsList.get(1), test_bin);
		execute_tair_tool(dsList.get(2), test_bin);
		execute_tair_tool(dsList.get(0), test_bin2);
		execute_tair_tool(dsList.get(1), test_bin2);
		execute_tair_tool(dsList.get(2), test_bin2);

		// stop master cs
		if (!control_cs(csList.get(0), FailOverBaseCase.stop, 1))
			fail("stop master cs failed!");
		log.info("Stop master cs Successful!");
		waitto(down_time);
		if (check_keyword(csList.get(1),
				"MASTER_CONFIG changed " + csList.get(1),
				FailOverBaseCase.tair_bin + "logs/config.log") != 1)
			fail("check master cs not changed!");
		log.info("check master cs changed to " + csList.get(1) + " !");

		int csCount = check_keyword(csList.get(1), "MASTER_CONFIG changed "
				+ csList.get(0), FailOverBaseCase.tair_bin + "logs/config.log");
		// restart master cs
		if (!control_cs(csList.get(0), FailOverBaseCase.start, 0))
			fail("restart master cs failed!");
		log.info("Restart master cs Successful!");
		waitto(down_time);
		if (check_keyword(csList.get(1),
				"MASTER_CONFIG changed " + csList.get(0),
				FailOverBaseCase.tair_bin + "logs/config.log") != csCount + 1)
			fail("check master cs not changed!");
		log.info("check master cs changed to " + csList.get(0) + " !");

		waitForFinish(dsList.get(0));
		waitForFinish(dsList.get(1));
		waitForFinish(dsList.get(2));
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(0), test_bin2) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(1), test_bin2) / put_count_float == 1);
		assertTrue(
				"get successful rate smaller than 100%!",
				getVerifySuccessful(dsList.get(2), test_bin2) / put_count_float == 1);
		log.info("Successfully verified local data!");

		log.info("end detail failover test case 07");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		batch_clean_test_data(dsList, test_bin);
		batch_clean_test_data(dsList, test_bin2);
		reset_cluster(csList, dsList);
		batch_uncomment(csList, FailOverBaseCase.tair_bin + "etc/group.conf",
				dsList, "#");
		if (!batch_modify(csList, FailOverBaseCase.tair_bin + "etc/group.conf",
				"_copy_count", "1"))
			fail("modify configure file failure");
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
		batch_clean_test_data(dsList, test_bin);
		batch_clean_test_data(dsList, test_bin2);
		reset_cluster(csList, dsList);
		batch_uncomment(csList, FailOverBaseCase.tair_bin + "etc/group.conf",
				dsList, "#");
		if (!modify_config_file(csList.get(0), FailOverBaseCase.tair_bin
				+ "etc/group.conf", "_data_move", "0"))
			fail("reset datamove to 0 failed!");
	}
}
