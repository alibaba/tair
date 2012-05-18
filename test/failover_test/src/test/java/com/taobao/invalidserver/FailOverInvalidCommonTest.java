package com.taobao.invalidserver;

import org.junit.Test;
import com.ibm.staf.STAFResult;

public class FailOverInvalidCommonTest extends
		FailOverBaseCase {

	public void startCluster() {
		// start tair cluster
		if (!control_cluster(csList1, dsList1, start, 0))
			fail("start cluster A failure!");
		if (!control_cluster(csList2, dsList2, start, 0))
			fail("start cluster B failure!");
		log.error("start cluster A & B successful!");
		waitto(5);

		// start invalid server cluster
		if (!control_iv(ivList.get(0), start, 0))
			fail("start invalid server 1 failure!");
		if (!control_iv(ivList.get(1), start, 0))
			fail("start invalid server 2 failure!");
		log.error("Start Invalid Server Cluster Successful!");
		waitto(5);
	}

	public void changeToolConf(String machine, String path, String value) {
		// change test tool's configuration
		if (!modify_config_file(machine, path + "TairTool.conf", "actiontype",
				value))
			fail("modify configure file failure");
		if (!modify_config_file(machine, path + "TairTool.conf", "datasize",
				FailOverBaseCase.put_count))
			fail("modify configure file failure");
	}

	public void changeToolConf(String machine, String path, String key, String value) {
		if (!modify_config_file(machine, path + "TairTool.conf", key, value))
			fail("modify configure file failure");
	}

	public void waitForFinish(String machine, String path, int expect) {
		int waitcnt = 0;
		int flag = 1;
		while (!checkIfFinish(machine, path, expect)) {
			log.debug("wait for TairTool finish!");
			waitto(30);
			if (++waitcnt > 15)
				break;
		}
		if (waitcnt > 15)
			flag = 0;

		String cmd = "kill -9 `ps -ef|grep TairTool |grep -v grep|awk  \'{print $2}\'`";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			assertTrue("can't kill TairTool!", false);
		else if (flag == 0)
			assertTrue("wait for TairTool finish timeout!", false);
	}

	public boolean waitForAllFinish(String machine, int expect1, int expect2) {
		int waitcnt = 0;
		while (!checkIfFinish(machine, test_bin, expect1)
				|| !checkIfFinish(machine, test_bin, expect2)) {
			log.debug("wait for TairTool finish!");
			waitto(30);
			if (++waitcnt > 15)
				break;
		}
		if (waitcnt > 15) {
			log.error("wait for TairTool finish timeout!");
			return false;
		}

		String cmd = "kill -9 `ps -ef|grep TairTool |grep -v grep|awk  \'{print $2}\'`";
		STAFResult result = executeShell(stafhandle, "local", cmd);
		if (result.rc != 0)
			return false;
		else
			return true;
	}

	public boolean checkIfFinish(String machine, String path, int expect) {
		boolean ret = false;
		String cmd = "cd " + path + " && ";
		cmd += "grep \"Successful\" datadbg0.log | wc -l";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			String stdout = getShellOutput(result);
			try {
				int temp = (new Integer(stdout.trim())).intValue();
				if (temp == expect)
					ret = true;
				else
					ret = false;
				log.error("expect=" + expect + " actual=" + temp);
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
				ret = false;
			}
		}

		return ret;
	}

	protected void Failover_01_restart_one_invalid_server(String cmd,
			String async) {
		startCluster();

		int waitcnt = 0;
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(5);

		// close 1 invalid server
		if (!control_iv((String) ivList.get(0), FailOverBaseCase.stop, 1))
			fail("close invalid server 1 failure!");
		log.error("invalid server 1 has been closed!");
		waitto(FailOverBaseCase.down_time);
		// check if client know invalid server 1 has been killed
		while (check_keyword(clList.get(0), "invalid server " + ".*" + "is down",
				FailOverBaseCase.test_bin + "datadbg0.log") != 1) {
			log.error("waiting rebuild invalid server list!");
			waitto(2);
			if (++waitcnt > 5)
				break;
		}
		if (waitcnt > 5)
			fail("invalid server 1 has been down, but client did't know!");
		waitcnt = 0;

		waitto(5);

		// restart the killed invalid server
		if (!control_iv((String) ivList.get(0), FailOverBaseCase.start, 0))
			fail("restart the killed invalid server failure!");
		log.error("invalid server 1 has been restarted!");
		waitto(FailOverBaseCase.down_time);
		// check if client know invalid server 1 has been restarted
		while (check_keyword(clList.get(0), "invalid server " + ".*" + " revoked",
				FailOverBaseCase.test_bin + "datadbg0.log") != 1) {
			log.error("waiting rebuild invalid server list!");
			waitto(2);
			if (++waitcnt > 5)
				break;
		}
		if (waitcnt > 5)
			fail("invalid server 1 has been restarted, but client did't know!");
		waitcnt = 0;

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("invalid successful rate smaller than 90%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float > 0.9);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) < 100);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) < 100);
		log.error("Successfully Verified data!");
	}

	protected void Failover_02_restart_all_invalid_server(String cmd,
			String async) {
		startCluster();

		int waitcnt = 0;
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(5);

		// close all invalid server
		if (!control_iv((String) ivList.get(0), FailOverBaseCase.stop, 1))
			fail("close invalid server 1 failure!");
		if (!control_iv((String) ivList.get(1), FailOverBaseCase.stop, 1))
			fail("close invalid server 1 failure!");
		log.error("all invalid server has been closed!");
		// waitto(FailOverBaseCase.down_time);
		// check if client know all invalid server has been killed
		while (check_keyword(clList.get(0), "invalid server " + ".*" + "is down",
				test_bin + "datadbg0.log") != 2) {
			log.error("waiting rebuild invalid server list!");
			waitto(2);
			if (++waitcnt > 5)
				break;
		}
		if (waitcnt > 5)
			fail("all invalid server has been down, but client did't know!");
		waitcnt = 0;

		// waitto(5);

		// restart the killed invalid server
		if (!control_iv((String) ivList.get(0), FailOverBaseCase.start, 0))
			fail("restart the killed invalid server 1 failure!");
		if (!control_iv((String) ivList.get(1), FailOverBaseCase.start, 0))
			fail("restart the killed invalid server 2 failure!");
		log.error("all invalid server has been restarted!");
		waitto(FailOverBaseCase.down_time);
		// check if client know all invalid server has been restarted
		while (check_keyword(clList.get(0), "invalid server " + ".*" + " revoked",
				test_bin + "datadbg0.log") == 0) {
			log.error("waiting rebuild invalid server list!");
			waitto(2);
			if (++waitcnt > 5)
				break;
		}
		if (waitcnt > 5)
			fail("all invalid server has been restarted, but client did't know!");
		waitcnt = 0;

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("invalid successful rate smaller than 90%!",
				getVerifySuccessful(clList.get(0), test_bin) == put_count_float -20);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) < 2000);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) < 30000);
		log.error("Successfully Verified data!");
	}

	protected void Failover_03_add_one_invalid_server(String cmd, String async) {
		startCluster();

		int waitcnt = 0;
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		changeToolConf(clList.get(0), test_bin, "start", "100000");
		changeToolConf(clList.get(1), test_bin, "start", "100000");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

        int suc1 = getVerifySuccessful(clList.get(0), test_bin);
        int suc2 = getVerifySuccessful(clList.get(1), test_bin);
		assertTrue("put successful rate smaller than 100%!", suc1 / put_count_float > 0.99);
		assertTrue("put successful rate smaller than 100%!", suc2 / put_count_float > 0.99);

		// change test tool's configuration
//		changeToolConf(test_bin, "actiontype", cmd);
//		changeToolConf(test_bin, "async", async);
		changeToolConf(clList.get(0), test_bin, "start", "0");
		changeToolConf(clList.get(1), test_bin, "start", "0");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitto(5);

		int inval_count = check_keyword(clList.get(0), "got invalid server " + ".*",
				FailOverBaseCase.test_bin + "datadbg0.log");

		// start 1 new invalid server
		if (!control_iv((String) ivList.get(2), FailOverBaseCase.start, 0))
			fail("start 1 new invalid server failure!");
		log.error("invalid server 3 has been started!");
		waitto(5);

		if (!modify_config_file(csList1.get(0), tair_bin + "etc/group.conf",
				"invalidate_server",
				"10.232.4.20:5196,10.232.4.21:5196,10.232.4.22:5196"))
			fail("modify configure file failure");
		if (!modify_config_file(csList2.get(0), tair_bin + "etc/group.conf",
				"invalidate_server",
				"10.232.4.20:5196,10.232.4.21:5196,10.232.4.22:5196"))
			fail("modify configure file failure");
		touch_file(csList1.get(0), tair_bin + "etc/group.conf");
		touch_file(csList2.get(0), tair_bin + "etc/group.conf");

		// check if client know invalid server 1 has been restarted
		while (check_keyword(clList.get(0), "got invalid server " + ".*",
				FailOverBaseCase.test_bin + "datadbg0.log") != inval_count + 3) {
			log.error("waiting rebuild invalid server list!");
			waitto(2);
			if (++waitcnt > 5)
				break;
		}
		if (waitcnt > 5)
			fail("invalid server 3 has been started, but client did't know!");
		waitcnt = 0;

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		waitForFinish(clList.get(1), test_bin, 2);
        int suc3 = getVerifySuccessful(clList.get(0), test_bin);
        int suc4 = getVerifySuccessful(clList.get(1), test_bin);
		assertTrue("put successful rate smaller than 100%!", suc3 / put_count_float > 0.99);
		assertTrue("put successful rate smaller than 100%!", suc4 / put_count_float > 0.99);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 3);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == suc1 + suc3);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == suc2 + suc4);
		log.error("Successfully Verified data!");
	}

	protected void Failover_04_add_two_invalid_server(String cmd, String async) {
		if (!modify_config_file(csList1.get(0), tair_bin + "etc/group.conf",
				"invalidate_server", "10.232.4.20:5196"))
			fail("modify configure file failure");
		if (!modify_config_file(csList2.get(0), tair_bin + "etc/group.conf",
				"invalidate_server", "10.232.4.20:5196"))
			fail("modify configure file failure");
		startCluster();

		int waitcnt = 0;
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		changeToolConf(clList.get(0), test_bin, "start", "100000");
		changeToolConf(clList.get(1), test_bin, "start", "100000");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

        int suc1 = getVerifySuccessful(clList.get(0), test_bin);
        int suc2 = getVerifySuccessful(clList.get(1), test_bin);
		assertTrue("put successful rate smaller than 100%!", suc1 / put_count_float > 0.99);
		assertTrue("put successful rate smaller than 100%!", suc2 / put_count_float > 0.99);

		// change test tool's configuration
//		changeToolConf(test_bin, "actiontype", cmd);
//		changeToolConf(test_bin, "async", async);
		changeToolConf(clList.get(0), test_bin, "start", "0");
		changeToolConf(clList.get(1), test_bin, "start", "0");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitto(5);

		int inval_count = check_keyword(clList.get(0), "got invalid server " + ".*",
				FailOverBaseCase.test_bin + "datadbg0.log");

		// start the second new invalid server
		if (!control_iv((String) ivList.get(2), FailOverBaseCase.start, 0))
			fail("start 1 new invalid server failure!");
		log.error("2 invalid server has been started!");
		waitto(5);

		if (!modify_config_file(csList1.get(0), tair_bin + "etc/group.conf",
				"invalidate_server",
				"10.232.4.20:5196,10.232.4.21:5196,10.232.4.22:5196"))
			fail("modify configure file failure");
		if (!modify_config_file(csList2.get(0), tair_bin + "etc/group.conf",
				"invalidate_server",
				"10.232.4.20:5196,10.232.4.21:5196,10.232.4.22:5196"))
			fail("modify configure file failure");
		touch_file(csList1.get(0), tair_bin + "etc/group.conf");
		touch_file(csList2.get(0), tair_bin + "etc/group.conf");

		// check if client know invalid server 1 has been restarted
		while (check_keyword(clList.get(0), "got invalid server " + ".*",
				FailOverBaseCase.test_bin + "datadbg0.log") != inval_count + 3) {
			log.error("waiting rebuild invalid server list!");
			waitto(2);
			if (++waitcnt > 5)
				break;
		}
		if (waitcnt > 5)
			fail("2 invalid server has been started, but client did't know!");
		waitcnt = 0;

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		waitForFinish(clList.get(1), test_bin, 2);
        int suc3 = getVerifySuccessful(clList.get(0), test_bin);
        int suc4 = getVerifySuccessful(clList.get(1), test_bin);
		assertTrue("put successful rate smaller than 100%!", suc3 / put_count_float > 0.99);
		assertTrue("put successful rate smaller than 100%!", suc4 / put_count_float > 0.99);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 3);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == suc1 + suc3);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == suc2 + suc4);
		log.error("Successfully Verified data!");
	}

	protected void Failover_05_shutoff_net_between_client_and_one_invalid_server(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(5);

		// shut off net between client and 1 invalid server
		if (!shutoff_net(clList.get(0), ivList.get(0)))
			fail("shutoff net between client and invalid server 1 failure!");
		log.error("shutoff net between client and invalid server 1 success!");

		// ///////////////////////////////////////////return
		// ResultCode.CONNERROR

		waitto(30);

		// recover net between client and the invalid server
		if (!recover_net(clList.get(0)))
			fail("recover net between client and the invalid server failure!");
		log.error("recover net between client and the invalid server success!");

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("invalid successful rate smaller than 90%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float > 0.9);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) < 100);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) < 100);
		log.error("Successfully Verified data!");
	}

	protected void Failover_06_shutoff_net_between_client_and_all_invalid_server(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(5);

		// shut off net between client and all invalid server
		if (!shutoff_net(clList.get(0), ivList.get(0)))
			fail("shutoff net between client and invalid server 1 failure!");
		if (!shutoff_net(clList.get(0), ivList.get(1)))
			fail("shutoff net between client and invalid server 2 failure!");
		log.error("shutoff net between client and all invalid server success!");

		// ///////////////////////////////////////////return
		// ResultCode.CONNERROR

		waitto(30);

		// recover net between client and the invalid server
		if (!recover_net(clList.get(0)))
			fail("recover net between client and all invalid server failure!");
		log.error("recover net between client and all invalid server success!");

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("invalid successful rate smaller than 90%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float > 0.9);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) < 100);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) < 100);
		log.error("Successfully Verified data!");
	}

	protected void Failover_07_shutoff_net_between_one_invalid_server_and_one_cluster(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		if ("1".equals(async)) {
			changeToolConf(clList.get(0), test_bin, "datasize", "22000");
			changeToolConf(clList.get(1), test_bin, "datasize", "22000");
		} else {
			changeToolConf(clList.get(0), test_bin, "datasize", "500");
			changeToolConf(clList.get(1), test_bin, "datasize", "500");
		}
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		if ("1".equals(async)) {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 22000 == 1);
		} else {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 500 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 500 == 1);
		}

		// shut off net between 1 iv and 1 cluster
		for (int i = 0; i < dsList1.size(); i++) {
			if (!shutoff_net(ivList.get(0), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList1.get(i) + " failure!");
		}
		log.error("shutoff net between iv 1 and 1 cluster success!");

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) / 22000.0 > 0.9);
			//grep if QUEUE_OVERFLOWED detected
			if(check_keyword(clList.get(0), "3968", test_bin + "datadbg0.log") == 0)
				fail("QUEUE_OVERFLOWED not detected!");
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 500 == 1);
		}

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "actiontype", "get");
		changeToolConf(clList.get(1), test_bin, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		if ("1".equals(async)) {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000.0 < 0.6);
			int suc_old = getVerifySuccessful(clList.get(1), test_bin);
            execute_tair_tool(clList.get(1), test_bin);
            waitForFinish(clList.get(1), test_bin, 3);
            int suc_new = getVerifySuccessful(clList.get(1), test_bin);
            assertTrue("get count didn't changed!", suc_new < suc_old);
			log.error("Successfully Verified data!");
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 500.0 == 0.5);
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(1), test_bin) == 0);
			log.error("Successfully Verified data!");
		}

		// recover net
		// recover net between client and the invalid server
		if (!recover_net(ivList.get(0)))
			fail("recover net between iv 1 and cluster 1 failure!");
		log.error("recover net between iv 1 and cluster 1 success!");

		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		// reput
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);
		// reinvalid
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		// reget
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == 0);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == 0);
		log.error("Successfully Verified data!");
	}

	protected void Failover_08_shutoff_net_between_one_invalid_server_and_all_cluster(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		if("1".equals(async)) {
			changeToolConf(clList.get(0), test_bin, "datasize", "15000");
			changeToolConf(clList.get(1), test_bin, "datasize", "15000");
		} else {
			changeToolConf(clList.get(0), test_bin, "datasize", "250");
			changeToolConf(clList.get(1), test_bin, "datasize", "250");
		}
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		if("1".equals(async)) {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 15000 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 15000 == 1);
		} else {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 250 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 250 == 1);
		}

		// shut off net between 1 iv and all cluster
		for (int i = 0; i < dsList1.size(); i++) {
			if (!shutoff_net(ivList.get(1), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList1.get(i) + " failure!");
			if (!shutoff_net(ivList.get(1), dsList2.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList2.get(i) + " failure!");
		}
		log.error("shutoff net between iv 1 and all cluster success!");

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 15000.0 > 0.9);
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) == 250);
		}

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "actiontype", "get");
		changeToolConf(clList.get(1), test_bin, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 15000.0 == 0.5);
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(1), test_bin) / 15000.0 == 0.5);
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) == 125);
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(1), test_bin) == 125);
		}
		
		log.error("Successfully Verified data!");

		// recover net
		// recover net between client and the invalid server
		if (!recover_net(ivList.get(1)))
			fail("recover net between iv 1 and all cluster failure!");
		log.error("recover net between iv 1 and all cluster success!");

		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		// reput
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);
		// reinvalid
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		// reget
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == 0);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == 0);
		log.error("Successfully Verified data!");
	}

	protected void Failover_09_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		if("1".equals(async)) {
			changeToolConf(clList.get(0), test_bin, "datasize", "22000");
			changeToolConf(clList.get(1), test_bin, "datasize", "22000");
		} else {
			changeToolConf(clList.get(0), test_bin, "datasize", "250");
			changeToolConf(clList.get(1), test_bin, "datasize", "250");
		}
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		if("1".equals(async)) {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 22000 == 1);
		} else {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 250 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 250 == 1);
		}

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);

		waitto(5);

		// shut off net between 1 iv and all cluster
		for (int i = 0; i < dsList1.size(); i++) {
			if (!shutoff_net(ivList.get(0), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList1.get(i) + " failure!");
			if (!shutoff_net(ivList.get(0), dsList2.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList2.get(i) + " failure!");
		}
		log.error("shutoff net between iv 1 and all cluster success!");

		// recover net between client and the invalid server
		if (!recover_net(ivList.get(0)))
			fail("recover net between iv 1 and all cluster failure!");
		log.error("recover net between iv 1 and all cluster success!");

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) / 22000 == 1);
//			//grep if QUEUE_OVERFLOWED detected
//			if(check_keyword(clList.get(), "3968", test_bin + "datadbg0.log") == 0)
//				fail("QUEUE_OVERFLOWED not detected!");
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 250 == 1);
		}

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "actiontype", "get");
		changeToolConf(clList.get(1), test_bin, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == 0);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == 0);
		log.error("Successfully Verified data!");
	}

	protected void Failover_10_shutoff_net_between_all_invalid_server_and_one_cluster(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		if("1".equals(async)) {
			changeToolConf(clList.get(0), test_bin, "datasize", "22000");
			changeToolConf(clList.get(1), test_bin, "datasize", "22000");
		} else {
			changeToolConf(clList.get(0), test_bin, "datasize", "250");
			changeToolConf(clList.get(1), test_bin, "datasize", "250");
		}
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		if("1".equals(async)) {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 22000 == 1);
		} else {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 250 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 250 == 1);
		}

		// shut off net between all iv and 1 cluster
		for (int i = 0; i < dsList1.size(); i++) {
			if (!shutoff_net(ivList.get(0), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList1.get(i) + " failure!");
			if (!shutoff_net(ivList.get(1), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(1) + " and server:"
						+ dsList1.get(i) + " failure!");
		}
		log.error("shutoff net between all iv and 1 cluster success!");

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000.0 > 0.9);
			//grep if QUEUE_OVERFLOWED detected
			if(check_keyword(clList.get(0), "3968", test_bin + "datadbg0.log") == 0)
				fail("QUEUE_OVERFLOWED not detected!");
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 250 == 1);
		}

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "actiontype", "get");
		changeToolConf(clList.get(1), test_bin, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000 == 1);
            int suc_old = getVerifySuccessful(clList.get(1), test_bin);
            execute_tair_tool(clList.get(1), test_bin);
            waitForFinish(clList.get(1), test_bin, 3);
            int suc_new = getVerifySuccessful(clList.get(1), test_bin);
			assertTrue("get count didn't changed!", suc_new < suc_old);
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) / 250 == 1);
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(1), test_bin) == 0);
		}
		log.error("Successfully Verified data!");

		// recover net
		// recover net between all iv and the cluster 1
		if (!recover_net(ivList.get(0)))
			fail("recover net between iv 1 and cluster 1 failure!");
		if (!recover_net(ivList.get(1)))
			fail("recover net between iv 2 and cluster 1 failure!");
		log.error("recover net between all iv and cluster 1 success!");

		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		// reput
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);
		// reinvalid
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		// reget
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == 0);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == 0);
		log.error("Successfully Verified data!");
	}

	protected void Failover_11_shutoff_net_between_all_invalid_server_and_all_cluster(
			String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		if("1".equals(async)) {
			changeToolConf(clList.get(0), test_bin, "datasize", "22000");
			changeToolConf(clList.get(1), test_bin, "datasize", "22000");
		} else {
			changeToolConf(clList.get(0), test_bin, "datasize", "50");
			changeToolConf(clList.get(1), test_bin, "datasize", "50");
		}
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		if("1".equals(async)) {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 22000 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 22000 == 1);
		} else {
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(0), test_bin) / 50 == 1);
			assertTrue("put successful rate smaller than 100%!",
					getVerifySuccessful(clList.get(1), test_bin) / 50 == 1);
		}

		// shut off net between all iv and all cluster
		for (int i = 0; i < dsList1.size(); i++) {
			if (!shutoff_net(ivList.get(0), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList1.get(i) + " failure!");
			if (!shutoff_net(ivList.get(0), dsList2.get(i)))
				fail("shutoff net between iv:" + ivList.get(0) + " and server:"
						+ dsList2.get(i) + " failure!");
			if (!shutoff_net(ivList.get(1), dsList1.get(i)))
				fail("shutoff net between iv:" + ivList.get(1) + " and server:"
						+ dsList1.get(i) + " failure!");
			if (!shutoff_net(ivList.get(1), dsList2.get(i)))
				fail("shutoff net between iv:" + ivList.get(1) + " and server:"
						+ dsList2.get(i) + " failure!");
		}
		log.error("shutoff net between all iv and all cluster success!");

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		if("1".equals(async)) {
			getVerifySuccessful(clList.get(0), test_bin);
			//grep if QUEUE_OVERFLOWED detected
			if(check_keyword(clList.get(0), "3968", test_bin + "datadbg0.log") == 0)
				fail("QUEUE_OVERFLOWED not detected!");
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) == 50);
		}

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "actiontype", "get");
		changeToolConf(clList.get(1), test_bin, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		if("1".equals(async)) {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) == 22000);
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(1), test_bin) == 22000);
		} else {
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(0), test_bin) == 50);
			assertTrue("get data verify failure!",
					getVerifySuccessful(clList.get(1), test_bin) == 50);
		}
		log.error("Successfully Verified data!");

		// recover net
		// recover net between all iv and the cluster 1
		if (!recover_net(ivList.get(0)))
			fail("recover net between iv 1 and all cluster failure!");
		if (!recover_net(ivList.get(1)))
			fail("recover net between iv 2 and all cluster failure!");
		log.error("recover net between all iv and all cluster success!");

		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		// reput
		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);
		// reinvalid
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		// reget
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == 0);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == 0);
		log.error("Successfully Verified data!");
	}
	
	protected void Failover_12_normal(String cmd, String async) {
		startCluster();

		changeToolConf(clList.get(0), test_bin, "put");
		changeToolConf(clList.get(1), test_bin, "put");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 1);
		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");

		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);
		assertTrue("put successful rate smaller than 100%!",
				getVerifySuccessful(clList.get(1), test_bin) / put_count_float == 1);

		// change test tool's configuration
		changeToolConf(clList.get(0), test_bin, "actiontype", cmd);
		changeToolConf(clList.get(0), test_bin, "async", async);
		execute_tair_tool(clList.get(0), test_bin);

		// wait for invalid finish
		waitForFinish(clList.get(0), test_bin, 2);
		assertTrue("invalid successful rate smaller than 90%!",
				getVerifySuccessful(clList.get(0), test_bin) / put_count_float == 1);

		// change test tool's configuration to read
		changeToolConf(clList.get(0), test_bin, "get");
		changeToolConf(clList.get(1), test_bin, "get");
		execute_tair_tool(clList.get(0), test_bin);
		execute_tair_tool(clList.get(1), test_bin);
		waitForFinish(clList.get(0), test_bin, 3);
		waitForFinish(clList.get(1), test_bin, 2);

		// verify get result
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(0), test_bin) == 0);
		assertTrue("get data verify failure!",
				getVerifySuccessful(clList.get(1), test_bin) == 0);
		log.error("Successfully Verified data!");
	}

	@Test
	public void testFailover_01_restart_one_invalid_server() {
		log.error("start invalid server test Failover case 01");
		Failover_01_restart_one_invalid_server("invalid", "0");
		log.error("end config test Failover case 01");
	}

	@Test
	public void testFailover_02_restart_all_invalid_server() {
		log.error("start invalid server test Failover case 02");
		Failover_02_restart_all_invalid_server("invalid", "0");
		log.error("end config test Failover case 02");
	}

	@Test
	public void testFailover_03_add_one_invalid_server() {
		log.error("start invalid server test Failover case 03");
		Failover_03_add_one_invalid_server("put", "0");
		log.error("end config test Failover case 03");
	}

	@Test
	public void testFailover_04_add_two_invalid_server() {
		log.error("start invalid server test Failover case 04");
		Failover_04_add_two_invalid_server("put", "0");
		log.error("end config test Failover case 04");
	}

	@Test
	public void testFailover_05_shutoff_net_between_client_and_one_invalid_server() {
		log.error("start invalid server test Failover case 05");
		Failover_05_shutoff_net_between_client_and_one_invalid_server(
				"invalid", "0");
		log.error("end config test Failover case 05");
	}

	@Test
	public void testFailover_06_shutoff_net_between_client_and_all_invalid_server() {
		log.error("start invalid server test Failover case 06");
		Failover_06_shutoff_net_between_client_and_all_invalid_server(
				"invalid", "0");
		log.error("end config test Failover case 06");
	}

	@Test
	public void testFailover_07_shutoff_net_between_one_invalid_server_and_one_cluster() {
		log.error("start invalid server test Failover case 07");
		Failover_07_shutoff_net_between_one_invalid_server_and_one_cluster(
				"invalid", "0");
		log.error("end config test Failover case 07");
	}

	@Test
	public void testFailover_08_shutoff_net_between_one_invalid_server_and_all_cluster() {
		log.error("start invalid server test Failover case 08");
		Failover_08_shutoff_net_between_one_invalid_server_and_all_cluster(
				"invalid", "0");
		log.error("end config test Failover case 08");
	}

	@Test
	public void testFailover_09_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover() {
		log.error("start invalid server test Failover case 09");
		Failover_09_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover(
				"invalid", "0");
		log.error("end config test Failover case 09");
	}

	@Test
	public void testFailover_10_shutoff_net_between_all_invalid_server_and_one_cluster() {
		log.error("start invalid server test Failover case 10");
		Failover_10_shutoff_net_between_all_invalid_server_and_one_cluster(
				"invalid", "0");
		log.error("end config test Failover case 10");
	}

	@Test
	public void testFailover_11_shutoff_net_between_all_invalid_server_and_all_cluster() {
		log.error("start invalid server test Failover case 11");
		Failover_11_shutoff_net_between_all_invalid_server_and_all_cluster(
				"invalid", "0");
		log.error("end config test Failover case 11");
	}

	@Test
	public void testFailover_12_restart_one_invalid_server_sync() {
		log.error("start invalid server test Failover case 12");
		Failover_01_restart_one_invalid_server("hidebyproxy", "0");
		log.error("end config test Failover case 12");
	}

	@Test
	public void testFailover_13_restart_all_invalid_server_sync() {
		log.error("start invalid server test Failover case 13");
		Failover_02_restart_all_invalid_server("hidebyproxy", "0");
		log.error("end config test Failover case 13");
	}

	@Test
	public void testFailover_14_add_one_invalid_server_sync() {
		log.error("start invalid server test Failover case 14");
		Failover_03_add_one_invalid_server("hidebyproxy", "0");
		log.error("end config test Failover case 14");
	}

	@Test
	public void testFailover_15_add_two_invalid_server_sync() {
		log.error("start invalid server test Failover case 15");
		Failover_04_add_two_invalid_server("hidebyproxy", "0");
		log.error("end config test Failover case 15");
	}

	@Test
	public void testFailover_16_shutoff_net_between_client_and_one_invalid_server_sync() {
		log.error("start invalid server test Failover case 16");
		Failover_05_shutoff_net_between_client_and_one_invalid_server(
				"hidebyproxy", "0");
		log.error("end config test Failover case 16");
	}

	@Test
	public void testFailover_17_shutoff_net_between_client_and_all_invalid_server_sync() {
		log.error("start invalid server test Failover case 17");
		Failover_06_shutoff_net_between_client_and_all_invalid_server(
				"hidebyproxy", "0");
		log.error("end config test Failover case 17");
	}

	@Test
	public void testFailover_18_shutoff_net_between_one_invalid_server_and_one_cluster_sync() {
		log.error("start invalid server test Failover case 18");
		Failover_07_shutoff_net_between_one_invalid_server_and_one_cluster(
				"hidebyproxy", "0");
		log.error("end config test Failover case 18");
	}

	@Test
	public void testFailover_19_shutoff_net_between_one_invalid_server_and_all_cluster_sync() {
		log.error("start invalid server test Failover case 19");
		Failover_08_shutoff_net_between_one_invalid_server_and_all_cluster(
				"hidebyproxy", "0");
		log.error("end config test Failover case 19");
	}

	@Test
	public void testFailover_20_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover_sync() {
		log.error("start invalid server test Failover case 20");
		Failover_09_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover(
				"hidebyproxy", "0");
		log.error("end config test Failover case 20");
	}

	@Test
	public void testFailover_21_shutoff_net_between_all_invalid_server_and_one_cluster_sync() {
		log.error("start invalid server test Failover case 21");
		Failover_10_shutoff_net_between_all_invalid_server_and_one_cluster(
				"hidebyproxy", "0");
		log.error("end config test Failover case 21");
	}

	@Test
	public void testFailover_22_shutoff_net_between_all_invalid_server_and_all_cluster_sync() {
		log.error("start invalid server test Failover case 22");
		Failover_11_shutoff_net_between_all_invalid_server_and_all_cluster(
				"hidebyproxy", "0");
		log.error("end config test Failover case 22");
	}

	@Test
	public void testFailover_23_restart_one_invalid_server_async() {
		log.error("start invalid server test Failover case 23");
		Failover_01_restart_one_invalid_server("hidebyproxy", "1");
		log.error("end config test Failover case 23");
	}

	@Test
	public void testFailover_24_restart_all_invalid_server_async() {
		log.error("start invalid server test Failover case 24");
		Failover_02_restart_all_invalid_server("hidebyproxy", "1");
		log.error("end config test Failover case 24");
	}

//	@Test
//	public void testFailover_25_add_one_invalid_server_async() {
//		log.error("start invalid server test Failover case 25");
//		Failover_03_add_one_invalid_server("hidebyproxy", "1");
//		log.error("end config test Failover case 25");
//	}

//	@Test
//	public void testFailover_26_add_two_invalid_server_async() {
//		log.error("start invalid server test Failover case 26");
//		Failover_04_add_two_invalid_server("hidebyproxy", "1");
//		log.error("end config test Failover case 26");
//	}

	@Test
	public void testFailover_27_shutoff_net_between_client_and_one_invalid_server_async() {
		log.error("start invalid server test Failover case 27");
		Failover_05_shutoff_net_between_client_and_one_invalid_server(
				"hidebyproxy", "1");
		log.error("end config test Failover case 27");
	}

	@Test
	public void testFailover_28_shutoff_net_between_client_and_all_invalid_server_async() {
		log.error("start invalid server test Failover case 28");
		Failover_06_shutoff_net_between_client_and_all_invalid_server(
				"hidebyproxy", "1");
		log.error("end config test Failover case 28");
	}

	@Test
	public void testFailover_29_shutoff_net_between_one_invalid_server_and_one_cluster_async() {
		log.error("start invalid server test Failover case 29");
		Failover_07_shutoff_net_between_one_invalid_server_and_one_cluster(
				"hidebyproxy", "1");
		log.error("end config test Failover case 29");
	}

	@Test
	public void testFailover_30_shutoff_net_between_one_invalid_server_and_all_cluster_async() {
		log.error("start invalid server test Failover case 30");
		Failover_08_shutoff_net_between_one_invalid_server_and_all_cluster(
				"hidebyproxy", "1");
		log.error("end config test Failover case 30");
	}

	@Test
	public void testFailover_31_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover_async() {
		log.error("start invalid server test Failover case 31");
		Failover_09_shutoff_net_between_one_invalid_server_and_all_cluster_then_recover(
				"hidebyproxy", "1");
		log.error("end config test Failover case 31");
	}

	@Test
	public void testFailover_32_shutoff_net_between_all_invalid_server_and_one_cluster_async() {
		log.error("start invalid server test Failover case 32");
		Failover_10_shutoff_net_between_all_invalid_server_and_one_cluster(
				"hidebyproxy", "1");
		log.error("end config test Failover case 32");
	}

	@Test
	public void testFailover_33_shutoff_net_between_all_invalid_server_and_all_cluster_async() {
		log.error("start invalid server test Failover case 33");
		Failover_11_shutoff_net_between_all_invalid_server_and_all_cluster(
				"hidebyproxy", "1");
		log.error("end config test Failover case 33");
	}
	
/*	@Test
	public void testFailOver_34_normal_invalid() {
		log.error("start invalid server test Failover case 34");
		Failover_12_normal("invalid", "0");
		log.error("start invalid server test Failover case 34");
	}
	
	@Test
	public void testFailOver_35_normal_hidebyproxy_sync() {
		log.error("start invalid server test Failover case 35");
		Failover_12_normal("hidebyproxy", "0");
		log.error("start invalid server test Failover case 35");
	}
	
	@Test
	public void testFailOver_36_normal_hidebyproxy_async() {
		log.error("start invalid server test Failover case 36");
		Failover_12_normal("hidebyproxy", "1");
		log.error("start invalid server test Failover case 36");
	}*/

	public void setUp() {
		log.error("clean tool and cluster while setUp!");
		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		clean_tool(clList.get(0), test_bin2);
		clean_tool(clList.get(1), test_bin2);
		reset_inval_cluster(ivList);
		reset_cluster(csList1, dsList1);
		reset_cluster(csList2, dsList2);
        batch_recover_net(dsList1);
        batch_recover_net(dsList2);
        batch_recover_net(ivList);
		if (!batch_modify(csList1, FailOverBaseCase.tair_bin
				+ "etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
		if (!batch_modify(csList2, FailOverBaseCase.tair_bin
				+ "etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
	}

	public void tearDown() {
		// clean_tool(clList.get(), test_bin);
		// clean_tool(clList.get(), test_bin);
		// clean_tool(clList.get(0), test_bin2);
		// clean_tool(clList.get(1), test_bin2);
		// reset_inval_cluster(ivList);
		// reset_cluster(csList1, dsList1);
		// reset_cluster(csList2, dsList2);
		recover_net(clList.get(0));
		batch_recover_net(dsList1);
		batch_recover_net(dsList2);
		batch_recover_net(ivList);
		log.error("clean tool and cluster while tearDown!");
		batch_uncomment(csList1, FailOverBaseCase.tair_bin + "etc/group.conf",
				dsList1, "#");
		batch_uncomment(csList2, FailOverBaseCase.tair_bin + "etc/group.conf",
				dsList2, "#");
		if (!modify_config_file(csList1.get(0), tair_bin + "etc/group.conf",
				"invalidate_server", "10.232.4.20:5196,10.232.4.22:5196"))
			fail("modify configure file failure");
		if (!modify_config_file(csList2.get(0), tair_bin + "etc/group.conf",
				"invalidate_server", "10.232.4.20:5196,10.232.4.22:5196"))
			fail("modify configure file failure");
	}
}
