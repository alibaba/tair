package com.taobao.invalidserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import com.ibm.staf.STAFResult;
import com.taobao.diamond.domain.ContextResult;
import com.taobao.diamond.domain.DiamondConf;
import com.taobao.diamond.domain.DiamondSDKConf;
import com.taobao.diamond.sdkapi.impl.DiamondSDKManagerImpl;

/**
 * @author ashu.cs
 * 
 */

public class FailOverInvalidDiamondTest2 extends FailOverBaseCase {

	private DiamondSDKManagerImpl diamondSDKManagerImpl;

	public FailOverInvalidDiamondTest2() {
		List<DiamondConf> dialyDiamondConfs = new ArrayList<DiamondConf>();
		DiamondConf diamondConf_1 = new DiamondConf("10.232.22.157", "8080",
				"admin", "admin");
		dialyDiamondConfs.add(diamondConf_1);

		DiamondSDKConf daliyDamondConf = new DiamondSDKConf(dialyDiamondConfs);
		daliyDamondConf.setServerId("daily");

		Map<String, DiamondSDKConf> diamondSDKConfMaps = new HashMap<String, DiamondSDKConf>(
				1);
		diamondSDKConfMaps.put("daily", daliyDamondConf);

		diamondSDKManagerImpl = new DiamondSDKManagerImpl(2000, 2000);
		diamondSDKManagerImpl.setDiamondSDKConfMaps(diamondSDKConfMaps);
		try {
			diamondSDKManagerImpl.init();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// ContextResult response = diamondSDKManagerImpl
		// .pulishFromDefaultServerAfterModified(
		// "com.taobao.tair.testInvalid", "testInvalid-CM3",
		// "content");
		// assertTrue("change diamond failed!", response.isSuccess());
	}

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

	public void changeToolConf(String machine, String path, String key,
			String value) {
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

	@Test
	public void testFailover_01_change_client_a_to_tair_b() {
		log.error("start diamond test Failover case 01");
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(0), test_bin, "datasize", put_count);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(100);
		// ////////////////////////////////////////////////////
		String temp = "", diamond_b = "";
		File file = new File("tair_b.conf");
		assertTrue("tair_b.conf not exists!", file.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				diamond_b += temp;
				diamond_b += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM3",
						diamond_b);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////

		waitForFinish(clList.get(0), test_bin, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(0), test_bin);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.9);

		// change test tool's configuration to read
		if (!scp_file(clList.get(0), test_bin, "read.kv", clList.get(0),
				test_bin2))
			fail("scp file failed!");
		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.17:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.18:5168");
		changeToolConf(clList.get(0), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 1);
		int suc_b = getVerifySuccessful(clList.get(0), test_bin2);

		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.14:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.15:5168");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 2);
		int suc_a = getVerifySuccessful(clList.get(0), test_bin2);

		// verify get result
		assertTrue("get data verify failure!", suc_a + suc_b == put_count);
		log.error("Successfully Verified data!");
		log.error("end diamond test Failover case 01");
	}

	@Test
	public void testFailover_02_change_client_b_to_tair_a() {
		log.error("start diamond test Failover case 02");
		startCluster();

		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "datasize", put_count);
		execute_tair_tool(clList.get(1), test_bin);
		waitto(100);
		// ////////////////////////////////////////////////////
		String temp = "", diamond_a = "";
		File file = new File("tair_a.conf");
		assertTrue("tair_a.conf not exists!", file.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				diamond_a += temp;
				diamond_a += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM4",
						diamond_a);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////

		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(1), test_bin);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.9);

		// change test tool's configuration to read
		if (!scp_file(clList.get(1), test_bin, "read.kv", clList.get(1),
				test_bin2))
			fail("scp file failed!");
		changeToolConf(clList.get(1), test_bin2, "master_cs",
				"10.232.4.17:5168");
		changeToolConf(clList.get(1), test_bin2, "slave_cs", "10.232.4.18:5168");
		changeToolConf(clList.get(1), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(1), test_bin2);
		waitForFinish(clList.get(1), test_bin2, 1);
		int suc_b = getVerifySuccessful(clList.get(1), test_bin2);

		changeToolConf(clList.get(1), test_bin2, "master_cs",
				"10.232.4.14:5168");
		changeToolConf(clList.get(1), test_bin2, "slave_cs", "10.232.4.15:5168");
		execute_tair_tool(clList.get(1), test_bin2);
		waitForFinish(clList.get(1), test_bin2, 2);
		int suc_a = getVerifySuccessful(clList.get(1), test_bin2);

		// verify get result
		assertTrue("get data verify failure!", suc_a + suc_b == put_count);
		log.error("Successfully Verified data!");
		log.error("end diamond test Failover case 02");
	}
	
	@Test
	public void testFailover_03_change_client_a_to_wrong_diamond() {
		log.error("start diamond test Failover case 03");
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(0), test_bin, "datasize", put_count);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(100);
		// ////////////////////////////////////////////////////
		String temp = "", diamond_fail = "";
		File file = new File("tair_fail.conf");
		assertTrue("tair_fail.conf not exists!", file.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				diamond_fail += temp;
				diamond_fail += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM3",
						diamond_fail);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////

		waitForFinish(clList.get(0), test_bin, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(0), test_bin);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.9);

		// change test tool's configuration to read
		if (!scp_file(clList.get(0), test_bin, "read.kv", clList.get(0),
				test_bin2))
			fail("scp file failed!");
		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.17:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.18:5168");
		changeToolConf(clList.get(0), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 1);
		int suc_b = getVerifySuccessful(clList.get(0), test_bin2);

		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.14:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.15:5168");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 2);
		int suc_a = getVerifySuccessful(clList.get(0), test_bin2);

		// verify get result
		assertTrue("get data verify failure!", suc_a + suc_b == put_count);
		log.error("Successfully Verified data!");
		log.error("end diamond test Failover case 03");
	}
	
	@Test
	public void testFailover_04_change_client_a_to_tair_b_by_group() {
		log.error("start diamond test Failover case 04");
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(0), test_bin, "datasize", put_count);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(100);
		// ////////////////////////////////////////////////////
		String temp = "", diamond_a = "";
		File file = new File("tair_group_change.conf");
		assertTrue("tair_a.conf not exists!", file.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				diamond_a += temp;
				diamond_a += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-GROUP",
						diamond_a);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////

		waitForFinish(clList.get(0), test_bin, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(0), test_bin);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.9);

		// change test tool's configuration to read
		if (!scp_file(clList.get(0), test_bin, "read.kv", clList.get(0),
				test_bin2))
			fail("scp file failed!");
		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.17:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.18:5168");
		changeToolConf(clList.get(0), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 1);
		int suc_b = getVerifySuccessful(clList.get(0), test_bin2);

		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.14:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.15:5168");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 2);
		int suc_a = getVerifySuccessful(clList.get(0), test_bin2);

		// verify get result
		assertTrue("get data verify failure!", suc_a + suc_b == put_count);
		log.error("Successfully Verified data!");
		log.error("end diamond test Failover case 04");
	}
	
	@Test
	public void testFailover_05_change_client_b_to_tair_a_by_group() {
		log.error("start diamond test Failover case 05");
		startCluster();

		changeToolConf(clList.get(1), test_bin, "actiontype", "put");
		changeToolConf(clList.get(1), test_bin, "datasize", put_count);
		execute_tair_tool(clList.get(1), test_bin);
		waitto(100);
		// ////////////////////////////////////////////////////
		String temp = "", diamond_a = "";
		File file = new File("tair_group_change.conf");
		assertTrue("tair_a.conf not exists!", file.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				diamond_a += temp;
				diamond_a += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-GROUP",
						diamond_a);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////

		waitForFinish(clList.get(1), test_bin, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(1), test_bin);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.9);

		// change test tool's configuration to read
		if (!scp_file(clList.get(1), test_bin, "read.kv", clList.get(1),
				test_bin2))
			fail("scp file failed!");
		changeToolConf(clList.get(1), test_bin2, "master_cs",
				"10.232.4.17:5168");
		changeToolConf(clList.get(1), test_bin2, "slave_cs", "10.232.4.18:5168");
		changeToolConf(clList.get(1), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(1), test_bin2);
		waitForFinish(clList.get(1), test_bin2, 1);
		int suc_b = getVerifySuccessful(clList.get(1), test_bin2);

		changeToolConf(clList.get(1), test_bin2, "master_cs",
				"10.232.4.14:5168");
		changeToolConf(clList.get(1), test_bin2, "slave_cs", "10.232.4.15:5168");
		execute_tair_tool(clList.get(1), test_bin2);
		waitForFinish(clList.get(1), test_bin2, 2);
		int suc_a = getVerifySuccessful(clList.get(1), test_bin2);

		// verify get result
		assertTrue("get data verify failure!", suc_a + suc_b == put_count);
		log.error("Successfully Verified data!");
		log.error("end diamond test Failover case 05");
	}
	
	@Test
	public void testFailover_06_change_client_a_to_wrong_diamond_group() {
		log.error("start diamond test Failover case 06");
		startCluster();

		changeToolConf(clList.get(0), test_bin, "actiontype", "put");
		changeToolConf(clList.get(0), test_bin, "datasize", put_count);
		execute_tair_tool(clList.get(0), test_bin);
		waitto(100);
		// ////////////////////////////////////////////////////
		String temp = "", diamond_fail = "";
		File file = new File("tair_group_fail.conf");
		assertTrue("tair_group_fail.conf not exists!", file.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			while ((temp = br.readLine()) != null) {
				diamond_fail += temp;
				diamond_fail += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-GROUP",
						diamond_fail);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////

		waitForFinish(clList.get(0), test_bin, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(0), test_bin);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.9);

		// change test tool's configuration to read
		if (!scp_file(clList.get(0), test_bin, "read.kv", clList.get(0),
				test_bin2))
			fail("scp file failed!");
		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.17:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.18:5168");
		changeToolConf(clList.get(0), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 1);
		int suc_b = getVerifySuccessful(clList.get(0), test_bin2);

		changeToolConf(clList.get(0), test_bin2, "master_cs",
				"10.232.4.14:5168");
		changeToolConf(clList.get(0), test_bin2, "slave_cs", "10.232.4.15:5168");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 2);
		int suc_a = getVerifySuccessful(clList.get(0), test_bin2);

		// verify get result
		assertTrue("get data verify failure!", suc_a + suc_b == put_count);
		log.error("Successfully Verified data!");
		log.error("end diamond test Failover case 06");
	}

	public void setUp() {
		log.error("clean tool and cluster while setUp!");
		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		clean_tool(clList.get(0), test_bin2);
		clean_tool(clList.get(1), test_bin2);
		reset_inval_cluster(ivList);
		reset_cluster(csList1, dsList1);
		reset_cluster(csList2, dsList2);
//		batch_recover_net(dsList1);
//		batch_recover_net(dsList2);
//		batch_recover_net(ivList);
		if (!batch_modify(csList1,
				FailOverBaseCase.tair_bin + "etc/group.conf", "_copy_count",
				"2"))
			fail("modify configure file failure");
		if (!batch_modify(csList2,
				FailOverBaseCase.tair_bin + "etc/group.conf", "_copy_count",
				"2"))
			fail("modify configure file failure");
		// /////////////////////////////////////////////////////
		String temp = "", diamond_a = "", diamond_b = "", diamond_group = "";
		File file_a = new File("tair_a.conf");
		assertTrue("tair_a.conf not exists!", file_a.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_a));
			while ((temp = br.readLine()) != null) {
				diamond_a += temp;
				diamond_a += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM3",
						diamond_a);
		assertTrue("change diamond failed!", response.isSuccess());
		// /////////////////////////////////////////////////////
		File file_b = new File("tair_b.conf");
		assertTrue("tair_b.conf not exists!", file_b.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_b));
			while ((temp = br.readLine()) != null) {
				diamond_b += temp;
				diamond_b += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		response = diamondSDKManagerImpl.pulishFromDefaultServerAfterModified(
				"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM4", diamond_b);
		assertTrue("change diamond failed!", response.isSuccess());
		// /////////////////////////////////////////////////////
		File file_group = new File("tair_group_back.conf");
		assertTrue("tair_group_back.conf not exists!", file_group.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_group));
			while ((temp = br.readLine()) != null) {
				diamond_group += temp;
				diamond_group += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-GROUP",
						diamond_group);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////
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
//		batch_recover_net(dsList1);
//		batch_recover_net(dsList2);
//		batch_recover_net(ivList);
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
		// /////////////////////////////////////////////////////
		String temp = "", diamond_a = "", diamond_b = "", diamond_group = "";
		File file_a = new File("tair_a.conf");
		assertTrue("tair_a.conf not exists!", file_a.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_a));
			while ((temp = br.readLine()) != null) {
				diamond_a += temp;
				diamond_a += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		ContextResult response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM3",
						diamond_a);
		assertTrue("change diamond failed!", response.isSuccess());
		// /////////////////////////////////////////////////////
		File file_b = new File("tair_b.conf");
		assertTrue("tair_b.conf not exists!", file_b.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_b));
			while ((temp = br.readLine()) != null) {
				diamond_b += temp;
				diamond_b += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		response = diamondSDKManagerImpl.pulishFromDefaultServerAfterModified(
				"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-CM4", diamond_b);
		assertTrue("change diamond failed!", response.isSuccess());
		// /////////////////////////////////////////////////////
		File file_group = new File("tair_group_back.conf");
		assertTrue("tair_group_back.conf not exists!", file_group.exists());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file_group));
			while ((temp = br.readLine()) != null) {
				diamond_group += temp;
				diamond_group += "\n";
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		response = diamondSDKManagerImpl
				.pulishFromDefaultServerAfterModified(
						"com.taobao.tair.testInvalid", "com.taobao.tair.testInvalid-GROUP",
						diamond_group);
		assertTrue("change diamond failed!", response.isSuccess());
		// ////////////////////////////////////////////////////
	}
}
