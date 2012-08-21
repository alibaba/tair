/**
 * 
 */
package com.taobao.uic;

import com.ibm.staf.*;
import com.taobao.tairtest.ConfParser;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.BeforeClass;

public class FailOverBaseCase extends BaseTestCase {

	// db type
	protected static String db_type;
	// directory
	protected static String tair_bin;
	protected static String test_bin;
	protected static String test_bin1;
	protected static String test_bin2;
	protected static String ycsb_bin;
	// key words for log
	protected static String start_migrate;
	protected static String finish_migrate;
	protected static String finish_rebuild;
	// parameters
	protected static int down_time;
	protected static int ds_down_time;
	protected static String put_count;
	protected static float put_count_float;
	protected static String kv_name;
	// server list
	protected static List<String> csList;
	protected static List<String> dsList;
	protected static List<String> csList1;
	protected static List<String> csList2;
	protected static List<String> dsList1;
	protected static List<String> dsList2;
	protected static List<String> addList;
	protected static List<String> clientList;
	protected static List<String> ycsbList;
	// server name
	protected static String csname;
	protected static String dsname;
	protected static String groupname;
	protected static String csport;
	protected static String dsport;
	protected static String toolname;
	protected static String toolconf;
	protected static String ycsbname;
	protected static String ycsbconf;
	// distinguish if mdb needs touch
	protected static int touch_flag;
	// server operation
	final static String groupconf = "etc/group.conf";
	final static String start = "start";
	final static String stop = "stop";
	final static String copycount = "_copy_count";
	// tool option
	final static String actiontype = "actiontype";
	final static String datasize = "datasize";
	final static String filename = "filename";
	final static String put = "put";
	final static String get = "get";
	final static String rem = "rem";
	final static String putget = "putget";
	// system option
	final static String local = "local";
	final static String low = "low";
	final static String high = "high";
	// uic option
	final static String _pre_load_flag = "_pre_load_flag";
	final static String tmp_down_server = "tmp_down_server";
	final static String _bucket_placement_flag = "_bucket_placement_flag";
	final static String resetserver = "resetserver";
	final static String resetgroup = "resetgroup";
	// network flow
	final static int flow_low = 2000000;
	final static int flow_middle = 15000000;
	final static int flow_high = 30000000;
	final static String SIGUSR2 = "12";
	// tool recover option
	final static String RECOVER_ALL = "RECOVER_ALL";
	final static String RECOVER_A_1DS = "RECOVER_A_1DS";
	final static String RECOVER_B_1DS = "RECOVER_B_1DS";
	final static String RECOVER_A_1CLS = "RECOVER_A_1CLS";
	final static String RECOVER_B_1CLS = "RECOVER_B_1CLS";

	@BeforeClass
	public static void setUpClass() {
		ConfParser parser = new ConfParser("failover.conf");

		// Read public configuration
		readPublic(parser);

		// Read mdb configuration
		if ("mdb".equals(db_type))
			readMdbConf(parser);

		// Read ldb configuration
		else if ("ldb".equals(db_type))
			readLdbConf(parser);

		// Read rdb configuration
		else if ("rdb".equals(db_type))
			readRdbConf(parser);

		// Read invalid configuration
		else if ("invalid".equals(db_type))
			readInvalidConf(parser);

		// Read uic configuration
		else if ("uic".equals(db_type))
			readUicConf(parser);

		else
			assertTrue(
					"db_type error! it must be mdb | ldb | rdb | invalid, but actual "
							+ db_type, false);
	}

	private static void readPublic(ConfParser ps) {
		db_type = ps.getValue("public", "db_type");
		tair_bin = ps.getValue("public", "tair_bin");
		test_bin = ps.getValue("public", "test_bin");
		start_migrate = ps.getValue("public", "start_migrate");
		finish_migrate = ps.getValue("public", "finish_migrate");
		finish_rebuild = ps.getValue("public", "finish_rebuild");
		down_time = Integer.parseInt(ps.getValue("public", "down_time"));
		ds_down_time = Integer.parseInt(ps.getValue("public", "ds_down_time"));
		put_count = ps.getValue("public", "put_count");
		put_count_float = (float) Integer.parseInt(put_count);
		kv_name = ps.getValue("public", "kv_name");
		csname = ps.getValue("public", "csname");
		dsname = ps.getValue("public", "dsname");
		groupname = ps.getValue("public", "groupname");
		csport = ps.getValue("public", "csport");
		dsport = ps.getValue("public", "dsport");
		toolname = ps.getValue("public", "toolname");
		toolconf = ps.getValue("public", "toolconf");
	}

	private static void readMdbConf(ConfParser ps) {
		String[] csArray = ps.getValue("mdb", "cs_list").split("\\,");
		if (csArray.length != 0)
			csList = Arrays.asList(csArray);
		else
			assertTrue("csList null! it must be at least one ip!", false);

		String[] dsArray = ps.getValue("mdb", "ds_list").split("\\,");
		if (dsArray.length != 0)
			dsList = Arrays.asList(dsArray);
		else
			assertTrue("dsList null! it must be at least one ip!", false);

		touch_flag = Integer.parseInt(ps.getValue("mdb", "touch_flag"));
	}

	private static void readLdbConf(ConfParser ps) {
	}

	private static void readRdbConf(ConfParser ps) {
	}

	private static void readInvalidConf(ConfParser ps) {
		String[] csArray1 = ps.getValue("invalid", "cs_list1").split("\\,");
		if (csArray1.length != 0)
			csList1 = Arrays.asList(csArray1);
		else
			assertTrue("csList1 null! it must be at least one ip!", false);

		String[] csArray2 = ps.getValue("invalid", "cs_list2").split("\\,");
		if (csArray2.length != 0)
			csList2 = Arrays.asList(csArray2);
		else
			assertTrue("csList2 null! it must be at least one ip!", false);

		String[] dsArray1 = ps.getValue("invalid", "ds_list1").split("\\,");
		if (dsArray1.length != 0)
			dsList1 = Arrays.asList(dsArray1);
		else
			assertTrue("dsList1 null! it must be at least one ip!", false);

		String[] dsArray2 = ps.getValue("invalid", "ds_list2").split("\\,");
		if (dsArray2.length != 0)
			dsList2 = Arrays.asList(dsArray2);
		else
			assertTrue("dsList2 null! it must be at least one ip!", false);

		test_bin1 = ps.getValue("invalid", "test_bin1");
		test_bin2 = ps.getValue("invalid", "test_bin2");
	}

	private static void readUicConf(ConfParser ps) {
		String[] csArray1 = ps.getValue("uic", "cs_list").split("\\,");
		if (csArray1.length != 0)
			csList1 = Arrays.asList(csArray1);
		else
			assertTrue("csList1 null! it must be at least one ip!", false);

		String[] csArray2 = ps.getValue("uic", "cs_list2").split("\\,");
		if (csArray2.length != 0)
			csList2 = Arrays.asList(csArray2);
		else
			assertTrue("csList2 null! it must be at least one ip!", false);

		String[] dsArray1 = ps.getValue("uic", "ds_list").split("\\,");
		if (dsArray1.length != 0)
			dsList1 = Arrays.asList(dsArray1);
		else
			assertTrue("dsList1 null! it must be at least one ip!", false);

		String[] dsArray2 = ps.getValue("uic", "ds_list2").split("\\,");
		if (dsArray2.length != 0)
			dsList2 = Arrays.asList(dsArray2);
		else
			assertTrue("dsList2 null! it must be at least one ip!", false);
		
		String[] addArray = ps.getValue("uic", "add_list").split("\\,");
		if (addArray.length != 0)
			addList = Arrays.asList(addArray);
		else
			assertTrue("addList null! it must be at least one ip!", false);

		String[] clArray = ps.getValue("uic", "client_list").split("\\,");
		if (clArray.length != 0)
			clientList = Arrays.asList(clArray);
		else
			assertTrue("client list null! it must be at least one ip!", false);

		String[] ycsbArray = ps.getValue("uic", "ycsb_list").split("\\,");
		if (clArray.length != 0)
			ycsbList = Arrays.asList(ycsbArray);
		else
			assertTrue("ycsb list null! it must be at least one ip!", false);

		ycsb_bin = ps.getValue("uic", "ycsb_bin");
		ycsbname = ps.getValue("uic", "ycsbname");
		ycsbconf = ps.getValue("uic", "ycsbconf");
	}

	public boolean control_cs(String machine, String opID, int type) {
		log.debug("control cs:" + machine + " " + opID + " type=" + type);
		boolean ret = false;
		String cmd = "cd " + tair_bin + " && ./tair.sh " + opID
				+ "_cs && sleep 5";
		if (opID.equals(stop) && type == 1)
			cmd = "killall -9 " + csname + " && sleep 2";
		executeShell(stafhandle, machine, cmd);
		cmd = "ps -ef|grep " + csname + "|wc -l";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.debug("cs rc!=0");
			ret = false;
		} else {
			String stdout = getShellOutput(result);
			log.debug("------------cs ps result--------------" + stdout);
			if (opID.equals(start)
					&& (new Integer(stdout.trim())).intValue() != 3) {
				ret = false;
			} else if (opID.equals(stop)
					&& (new Integer(stdout.trim())).intValue() != 2) {
				ret = false;
			} else {
				ret = true;
			}
		}
		return ret;
	}

	public boolean control_ds(String machine, String opID, int type) {
		log.debug("control ds:" + machine + " " + opID + " type=" + type);
		boolean ret = false;
		String cmd = "cd " + tair_bin + " && ./tair.sh " + opID + "_ds ";
		int expectNum = 0;
		if (opID.equals(stop)) {
			expectNum = 2;
		} else if (opID.equals(start)) {
			expectNum = 3;
		}
		executeShell(stafhandle, machine, cmd);

		if (opID.equals(stop) && type == 1)
			cmd = "killall -9 " + dsname + " && sleep 2";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		int waittime = 0;
		cmd = "ps -ef|grep " + dsname + "|wc -l";
		while (waittime < 110) {
			result = executeShell(stafhandle, machine, cmd);
			if (result.rc != 0) {
				log.debug("ds rc!=0");
				ret = false;
			} else {
				String stdout = getShellOutput(result);
				if ((new Integer(stdout.trim())).intValue() == expectNum) {

					log.debug("------------ds ps result--------------" + stdout);
					ret = true;
					break;
				} else {
					ret = false;
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {

					}
					waittime++;
				}
			}
		}
		return ret;
	}

	public boolean batch_control_cs(List<String> cs_group, String opID, int type) {
		boolean ret = false;
		for (Iterator<String> it = cs_group.iterator(); it.hasNext();) {
			if (!control_cs(it.next(), opID, type)) {
				ret = false;
				break;
			} else
				ret = true;
		}
		return ret;
	}

	public boolean batch_control_ds(List<String> ds_group, String opID, int type) {
		boolean ret = false;
		for (Iterator<String> it = ds_group.iterator(); it.hasNext();) {
			if (!control_ds(it.next(), opID, type)) {
				ret = false;
				break;
			} else
				ret = true;
		}
		return ret;
	}

	public boolean control_cluster(List<String> cs_group,
			List<String> ds_group, String opID, int type) {
		boolean ret = false;
		if (!batch_control_ds(ds_group, opID, type)
				|| !batch_control_cs(cs_group, opID, type))
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean clean_data(String machine) {
		boolean ret = false;
		String cmd = "cd " + tair_bin + " && ./tair.sh clean";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean clean_tool(String machine) {
		boolean ret = false;
		killall_tool_proc(machine);
		String cmd = "cd " + test_bin + " && ";
		cmd += "./clean.sh";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean batch_clean_tool(List<String> client_group) {
		boolean ret = false;
		for (Iterator<String> it = client_group.iterator(); it.hasNext();) {
			if (!clean_tool(it.next())) {
				ret = false;
				break;
			} else
				ret = true;
		}
		return ret;
	}

	public boolean clean_ycsb(String machine) {
		boolean ret = false;
		killall_ycsb_proc(machine);
		String cmd = "cd " + ycsb_bin + " && ";
		cmd += "rm -f out/*res";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean batch_clean_ycsb(List<String> ycsb_group) {
		boolean ret = false;
		for (Iterator<String> it = ycsb_group.iterator(); it.hasNext();) {
			if (!clean_ycsb(it.next())) {
				ret = false;
				break;
			} else
				ret = true;
		}
		return ret;
	}

	public boolean execute_data_verify_tool() {
		return execute_data_verify_tool("local");
	}

	public boolean execute_data_verify_tool(String machine) {
		return execute_data_verify_tool(machine, test_bin);
	}

	public boolean execute_data_verify_tool(String machine, String test_bin) {
		log.debug("start verify tool,run batchData");
		boolean ret = false;
		String cmd = "cd " + test_bin + " && ";
		cmd += "./batchData.sh";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean execute_ycsb_tool() {
		return execute_ycsb_tool("local");
	}

	public boolean execute_ycsb_tool(String machine) {
		return execute_ycsb_tool(machine, ycsb_bin);
	}

	public boolean execute_ycsb_tool(String machine, String test_bin) {
		log.info("start ycsb tool on " + machine);
		boolean ret = false;
		String cmd = "cd " + test_bin + " && ";
		cmd += "./run.sh workload_tair uictest &";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}
	
	public boolean batch_execute_ycsb_tool(List<String> ycsb_group) {
		boolean ret = false;
		for (Iterator<String> it = ycsb_group.iterator(); it.hasNext();) {
			if (!execute_ycsb_tool(it.next())) {
				ret = false;
				break;
			} else
				ret = true;
			waitto(2);
		}
		return ret;
	}
	
	public float getReturnTime(String machine) {
		float ret = -1;
		String cmd = "cd " + ycsb_bin + " && ";
		cmd += "grep \"INSERT AverageLatency(ms)\" out/uictest.res|tail -1|awk -F \"=\" \'{print $2}\'|awk -F \"]\" \'{print $1}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			log.error("get staf execute result not 0: " + result.rc);
		else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Float(stdout.trim())).floatValue();
				log.debug("get return time on " + machine + ": " + ret);
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
			}
		}
		return ret;
	}

	public boolean killall_tool_proc(String machine) {
		log.debug("force kill all tool process on " + machine);
		boolean ret = false;
		String cmd = "kill -9 `ps -ef|grep " + toolname
				+ "|grep -v grep |awk -F \" \" \'{print $2}\'`";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean killall_ycsb_proc(String machine) {
		log.debug("force kill all ycsb process on " + machine);
		boolean ret = false;
		String cmd = "ps -ef|grep " + ycsbname
				+ " |grep -v grep |grep -v sh |awk -F \" \" \'{print $2}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			String stdout = getShellOutput(result);
			cmd = "kill -9 " + stdout;
			result = executeShell(stafhandle, machine, cmd);
			if (result.rc != 0)
				ret = false;
			else
				ret = true;
		}
		return ret;
	}

	public boolean batch_clean_data(List<String> machines) {
		boolean ret = true;
		for (Iterator<String> it = machines.iterator(); it.hasNext();) {
			if (!clean_data(it.next()))
				ret = false;
		}
		return ret;
	}

	public boolean reset_cluster(List<String> csList, List<String> dsList) {
		boolean ret = false;
		log.debug("stop and clean cluster!");
		if (control_cluster(csList, dsList, stop, 1)
				&& batch_clean_data(csList) && batch_clean_data(dsList))
			ret = true;
		return ret;
	}

	public int check_process(String machine, String prname) {
		log.debug("check process " + prname + " on " + machine);
		int ret = 0;
		String cmd = "ps -ef|grep " + prname + "|wc -l";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = -1;
		else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
				ret = -1;
			}
		}
		return ret;
	}

	public int check_keyword(String machine, String keyword, String logfile) {
		int ret = 0;
		String cmd = "grep \"" + keyword + "\" " + logfile + " |wc -l";
		log.debug("check keyword:" + cmd + " on " + machine + " in file:"
				+ logfile);
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = -1;
		else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
				log.error("time=" + ret);
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
				ret = -1;
			}
		}
		return ret;
	}

	public boolean modify_config_file(String machine, String confname,
			String key, String value) {
		log.debug("change file:" + confname + " on " + machine + " key=" + key
				+ " value=" + value);
		boolean ret = false;
		String cmd = "sed -i \"s/" + key + "=.*$/" + key + "=" + value + "/\" "
				+ confname + " && grep \"" + key + "=.*$\" " + confname
				+ "|awk -F\"=\" \'{print $2}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			String stdout = getShellOutput(result);
			if (stdout.trim().equals(value.trim()))
				ret = true;
			else
				ret = false;
		}
		return ret;
	}
	
	public void modify_file(String machine, String confname, String src,
			String des) {
		String cmd = "sed -i \"s/" + src + "/" + des + "/\" " + confname;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			fail("staf result rc not 0: " + result.rc);
		else {
			log.info("change file: " + confname + " on " + machine + " successful!");
		}
	}

	public boolean batch_modify(List<String> machines, String confname,
			String key, String value) {
		boolean ret = true;
		for (Iterator<String> it = machines.iterator(); it.hasNext();) {
			if (!modify_config_file(it.next(), confname, key, value)) {
				ret = false;
				break;
			}
		}
		return ret;
	}

	public void shutoff_net(String machine1, String port1, String machine2,
			String port2) {
		log.debug("shut off net between " + machine1 + ":" + port1 + " and "
				+ machine2 + ":" + port2);
		String cmd = "ssh root@localhost " + "\"/sbin/iptables -A OUTPUT -d "
				+ machine2 + " -p tcp --dport " + port2
				+ " -j DROP && /sbin/iptables-save\"";
		STAFResult result = executeShell(stafhandle, machine1, cmd);
		if (result.rc != 0) {
			fail("shut off net between " + machine1 + " and " + machine2
					+ " failed!");
		} else {
			cmd = "ssh root@localhost " + "\"/sbin/iptables -A INPUT -s "
					+ machine2 + " -p tcp --dport " + port1
					+ " -j DROP && /sbin/iptables-save\"";
			result = executeShell(stafhandle, machine1, cmd);
			if (result.rc != 0) {
				fail("shut off net between " + machine1 + " and " + machine2
						+ " failed!");
			} else {
				log.info("shut off net between " + machine1 + " and "
						+ machine2 + " successful!");
			}
		}
	}

	protected int getVerifySuccessful(String machine) {
		int ret = 0;
		String verify = "tail -10 " + test_bin
				+ "datadbg0.log|grep \"Successful\"|awk -F\" \" \'{print $7}\'";
		log.debug("do verify on " + machine);
		STAFResult result = executeShell(stafhandle, machine, verify);
		if (result.rc != 0)
			ret = -1;
		else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
				log.info("verify result: " + ret);
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
				ret = -1;
			}
		}
		log.error(ret);
		return ret;
	}

	protected int[] getToolResult(String machine, String key) {
		int suc = 0;
		int fail = 0;
		int failConNError = 0;
		int failException = 0;

		String cmd = "cd " + test_bin + " && tail -200 datadbg0.log | grep \""
				+ key + ":\" | tail -1";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.error("get result " + result);
		} else {
			String stdout = getShellOutput(result);
			String[] outArray = stdout.trim().split(" ");
			int outLength = outArray.length;
			suc = Integer.parseInt(outArray[outLength - 4]);
			fail = Integer.parseInt(outArray[outLength - 3]);
			failConNError = Integer.parseInt(outArray[outLength - 2]);
			failException = Integer.parseInt(outArray[outLength - 1]);
			log.debug("check " + key + " on " + machine + "----success: " + suc
					+ " fail: " + fail + " failConNError: " + failConNError
					+ " failException: " + failException);
		}
		return new int[] { suc, fail, failConNError, failException };
	}

	protected void waitToolRecover(String machine, String recoverOption) {
		int waitcnt = 0;
		int[] putArrayOld = getToolResult(machine, put);
		int[] getArrayOld = getToolResult(machine, get);
		int[] remArrayOld = getToolResult(machine, rem);
		int[] putArrayNow;
		int[] getArrayNow;
		int[] remArrayNow;
		while (waitcnt++ < 50) {
			waitto(2);
			putArrayNow = getToolResult(machine, put);
			getArrayNow = getToolResult(machine, get);
			remArrayNow = getToolResult(machine, rem);

			if (RECOVER_ALL.equals(recoverOption)
					&& putArrayNow[0] > putArrayOld[0]
					&& putArrayNow[1] == putArrayOld[1]
					&& getArrayNow[0] > getArrayOld[0]
					&& getArrayNow[1] == getArrayOld[1]
					&& remArrayNow[0] > remArrayOld[0]
					&& remArrayNow[1] == remArrayOld[1])
				break;
			else if (RECOVER_A_1DS.equals(recoverOption)
					&& putArrayNow[0] > putArrayOld[0]
					&& putArrayNow[1] > putArrayOld[1]
					&& putArrayNow[1] == putArrayNow[2]
					&& getArrayNow[0] > getArrayOld[0]
					&& getArrayNow[1] == getArrayOld[1]
					&& remArrayNow[0] > remArrayOld[0]
					&& remArrayNow[1] > remArrayOld[1]
					&& remArrayNow[1] == remArrayNow[2])
				break;
			else if (RECOVER_B_1DS.equals(recoverOption)
					&& putArrayNow[0] > putArrayOld[0]
					&& putArrayNow[1] > putArrayOld[1]
					&& (putArrayNow[1] - putArrayOld[1] == putArrayNow[3]
							- putArrayOld[3])
					&& getArrayNow[0] > getArrayOld[0]
					&& getArrayNow[1] == getArrayOld[1]
					&& remArrayNow[0] > remArrayOld[0]
					&& remArrayNow[1] > remArrayOld[1]
					&& (remArrayNow[1] - remArrayOld[1] == remArrayNow[3]
							- remArrayOld[3]))
				break;
			else if (RECOVER_A_1CLS.equals(recoverOption)
                    && putArrayNow[0] == putArrayOld[0]
                    && putArrayNow[1] > putArrayOld[1]
                    && putArrayNow[1] == putArrayNow[2]
                    && getArrayNow[0] > getArrayOld[0]
                    && getArrayNow[1] == getArrayOld[1]
                    && remArrayNow[0] == remArrayOld[0]
                    && remArrayNow[1] > remArrayOld[1]
                    && remArrayNow[1] == remArrayNow[2])
                break;
            else if (RECOVER_B_1CLS.equals(recoverOption)
                    && putArrayNow[0] == putArrayOld[0]
                    && putArrayNow[1] > putArrayOld[1]
                    && (putArrayNow[1] - putArrayOld[1] == putArrayNow[3] - putArrayOld[3])
                    && getArrayNow[0] > getArrayOld[0]
                    && getArrayNow[1] == getArrayOld[1]
                    && remArrayNow[0] == remArrayOld[0]
                    && remArrayNow[1] > remArrayOld[1]
                    && (remArrayNow[1] - remArrayOld[1] == remArrayNow[3] - remArrayOld[3]))
                break;
			else {
				putArrayOld = putArrayNow;
				getArrayOld = getArrayNow;
				remArrayOld = remArrayNow;
			}
		}
		if (waitcnt >= 50)
			fail("wait time out, client still not recover!");
		else
			log.info("client on " + machine + " recoverd!");
	}

	protected int checkNetworkFlow(String machine) {
		int ret = -1;
		String cmd = "sar -n DEV 1 10| tail -5 - |grep eth0 | awk \'{printf \"%ld\\n\", $5+$6}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.error("get result failed: " + result);
		} else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
				log.debug("get network flow on " + machine + ": " + ret);
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
				ret = -1;
			}
		}
		return ret;
	}

	protected String resetServer(String machine, String option) {
		String ret = null;
		String cmd = "cd " + tair_bin + " && ./calcgroupA.sh " + option;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.error("get result failed: " + result);
		} else {
			String stdout = getShellOutput(result);
			try {
				ret = stdout.trim();
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
			}
		}
		return ret;
	}
	
	protected String controlGroupCalc(String machine, String option) {
		String ret = null;
		String cmd = "cd " + tair_bin + " && ./calcgroupA.sh " + option;
		if(csList2.get(0).equals(machine))
			cmd = "cd " + tair_bin + " && ./calcgroupB.sh " + option;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.error("get result failed: " + result);
		} else {
			String stdout = getShellOutput(result);
			try {
				ret = stdout.trim();
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
			}
		}
		return ret;
	}

	protected String getKeyword(String machine, String keyword, String logfile) {
		String ret = null;
		String cmd = "grep \"" + keyword + "\" " + logfile
				+ "|awk -F \"=\" \'{print $2}\'";
		log.debug("check keyword:" + cmd + " on " + machine + " in file:"
				+ logfile);
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			log.error("staf result not 0: " + result.rc);
		else {
			String stdout = getShellOutput(result);
			try {
				ret = stdout.trim();
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
			}
		}
		return ret;
	}
	
	protected void addToGroup(String machine, String fromKey, String addDs) {
		String cmd = "cd " + tair_bin + " && sed -i \"/" + fromKey
				+ "/a\\_server_list=" + addDs + ":" + dsport
				+ "\" etc/group.conf";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			fail("staf result rc not 0: " + result.rc);
		else {
			log.info("add ds: " + addDs + " to group.conf on " + machine
					+ " successful!");
		}
	}

	protected void startCluster() {
		log.info("Starting Cluster......");
		if (!control_cluster(csList1, dsList1, start, 0))
			fail("start cluster1 failure!");
		if (!control_cluster(csList2, dsList2, start, 0))
			fail("start cluster2 failure!");
		log.info("Start Cluster Successful!");
		waitto(down_time);
		String calcCmd = "flowlimit";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("set flowlimit on " + csList1.get(0) + " successful!");
		assertEquals("successful", controlGroupCalc(csList2.get(0), calcCmd));
		log.info("set flowlimit on " + csList2.get(0) + " successful!");
	}

	protected int[] prepareData() {
		// prepare data on every client
		modifyToolConf(clientList.get(0), put);
		modifyToolConf(clientList.get(1), put);
		execute_data_verify_tool(clientList.get(0));
		execute_data_verify_tool(clientList.get(1));
		waitToolFinish(clientList.get(0));
		waitToolFinish(clientList.get(1));
		int datacnt1 = getVerifySuccessful(clientList.get(0));
		int datacnt2 = getVerifySuccessful(clientList.get(1));
		assertTrue("put successful rate small than 90%!", datacnt1
				/ put_count_float > 0.9);
		assertTrue("put successful rate small than 90%!", datacnt2
				/ put_count_float > 0.9);
		log.info("Write data over!");

		return new int[] { datacnt1, datacnt2 };
	}

	protected void verifyData(int[] dataArray) {
		modifyToolConf(clientList.get(0), get);
		modifyToolConf(clientList.get(1), get);
		execute_data_verify_tool(clientList.get(0));
		execute_data_verify_tool(clientList.get(1));
		waitToolFinish(clientList.get(0));
		waitToolFinish(clientList.get(1));
		int datacnt1 = getVerifySuccessful(clientList.get(0));
		int datacnt2 = getVerifySuccessful(clientList.get(1));
		assertTrue("get successful wrong! on " + clientList.get(0),
				datacnt1 == dataArray[0]);
		assertTrue("get successful wrong! on " + clientList.get(1),
				datacnt2 == dataArray[1]);
		log.info("verify data over!");
	}
	
	protected void verifyData(int data1, boolean data1Flag, int data2, boolean data2Flag) {
		modifyToolConf(clientList.get(0), get);
		modifyToolConf(clientList.get(1), get);
		execute_data_verify_tool(clientList.get(0));
		execute_data_verify_tool(clientList.get(1));
		waitToolFinish(clientList.get(0));
		waitToolFinish(clientList.get(1));
		int datacnt1 = getVerifySuccessful(clientList.get(0));
		int datacnt2 = getVerifySuccessful(clientList.get(1));
		if(data1Flag)
			assertTrue("get successful wrong! on " + clientList.get(0),
				datacnt1 == data1);
		else
			assertTrue("get successful wrong! on " + clientList.get(0),
					datacnt1 > data1);
		if(data2Flag)
			assertTrue("get successful wrong! on " + clientList.get(1),
				datacnt2 == data2);
		else
			assertTrue("get successful wrong! on " + clientList.get(1),
					datacnt2 > data2);
		log.info("verify data over!");
	}

	protected void startToolPutGet(String machine) {
		modifyToolConf(machine, putget);
		execute_data_verify_tool(machine);
	}

	protected void modifyToolConf(String machine, String actionType) {
		if (!modify_config_file(machine, test_bin + toolconf, actiontype,
				actionType))
			fail("modify configure file failure");
		if (!modify_config_file(machine, test_bin + toolconf, datasize,
				put_count))
			fail("modify configure file failure");
	}

	protected void waitToolFinish(String machine) {
		int waitcnt = 0;
		while (check_process(machine, toolname) != 2) {
			waitto(4);
			if (++waitcnt > 90)
				break;
		}
		if (waitcnt > 90)
			fail("wait time out!");
	}
	
	protected void sendSignal(String machine, String process, String signal) {
		log.info("send signal " + signal + " to " + process + " on " + machine);
		int pid = -1;
		String cmd = "ps -ef|grep " + process
				+ "|grep -v grep |grep -v sh|awk -F \" \" \'{print $2}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			fail("result.rc not 0 while send signal: " + result.rc);
		else {
			String stdout = getShellOutput(result);
			try {
				pid = (new Integer(stdout.trim())).intValue();
				if (pid != -1) {
					cmd = "kill -" + signal + " " + pid;
					result = executeShell(stafhandle, machine, cmd);
					if (result.rc != 0)
						fail("result.rc not 0 while send signal: " + result.rc);
				}
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
			}
		}
	}
	
	protected void batchSendSignal(List<String> machineList, String process,
			String signal) {
		for (Iterator<String> it = machineList.iterator(); it.hasNext();)
			sendSignal(it.next(), process, signal);
	}
}

