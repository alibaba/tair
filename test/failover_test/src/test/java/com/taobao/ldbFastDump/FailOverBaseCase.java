package com.taobao.ldbFastDump;

import com.ibm.staf.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Assert;

public class FailOverBaseCase {
	
	protected static Logger log=Logger.getLogger("Test");
	protected STAFHandle stafhandle=null;
	
	public FailOverBaseCase () {
		gethandle();
	}
	
	// Directory
	final static String tair_bin = "/home/admin/tair_bin_import/";
	final static String test_bin = "/home/admin/ashu/recovery/fastdump/";
	// Server Operation
	final static String start = "start";
	final static String stop = "stop";
	// Tool Option
	final static String put = "put";
	final static String get = "get";
	final static String rem = "rem";
	// Key Words for log
	final static String start_migrate = "need migrate,";
	final static String finish_migrate = "migrate all done";
	final static String finish_rebuild = "version changed";
	final static String verchange_group_1 = "\\[group_1\\] version changed";
	final static String verchange_group_2 = "\\[group_2\\] version changed";
	final static String rebuild_group_1 = "start rebuild table of group_1";
	final static String rebuild_group_2 = "start rebuild table of group_2";
	// Parameters
	final static int down_time = 10;
	final static int ds_down_time = 10;
	final static String put_count = "100000";
	final static float put_count_float = 100000.0f;
	// Server List
	final String csarr[] = new String[] { "10.232.4.14", "10.232.4.17" };
	final String dsarr[] = new String[] { "10.232.4.14", "10.232.4.15",
			"10.232.4.16", "10.232.4.17", "10.232.4.18", "10.232.4.19" };
	final String dsarr1[] = new String[] { "10.232.4.14", "10.232.4.15",
			"10.232.4.16" };
	final String dsarr2[] = new String[] { "10.232.4.17", "10.232.4.18",
			"10.232.4.19" };
	final List<String> csList = Arrays.asList(csarr);
	final List<String> dsList = Arrays.asList(dsarr);
	final List<String> dsList1 = Arrays.asList(dsarr1);
	final List<String> dsList2 = Arrays.asList(dsarr2);

	public boolean control_cs(String machine, String opID, int type) {
		log.debug("control cs:" + machine + " " + opID + " type=" + type);
		boolean ret = false;
		String cmd = "cd " + FailOverBaseCase.tair_bin + " && ./tair.sh "
				+ opID + "_cs && sleep 5";
		if (opID.equals(FailOverBaseCase.stop) && type == 1)
			cmd = "killall -9 tair_cfg_svr_import && sleep 2";
		executeShell(stafhandle, machine, cmd);
		cmd = "ps -ef|grep tair_cfg_svr_import|wc -l";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.debug("cs rc!=0");
			ret = false;
		} else {
			String stdout = getShellOutput(result);
			log.debug("------------cs ps result--------------" + stdout);
			if (opID.equals(FailOverBaseCase.start)
					&& (new Integer(stdout.trim())).intValue() != 3) {
				ret = false;
			} else if (opID.equals(FailOverBaseCase.stop)
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
		String cmd = "cd " + FailOverBaseCase.tair_bin + " && ./tair.sh "
				+ opID + "_ds ";
		int expectNum = 0;
		if (opID.equals(FailOverBaseCase.stop)) {
			expectNum = 2;
		} else if (opID.equals(FailOverBaseCase.start)) {
			expectNum = 3;
		}
		executeShell(stafhandle, machine, cmd);

		if (opID.equals(FailOverBaseCase.stop) && type == 1)
			cmd = "killall -9 tair_server_import && sleep 2";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		int waittime = 0;
		cmd = "ps -ef|grep tair_server_import|wc -l";
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
			if (!control_cs((String) it.next(), opID, type)) {
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
			if (!control_ds((String) it.next(), opID, type)) {
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
		String cmd = "cd " + FailOverBaseCase.tair_bin + " && ./tair.sh clean";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean clean_tool(String machine) {
		boolean ret = false;
		killall_tool_proc();
		String cmd = "cd " + FailOverBaseCase.test_bin + " && ";
		cmd += "./clean.sh";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean execute_data_verify_tool(String path, String opID) {
		log.debug("start verify tool, run batchData");
		boolean ret = false;
		String cmd = "cd " + path + " && ";
		cmd += "./batchData.sh " + opID;
		STAFResult result = executeShell(stafhandle, "local", cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean killall_tool_proc() {
		log.debug("force kill all data tool process");
		boolean ret = false;
		String cmd = "killall -9 tairtool_put; killall -9 tairtool_get";
		STAFResult result = executeShell(stafhandle, "local", cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean batch_clean_data(List<String> machines) {
		boolean ret = true;
		for (Iterator<String> it = machines.iterator(); it.hasNext();) {
			if (!clean_data((String) it.next()))
				ret = false;
		}
		return ret;
	}

	public boolean reset_cluster(List<String> csList, List<String> dsList) {
		boolean ret = false;
		log.debug("stop and clean cluster!");
		if (control_cluster(csList, dsList, FailOverBaseCase.stop, 1)
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

	public int touch_file(String machine, String file) {
		int ret = 0;
		String cmd = "touch " + file;
		log.debug("touch file:" + file + " on " + machine);
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = -1;
		return ret;
	}

	/**
	 * @param machine
	 *            machine which you want to operate
	 * @param confname
	 *            file which you want to modify , should include full path
	 * @param key
	 *            key which you want to modify
	 * @param value
	 *            value which you want to change
	 * @return
	 */
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
//			String stdout = getShellOutput(result);
//			if (stdout.trim().equals(value))
//				ret = true;
//			else
//				ret = false;
			ret = true;
		}
		return ret;
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////
	public boolean batch_modify(List<String> machines, String confname,
			String key, String value) {
		boolean ret = true;
		for (Iterator<String> it = machines.iterator(); it.hasNext();) {
			if (!modify_config_file((String) it.next(), confname, key, value)) {
				ret = false;
				break;
			}
		}
		return ret;
	}

	public boolean shutoff_net(String machine1, String machine2) {
		log.debug("shut off net between " + machine1 + " and " + machine2);
		String cmd = "ssh root@localhost " + "\"/sbin/iptables -A OUTPUT -d " + machine2 +" -j DROP\"";
		STAFResult result = executeShell(stafhandle, machine1, cmd);
		if (result.rc != 0) {
			return false;
		}
		else {
			cmd = "ssh root@localhost " + "\"/sbin/iptables-save\"";
			result = executeShell(stafhandle, machine1, cmd);
			if (result.rc != 0) {
				return false;
			}
		}
		cmd = "ssh root@localhost " + "\"/sbin/iptables -A INPUT -s " + machine2 +" -j DROP\"";
		result = executeShell(stafhandle, machine1, cmd);
		if (result.rc != 0) {
			return false;
		}
		else {
			cmd = "ssh root@localhost " + "\"/sbin/iptables-save\"";
			result = executeShell(stafhandle, machine1, cmd);
			if (result.rc != 0) {
				return false;
			}
			else {
				return true;
			}
		}
	}

	public boolean recover_net(String machine) {
		log.debug("recover net on " + machine);
		boolean ret = true;
		String cmd = "ssh root@localhost " + "\"/sbin/iptables -F\"";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		return ret;
	}

	public boolean batch_recover_net(List<String> machines) {
		boolean ret = true;
		for (Iterator<String> it = machines.iterator(); it.hasNext();) {
			if (!recover_net((String) it.next())) {
				ret = false;
				break;
			}
		}
		return ret;
	}

	protected int getVerifySuccessful(String machine, String path, String logfile) {
		int ret = 0;
		String verify = "tail -10 " + path
				+ logfile + "|grep \"Successful\"|awk -F\" \" \'{print $3}\'";
		log.debug("do verify on " + machine);
		STAFResult result = executeShell(stafhandle, machine, verify);
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
		log.error(ret);
		return ret;
	}

	// if(!copy_file(csList.get(0), FailOverBaseCase.table_path +
	// "group_1_server_table", local))fail("copy table file failed!");
	public boolean scp_file(String source_machine, String filename,
			String target_machine) {
		log.error("Copy " + filename + " from " + source_machine + " to "
				+ target_machine);
		boolean ret = true;
		String cmd = "scp " + filename + " " + target_machine + ":"
				+ FailOverBaseCase.test_bin;
		STAFResult result = executeShell(stafhandle, source_machine, cmd);
		if (result.rc != 0)
			ret = false;
		return ret;
	}

	public boolean cp_file(String machine, String source_path,
			String target_path, String filename) {
		log.error("copy " + filename + " from " + source_path + " to "
				+ filename + " on " + machine);
		boolean ret = true;
		String cmd = "cp " + source_path + filename + " " + target_path;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		return ret;
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////
	public boolean comment_line(String machine, String file, String keyword,
			String comment) {
		boolean ret = false;
		String cmd = "sed -i \'s/.*" + keyword + "/" + comment + "&/\' " + file;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			// String stdout=getShellOutput(result);
			ret = true;
		}
		return ret;
	}

	public boolean uncomment_line(String machine, String file, String keyword,
			String comment) {
		boolean ret = false;
		String cmd = "sed -i \'s/" + comment + "*\\(.*" + keyword
				+ ".*$\\)/\\1/\' " + file;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			// String stdout=getShellOutput(result);
			ret = true;
		}
		return ret;
	}

	public boolean batch_uncomment(List<String> machines, String confname,
			List<String> keywords, String comment) {
		boolean ret = true;
		for (Iterator<String> ms = machines.iterator(); ms.hasNext();) {
			String cms = (String) ms.next();
			for (Iterator<String> ks = keywords.iterator(); ks.hasNext();) {
				if (!uncomment_line(cms, confname, (String) ks.next(), comment))
					ret = false;
			}
		}
		return ret;
	}

	protected int getVerifySuccessful() {
		int ret = 0;
		String verify = "cd " + FailOverBaseCase.test_bin + " && ";
		verify += " tail -10 datadbg0.log|grep \"Successful\"|awk -F\" \" \'{print $3}\'";
		log.debug("do verify on local");
		STAFResult result = executeShell(stafhandle, "local", verify);
		if (result.rc != 0) {
			log.debug("get result " + result);
			ret = -1;
		} else {
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

	protected int getKeyNumber(String machine, String path, String key) {
		log.info("get " + key + " number on" + machine + ":" + path);
		int ret = -1;
		String cmd = "cd " + path + " && ";
		cmd += "grep \"Total " + key
				+ "\" get.log|tail -1|awk -F \" \" \'{print $3}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.error("get result " + result);
		} else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
				log.info("get " + key + " number:" + ret);
			} catch (Exception e) {
				log.error("get verify exception: " + stdout);
			}
		}
		return ret;
	}

	protected String control_sh(String machine, String path, String shell_name,
			String parameter) {
		log.info("run " + shell_name + " " + parameter + " on " + machine + ":"
				+ path);
		String ret = "";
		String cmd = "cd " + path + " && ./" + shell_name + " " + parameter;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = "";
		else {
			String stdout = getShellOutput(result);
			try {
				ret = stdout.trim();
			} catch (Exception e) {
				log.debug("get shell return exception: " + stdout);
				ret = "";
			}
		}
		return ret;
	}

	protected String getGroupKeyword(String machine, String group_name,
			String key) {
		log.info("check " + group_name + " status on master cs");
		String ret = "get failed!";
		String cmd = "cd " + FailOverBaseCase.tair_bin + " && ";
		if ("group_1".equals(group_name))
			cmd += "grep \"" + key
					+ "\" etc/group.conf |awk -F \"=\" \'{print $2}\'|head -1";
		else if ("group_2".equals(group_name))
			cmd += "grep \"" + key
					+ "\" etc/group.conf |awk -F \"=\" \'{print $2}\'|tail -1";
		else
			return ret;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.debug("get result " + result);
		} else {
			String stdout = getShellOutput(result);
			try {
				ret = stdout.trim();
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
			}
		}
		return ret;
	}

	protected boolean sendSignal(String machine, String process, String signal) {
		log.info("send signal " + signal + " to " + process + " on " + machine);
		boolean ret = false;
		int pid = -1;
		String cmd = "ps -ef|grep " + process
				+ "|grep -v grep|awk -F \" \" \'{print $2}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			String stdout = getShellOutput(result);
			try {
				pid = (new Integer(stdout.trim())).intValue();
				if (pid != -1) {
					cmd = "kill -" + signal + " " + pid;
					result = executeShell(stafhandle, machine, cmd);
					if (result.rc != 0)
						ret = false;
					else
						ret = true;
				}
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
			}
		}
		return ret;
	}
	/*
	 * grep "\[group_1\] version changed:" logs/config.log |awk -F " " '{print
	 * $10}'|tail -1 grep "\[group_2\] version
	 * changed:" logs/config.log |awk -F " " '{print $10}'|tail -1
	 */
	
	
	
	
	
	
	
	
	public void waitto(int sec)
	{
		log.debug("wait for "+sec+"s");
		try
		{
			Thread.sleep(sec*1000);
		}
		catch(Exception e)
		{}
	}
	public STAFResult executeShell(STAFHandle stafHandle,String machine ,String cmd )
	{
		STAFResult result=stafHandle.submit2(machine, "PROCESS", "START SHELL COMMAND "+STAFUtil.wrapData(cmd)+" WAIT RETURNSTDOUT STDERRTOSTDOUT");
		return result;
	}
	private void gethandle()
	{
		try{
			stafhandle=new STAFHandle("Base Handle");
			}
			catch(STAFException e)
			{
				log.error("create handle failed!",e);
			}
	}
	public String getShellOutput(STAFResult result)
	{
		Map rstMap=(Map)result.resultObj;
		List rstList=(List)rstMap.get("fileList");
		Map stdoutMap=(Map)rstList.get(0);
		String stdout=(String)stdoutMap.get("data");
		return stdout;
	}
	public void fail(String message) {
		Assert.assertTrue(message, false);
	}
}
