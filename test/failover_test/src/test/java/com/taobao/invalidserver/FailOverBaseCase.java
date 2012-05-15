/**
 * 
 */
package com.taobao.invalidserver;

import com.ibm.staf.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;

/**
 * @author ashu.cs
 * 
 */
public class FailOverBaseCase extends BaseTestCase {
	// Directory
	final static String tair_bin = "/home/admin/tair_bin_iv/";
	final static String test_bin = "/home/admin/ashu/TairTool/";
	final static String test_bin2 = "/home/admin/baoni/TairTool/";
	final static String iv_bin = "/home/admin/tair_bin_iv/";
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
	// Parameters
	final static int down_time = 10;
	final static int ds_down_time = 10;
	final static String put_count = "100000";
	final static float put_count_float = 100000.0f;
	// Server List
	final String csarr1[] = new String[] { "10.232.4.14", "10.232.4.15" };
	final String csarr2[] = new String[] { "10.232.4.17", "10.232.4.18" };
	final String dsarr1[] = new String[] { "10.232.4.14", "10.232.4.15", "10.232.4.16" };
	final String dsarr2[] = new String[] { "10.232.4.17", "10.232.4.18", "10.232.4.19" };
	final String ivarr[] = new String[] { "10.232.4.20", "10.232.4.22", "10.232.4.21" };
    final String clarr[] = new String[] {"10.232.4.13", "10.232.4.23"};
	final List<String> csList1 = Arrays.asList(csarr1);
	final List<String> csList2 = Arrays.asList(csarr2);
	final List<String> dsList1 = Arrays.asList(dsarr1);
	final List<String> dsList2 = Arrays.asList(dsarr2);
	final List<String> ivList = Arrays.asList(ivarr);
    final List<String> clList = Arrays.asList(clarr);

	/**
	 * @param machine
	 * @param opID
	 * @param type
	 *            0:normal 1:force
	 * @return
	 */
	public boolean control_cs(String machine, String opID, int type) {
		log.debug("control cs:" + machine + " " + opID + " type=" + type);
		boolean ret = false;
		String cmd = "cd " + FailOverBaseCase.tair_bin + " && ./tair.sh " + opID + "_cs && sleep 5";
		if (opID.equals(FailOverBaseCase.stop) && type == 1)
			cmd = "killall -9 tair_cfg_svr_iv && sleep 2";
		executeShell(stafhandle, machine, cmd);
		cmd = "ps -ef|grep tair_cfg_svr_iv|wc -l";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0) {
			log.debug("cs rc!=0");
			ret = false;
		} else {
			String stdout = getShellOutput(result);
			log.debug("------------cs ps result--------------" + stdout);
			if (opID.equals(FailOverBaseCase.start) && (new Integer(stdout.trim())).intValue() != 3) {
				ret = false;
			} else if (opID.equals(FailOverBaseCase.stop) && (new Integer(stdout.trim())).intValue() != 2) {
				ret = false;
			} else {
				ret = true;
			}
		}
		return ret;
	}

	/**
	 * @param machine
	 * @param opID
	 * @param type
	 *            0:normal 1:force
	 * @return
	 */
	public boolean control_ds(String machine, String opID, int type) {
		log.debug("control ds:" + machine + " " + opID + " type=" + type);
		boolean ret = false;
		String cmd = "cd " + FailOverBaseCase.tair_bin + " && ./tair.sh " + opID + "_ds ";
		int expectNum = 0;
		if (opID.equals(FailOverBaseCase.stop)) {
			expectNum = 2;
		} else if (opID.equals(FailOverBaseCase.start)) {
			expectNum = 3;
		}
		executeShell(stafhandle, machine, cmd);

		if (opID.equals(FailOverBaseCase.stop) && type == 1)
			cmd = "killall -9 tair_server_iv && sleep 2";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		int waittime = 0;
		cmd = "ps -ef|grep tair_server_iv|wc -l";
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

	/**
	 * @param cs_group
	 * @param opID
	 * @param type
	 *            0:normal 1:force
	 * @return
	 */
	public boolean batch_control_cs(List cs_group, String opID, int type) {
		boolean ret = false;
		for (Iterator it = cs_group.iterator(); it.hasNext();) {
			if (!control_cs((String) it.next(), opID, type)) {
				ret = false;
				break;
			} else
				ret = true;
		}
		return ret;
	}

	/**
	 * @param ds_group
	 * @param opID
	 * @param type
	 *            0:normal 1:force
	 * @return
	 */
	public boolean batch_control_ds(List ds_group, String opID, int type) {
		boolean ret = false;
		for (Iterator it = ds_group.iterator(); it.hasNext();) {
			if (!control_ds((String) it.next(), opID, type)) {
				ret = false;
				break;
			} else
				ret = true;
		}
		return ret;
	}

	/**
	 * @param cs_group
	 * @param ds_group
	 * @param opID
	 * @param type
	 *            0:normal 1:force
	 * @return
	 */
	public boolean control_cluster(List cs_group, List ds_group, String opID, int type) {
		boolean ret = false;
		if (!batch_control_ds(ds_group, opID, type) || !batch_control_cs(cs_group, opID, type))
			ret = false;
		else
			ret = true;
		return ret;
	}
	
	public boolean control_iv(String iv, String opID, int type) {
		log.debug("control iv:" + iv + " " + opID + " type=" + type);
		boolean ret = false;
		String cmd = "cd " + FailOverBaseCase.iv_bin + " && ./tair.sh " + opID + "_iv ";
		int expectNum = 0;
		if (opID.equals(FailOverBaseCase.stop)) {
			expectNum = 2;
		} else if (opID.equals(FailOverBaseCase.start)) {
			expectNum = 3;
		}
		executeShell(stafhandle, iv, cmd);

		if (opID.equals(FailOverBaseCase.stop) && type == 1)
			cmd = "killall -9 inval_server && sleep 2";
		STAFResult result = executeShell(stafhandle, iv, cmd);
		int waittime = 0;
		cmd = "ps -ef|grep inval_server|wc -l";
        while (waittime < 110) {
			result = executeShell(stafhandle, iv, cmd);
			if (result.rc != 0) {
				log.debug("iv rc!=0");
				ret = false;
			} else {
				String stdout = getShellOutput(result);
				if ((new Integer(stdout.trim())).intValue() == expectNum) {

					log.debug("------------iv ps result--------------" + stdout);
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
	
	public boolean clean_iv_data(String machine) {
		boolean ret = false;
		String cmd = "cd " + FailOverBaseCase.iv_bin + " && ./tair.sh clean";
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
	
	public boolean clean_tool(String machine, String path) {
		boolean ret = false;
		killall_tool_proc();
		String cmd = "cd " + path + " && ";
		cmd += "./clean.sh";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}
	
	public boolean clean_bin_tool(String machine) {
		boolean ret = false;
		killall_tool_proc();
		String cmd = "cd " + FailOverBaseCase.tair_bin + "tools/" + " && ";
		cmd += "./clean.sh";
		STAFResult rst = executeShell(stafhandle, machine, cmd);
		if (rst.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean execute_data_verify_tool() {
		log.debug("start verify tool,run batchData");
		boolean ret = false;
		String cmd = "cd " + test_bin + " && ";
		cmd += "./batchData.sh";
		STAFResult result = executeShell(stafhandle, "local", cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}
	
	public boolean execute_tair_tool(String machine, String path) {
		log.debug("start verify tool,run batchData");
		boolean ret = false;
		String cmd = "cd " + path + " && ";
		cmd += "./batchData.sh";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean execute_stress_tool(int cnt) {
		log.debug("start stress tool");
		boolean ret = false;
		String cmd = "cd " + test_bin + " && ";
		cmd += "./debug.sh " + cnt;
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
		String cmd = "killall -9 tair3test";
		STAFResult result = executeShell(stafhandle, "local", cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}

	public boolean batch_clean_data(List machines) {
		boolean ret = true;
		for (Iterator it = machines.iterator(); it.hasNext();) {
			if (!clean_data((String) it.next()))
				ret = false;
		}
		return ret;
	}
	
	public boolean batch_clean_iv_data(List machines) {
		boolean ret = true;
		for (Iterator it = machines.iterator(); it.hasNext();) {
			if (!clean_iv_data((String) it.next()))
				ret = false;
		}
		return ret;
	}

	public boolean reset_cluster(List csList, List dsList) {
		boolean ret = false;
		log.debug("stop and clean cluster!");
		if (control_cluster(csList, dsList, FailOverBaseCase.stop, 1) && batch_clean_data(csList) && batch_clean_data(dsList))
			ret = true;
		return ret;
	}
	
	public boolean reset_inval_cluster(List iv_List) {
		boolean ret = false;
		log.debug("stop and clean invalid server!");
		if (batch_control_iv(ivList, FailOverBaseCase.stop, 1) && batch_clean_iv_data(ivList))
			ret = true;
		return ret;
	}
	
	public boolean batch_control_iv(List iv_group, String opID, int type) {
		boolean ret = false;
		for (Iterator it = iv_group.iterator(); it.hasNext();) {
			if (!control_iv((String) it.next(), opID, type)) {
				ret = false;
				break;
			} else
				ret = true;
		}
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
		log.debug("check keyword:" + cmd + " on " + machine + " in file:" + logfile);
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
	public boolean modify_config_file(String machine, String confname, String key, String value) {
		log.debug("change file:" + confname + " on " + machine + " key=" + key + " value=" + value);
		boolean ret = false;
		String cmd = "sed -i \"s/" + key + "=.*$/" + key + "=" + value + "/\" " + confname + " && grep \"" + key + "=.*$\" " + confname + "|awk -F\"=\" \'{print $2}\'";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			String stdout = getShellOutput(result);
			if (stdout.trim().equals(value))
				ret = true;
			else {
                log.error(stdout.trim());
				ret = false;
            }
		}
		return ret;
	}
	
	public boolean modify_key(String machine, String confname, String old, String now) {
		log.debug("change key:" + confname + " on " + machine + " old=" + old + " now=" + now);
		boolean ret = false;
		String cmd = "sed -i \"s/" + old + ".*$/" + now + "/\" " + confname
				+ " && grep \"" + now + "\" " + confname;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}
	
	public boolean modify_group_name(String machine, String confname, String old, String now) {
		log.debug("change group name:" + confname + " on " + machine + " old=" + old + " now=" + now);
		boolean ret = false;
		String cmd = "sed -i \"s/" + old + ".*$/" + now + "]/\" " + confname
				+ " && grep \"" + now + "\" " + confname;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else
			ret = true;
		return ret;
	}
	///////////////////////////////////////////////////////////////////////////////////////////////
	public boolean batch_modify(List machines, String confname, String key, String value) {
		boolean ret = true;
		for (Iterator it = machines.iterator(); it.hasNext();) {
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

	public boolean batch_recover_net(List machines) {
		boolean ret = true;
		for (Iterator it = machines.iterator(); it.hasNext();) {
			if (!recover_net((String)it.next())) {
				ret = false;
				break;
			}
		}
		return ret;
	}

	public boolean execute_tool_on_mac(String machine, String option) {
		log.debug("start " + option + " on " + machine);
		if(!modify_config_file(machine, FailOverBaseCase.tair_bin+"tools/DataDebug.conf", "actiontype", option))
			fail("modify configure file failed");
		if(!modify_config_file(machine, FailOverBaseCase.tair_bin+"tools/DataDebug.conf", "datasize", FailOverBaseCase.put_count))
			fail("modify configure file failed");
		if(!modify_config_file(machine, FailOverBaseCase.tair_bin+"tools/DataDebug.conf", "filename", "read.kv"))
			fail("modify configure file failed");

		boolean ret = true;
		String cmd = "cd " + FailOverBaseCase.tair_bin+ "tools/" + "&& ./batchData.sh";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		return ret;
	}

	public void wait_tool_on_mac(String machine)
	{
		log.error("start wait on " + machine);
		int count=0;
		while(check_process(machine, "DataDebug")!=2)
		{
			waitto(16);
			if(++count>150)break;
		}
		if(count>150)fail("wait time out on " + machine + "!");
		log.error("wait success on " + machine);
	}

	public boolean execute_clean_on_cs(String machine) {
		log.debug("clean kv and log files on " + machine);
		boolean ret = true;
		String cmd = "cd " + FailOverBaseCase.tair_bin + "tools/" + "&& ./clean.sh";
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		return ret;
	}

	public void wait_keyword_equal(String machine, String keyword, int expect_count)
	{
		int count=0;
		while(check_keyword(machine, keyword, FailOverBaseCase.tair_bin+"logs/config.log")!=expect_count)
		{
			waitto(2);
			if(++count>10)break;
		}
		if(count>10)fail("wish the count of " + keyword + " on " + machine + " not change, but changed!");
		log.error("the count of " + keyword + " on " + machine + " is right!");
	}

	public void wait_keyword_change(String machine, String keyword, int base_count)
	{
		int count=0;
		while(check_keyword(machine, keyword, FailOverBaseCase.tair_bin+"logs/config.log")<=base_count)
		{
			waitto(2);
			if(++count>10)break;
		}
		if(count>10)fail("wish the count of " + keyword + " on " + machine + " change, but not changed!");
		log.error("the count of " + keyword + " on " + machine + " is right!");
	}

	protected int getVerifySuccessful(String machine) {
		int ret = 0;
		String verify = "tail -10 " + FailOverBaseCase.tair_bin + "tools/" + "datadbg0.log|grep \"Successful\"|awk -F\" \" \'{print $3}\'";
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

	public boolean scp_file(String source_machine, String source_path,
			String filename, String target_machine, String target_path) {
		log.error("scp " + filename + " from " + source_machine + " "
				+ source_path + " to " + target_machine + " " + target_path);
		boolean ret = true;
		String cmd = "scp " + source_path + filename + " admin@"
				+ target_machine + ":" + target_path;
		STAFResult result = executeShell(stafhandle, source_machine, cmd);
		if (result.rc != 0)
			ret = false;
		return ret;
	}
	
//	public boolean control_netcut_sh(String machine, String opID) {
//		log.error("Copy " + opID + " files on " + machine);
//		boolean ret = true;
//		String cmd = "cd " + FailOverBaseCase.test_bin + " && ./shift.sh " + opID;
//		STAFResult result = executeShell(stafhandle, machine, cmd);
//		if (result.rc != 0)
//			ret = false;
//		return ret;
//	}
	////////////////////////////////////////////////////////////////////////////////////////////////////
	public boolean comment_line(String machine, String file, String keyword, String comment) {
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

	public boolean uncomment_line(String machine, String file, String keyword, String comment) {
		boolean ret = false;
		String cmd = "sed -i \'s/" + comment + "*\\(.*" + keyword + ".*$\\)/\\1/\' " + file;
		STAFResult result = executeShell(stafhandle, machine, cmd);
		if (result.rc != 0)
			ret = false;
		else {
			// String stdout=getShellOutput(result);
			ret = true;
		}
		return ret;
	}

	public boolean batch_uncomment(List machines, String confname, List keywords, String comment) {
		boolean ret = true;
		for (Iterator ms = machines.iterator(); ms.hasNext();) {
			String cms = (String) ms.next();
			for (Iterator ks = keywords.iterator(); ks.hasNext();) {
				if (!uncomment_line(cms, confname, (String) ks.next(), comment))
					ret = false;
			}
		}
		return ret;
	}

	/**
	 * @return successful count
	 */
	protected int getVerifySuccessful() {
		int ret = 0;
		String verify = "cd " + FailOverBaseCase.test_bin + " && ";
//		verify += " tail -10 datadbg0.log|grep \"Successful\"|awk -F\" \" \'{print $3}\'";
		verify += " grep \"Successful\" datadbg0.log |awk \'{print $7}\' |tail -1";
		log.debug("do verify on local");
		STAFResult result = executeShell(stafhandle, "local", verify);
		if (result.rc != 0)
		{
			log.debug("get result "+result );
			ret = -1;
		}
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
	
	protected int getVerifySuccessful(String machine, String path) {
		int ret = 0;
		String verify = "cd " + path + " && ";
		verify += " grep \"Successful\" datadbg0.log |awk \'{print $7}\' |tail -1";
		log.debug("do verify on local");
		STAFResult result = executeShell(stafhandle, machine, verify);
		if (result.rc != 0)
		{
			log.debug("get result "+result );
			ret = -1;
		}
		else {
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
			} catch (Exception e) {
				log.debug("get verify exception: " + stdout);
				ret = -1;
			}
		}
		log.error("Success count: " + ret);
		return ret;
	}

	/**
	 * @return successful count
	 */
	protected int getDSFailNum(String dsName) {
		int ret = 0;
		String verify = "cd " + FailOverBaseCase.test_bin + " && ";
		verify += "cat datadbg0.log|grep \"get failure: " + dsName + "\"|wc -l";
		STAFResult result = executeShell(stafhandle, "local", verify);
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
	
}
