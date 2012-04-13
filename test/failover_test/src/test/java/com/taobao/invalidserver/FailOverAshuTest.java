package com.taobao.invalidserver;

import org.junit.Test;
import com.ibm.staf.STAFResult;

/**
 * @author ashu.cs
 * 
 */

public class FailOverAshuTest extends FailOverBaseCase {

/*    @Test
    public void test_staf() {
        log.error("start staf test");

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

        // recover net
        // recover net between client and the invalid server
        if (!recover_net(ivList.get(1)))
            fail("recover net between iv 1 and all cluster failure!");
        log.error("recover net between iv 1 and all cluster success!");

        // end test
        log.error("end staf test case");
    }*/

	public void changeToolConf(String machine, String path, String value) {
		// change test tool's configuration
		if (!modify_config_file(machine, path + "DataDebug.conf", "actiontype",
				value))
			fail("modify configure file failure");
		if (!modify_config_file(machine, path + "DataDebug.conf", "datasize",
				FailOverBaseCase.put_count))
			fail("modify configure file failure");
	}

	public void changeToolConf(String machine, String path, String key,
			String value) {
		if (!modify_config_file(machine, path + "DataDebug.conf", key, value))
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
	public void testFailover_01_change_group_conf_to_wrong() {
		log.error("start group test Failover case 01");
		if (!control_cluster(csList1, dsList1, start, 0))
			fail("start cluster A failure!");
		log.error("start cluster A & B successful!");
		waitto(5);

		changeToolConf(clList.get(0), test_bin2, "actiontype", "put");
		changeToolConf(clList.get(0), test_bin2, "datasize", put_count);
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 1);
		log.error("put data over!");
		int put_count = getVerifySuccessful(clList.get(0), test_bin2);
		assertTrue("put successful rate smaller than 100%!", put_count
				/ put_count_float > 0.99);

		// change group.conf
		if(!modify_group_name(csList1.get(0), tair_bin + "etc/group.conf", "group_1", "group_2"))
			fail("change group name failed!");
		if(!modify_key(csList1.get(0), tair_bin + "etc/group.conf", "10.232.4.14", "10.232.4.28:5161"))
			fail("change ds ip 1 failed!");
		if(!modify_key(csList1.get(0), tair_bin + "etc/group.conf", "10.232.4.15", "10.232.4.29:5161"))
			fail("change ds ip 1 failed!");
		if(!modify_key(csList1.get(0), tair_bin + "etc/group.conf", "10.232.4.16", "10.232.4.30:5161"))
			fail("change ds ip 1 failed!");
		
		waitto(5);
		
		if(!control_cs(csList1.get(0), FailOverBaseCase.stop, 0))
			fail("close master cs failure!");
		log.error("master cs has been closed!");
		waitto(FailOverBaseCase.down_time);
		
		// change group.conf
		if(!modify_group_name(csList1.get(0), tair_bin + "etc/group.conf", "group_2", "group_1"))
			fail("change group name failed!");
		if(!modify_key(csList1.get(0), tair_bin + "etc/group.conf", "10.232.4.28", "10.232.4.14:5161"))
			fail("change ds ip 1 failed!");
		if(!modify_key(csList1.get(0), tair_bin + "etc/group.conf", "10.232.4.29", "10.232.4.15:5161"))
			fail("change ds ip 1 failed!");
		if(!modify_key(csList1.get(0), tair_bin + "etc/group.conf", "10.232.4.30", "10.232.4.16:5161"))
			fail("change ds ip 1 failed!");
		
		if(!control_cs(csList1.get(0), FailOverBaseCase.start, 0))
			fail("start master cs failure!");
		log.error("master cs has been restarted!");
		waitto(FailOverBaseCase.down_time);
		
		changeToolConf(clList.get(0), test_bin2, "actiontype", "get");
		execute_tair_tool(clList.get(0), test_bin2);
		waitForFinish(clList.get(0), test_bin2, 2);
		log.error("put data over!");
		int get_count = getVerifySuccessful(clList.get(0), test_bin2);
		assertTrue("put successful rate smaller than 100%!", get_count == put_count);
		log.error("Successfully Verified data!");
		
		log.error("end diamond test Failover case 01");
	}

	public void setUp() {
		log.error("clean tool and cluster while setUp!");
		clean_tool(clList.get(0), test_bin);
		clean_tool(clList.get(1), test_bin);
		clean_tool(clList.get(0), test_bin2);
		clean_tool(clList.get(1), test_bin2);
//		reset_inval_cluster(ivList);
		reset_cluster(csList1, dsList1);
		reset_cluster(csList2, dsList2);
        batch_recover_net(dsList1);
        batch_recover_net(dsList2);
        batch_recover_net(ivList);
		if (!batch_modify(csList1, FailOverBaseCase.tair_bin
				+ "etc/group.conf", "_copy_count", "2"))
			fail("modify configure file failure");
//		if (!batch_modify(csList2, FailOverBaseCase.tair_bin
//				+ "etc/group.conf", "_copy_count", "2"))
//			fail("modify configure file failure");
	}
    
	public void tearDown() {
		// clean_tool(clList.get(), test_bin);
		// clean_tool(clList.get(), test_bin);
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
//		batch_uncomment(csList2, FailOverBaseCase.tair_bin + "etc/group.conf",
//				dsList2, "#");
//		if (!modify_config_file(csList1.get(0), tair_bin + "etc/group.conf",
//				"invalidate_server", "10.232.4.20:5196,10.232.4.22:5196"))
//			fail("modify configure file failure");
//		if (!modify_config_file(csList2.get(0), tair_bin + "etc/group.conf",
//				"invalidate_server", "10.232.4.20:5196,10.232.4.22:5196"))
//			fail("modify configure file failure");
	}
}
