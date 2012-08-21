/**
 * 
 */
package com.taobao.uic;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailOverUicTest1 extends FailOverBaseCase {

	@Test
	public void testFailover_01_restart_one_ds_on_group_A() {
		log.info("start uic failover test case 01");
		startCluster();
		int[] dataArray = prepareData();

		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(20);

//		int versionCount = check_keyword(csList1.get(0), finish_rebuild,
//				tair_bin + "logs/config.log");

		// close 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), stop, 1));
		log.info("1 ds in cluster A has been closed!");
		waitto(ds_down_time);

		// check rebuild not happened
//		assertEquals(check_keyword(csList1.get(0), finish_rebuild, tair_bin
//						+ "logs/config.log"), versionCount);
//		log.info("check rebuild not occurred while shut down 1 ds in cluster A!");

		// check tmp_down_server
		assertEquals(dsList1.get(0) + ":" + dsport + ";",
				getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " correct!");

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_A_1DS);
		waitToolRecover(clientList.get(1), RECOVER_B_1DS);

		// restart 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), start, 1));
		log.info("1 ds in cluster A has been restarted!");
		waitto(down_time);

		// check rebuild not happened
//		assertEquals(check_keyword(csList1.get(0), finish_rebuild, tair_bin
//						+ "logs/config.log"), versionCount);
//		log.info("check rebuild not occurred while restart 1 ds in cluster A!");

		// check tmp_down_server
		assertEquals(dsList1.get(0) + ":" + dsport + ";",
				getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " correct!");

		// check network flow
		assertTrue("network flow on " + dsList1.get(0) + " not with expectations!",
				checkNetworkFlow(dsList1.get(0)) < flow_low);

		// reset ds
		assertEquals("successful", resetServer(csList1.get(0), resetserver));
		assertEquals("", getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " were cleared!");
//		assertEquals(check_keyword(csList1.get(0), finish_rebuild, tair_bin
//						+ "logs/config.log"), versionCount);
		waitto(5);
		
		// reset client
		batchSendSignal(clientList, toolname, SIGUSR2);
		batchSendSignal(ycsbList, ycsbname, SIGUSR2);

		// check network flow recover
		waitToolRecover(clientList.get(0), RECOVER_ALL);
		waitToolRecover(clientList.get(1), RECOVER_ALL);
		
		// check network flow
		assertTrue("network flow on " + dsList1.get(0) + " not with expectations!",
				checkNetworkFlow(dsList1.get(0)) > flow_high);

		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));

		// check data
		verifyData(dataArray);
		log.info("end uic failover test case 01");
	}

	@Test
	public void testFailover_02_backup_ds() {
		log.info("start uic failover test case 02");
		startCluster();
		String calcCmd = "calc AOld.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		int[] dataArray = prepareData();

		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(20);

//		int versionCount = check_keyword(csList1.get(0), finish_rebuild,
//				tair_bin + "logs/config.log");

		// close 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), stop, 1));
		log.info("1 ds in cluster A has been closed!");
		waitto(ds_down_time);

		// check rebuild not happened
//		assertEquals(check_keyword(csList1.get(0), finish_rebuild, tair_bin
//						+ "logs/config.log"), versionCount);
//		log.info("check rebuild not occurred while shut down 1 ds in cluster A!");

		// check tmp_down_server
		assertEquals(dsList1.get(0) + ":" + dsport + ";",
				getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " correct!");

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_A_1DS);
		waitToolRecover(clientList.get(1), RECOVER_B_1DS);

		// start backup ds then add it into cluster A
		assertTrue(control_ds(addList.get(0), start, 0));
		log.info("backup ds has been started!");
		modify_file(csList1.get(0), tair_bin + groupconf, "_server_list="
				+ dsList1.get(0) + ":" + dsport, "_server_list=" + addList.get(0)
				+ ":" + dsport);
		waitto(down_time);
		
		// check network flow
		assertTrue("network flow on " + dsList1.get(0) + " not with expectations!",
				checkNetworkFlow(dsList1.get(0)) < flow_low);

		// check tmp_down_server
//		assertEquals("", getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
//		if (check_keyword(csList1.get(0), finish_rebuild, tair_bin
//				+ "logs/config.log") == versionCount)
//			fail("rebuild not occurred while backup ds start into cluster A!");
//		waitto(5);
		
		// client need reset?
		// reset client
//		batchSendSignal(clientList, toolname, SIGUSR2);
//		batchSendSignal(ycsbList, ycsbname, SIGUSR2);

		// check group table not changed
		calcCmd = "calc ANew.group";
		if(!"successful".equals(controlGroupCalc(csList1.get(0), calcCmd)))
			fail("failed to invoke calc shell with cmd: " + calcCmd);
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		calcCmd = "backds AOld.group ANew.group";
		if(Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd)) != 0)
			fail("group table changed after use backup ds!");
		log.info("group table not changed after use backup ds!");
		
		// check network flow on backup ds
		int backup_ds_flow = checkNetworkFlow(addList.get(0));
		int A_ds2_flow = checkNetworkFlow(dsList1.get(1));
		if(backup_ds_flow < 0.5 * A_ds2_flow || backup_ds_flow > 1.5 * A_ds2_flow)
			fail("backup ds's network flow not near other ds in group A!");
		log.info("check backup ds's network flow near other ds in group A!");
		
		// check tool recover
		waitToolRecover(clientList.get(0), RECOVER_ALL);
		waitToolRecover(clientList.get(1), RECOVER_ALL);

		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));

		// check data
		verifyData((int)(dataArray[0] * 0.6), false, dataArray[1], true);
		log.info("end uic failover test case 02");
	}
	
	@Test
	public void testFailover_03_bucket_placement_flag_1() {
		log.info("start uic failover test case 03");
		startCluster();
		String calcCmd = "checkAB";
		int checkDif = Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd));
		assertTrue(checkDif > 500);
		log.info("check bucket table on A and B not equals when set _bucket_placement_flag 1: "
				+ checkDif);
		calcCmd = "calc AOld.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("calc bucket table on A successful!");
		int[] dataArray = prepareData();

		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(20);

//		int versionCount = check_keyword(csList1.get(0), finish_rebuild,
//				tair_bin + "logs/config.log");
		
		// remember network flow on every ds
		int OldFlowA1 = checkNetworkFlow(dsList1.get(0));
		int OldFlowA2 = checkNetworkFlow(dsList1.get(1));
		int OldFlowA3 = checkNetworkFlow(dsList1.get(2));
		int OldFlowB1 = checkNetworkFlow(dsList2.get(0));
		int OldFlowB2 = checkNetworkFlow(dsList2.get(1));
		int OldFlowB3 = checkNetworkFlow(dsList2.get(2));

		// close 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), stop, 1));
		log.info("1 ds in cluster A has been closed!");
		waitto(ds_down_time);

		// check tmp_down_server
		assertEquals(dsList1.get(0) + ":" + dsport + ";",
				getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " correct!");

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_A_1DS);
		waitToolRecover(clientList.get(1), RECOVER_B_1DS);
		
		int NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		int NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		int NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		int NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		int NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		int NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("network flow on " + dsList1.get(0)
				+ " not with expectations!", NowFlowA1 < flow_low);
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1, OldFlowA1 > NowFlowA1 * 10);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 > OldFlowA2 && NowFlowA2 < NowFlowB2 );
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 > OldFlowA3 && NowFlowA3 < NowFlowB3 );
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 < OldFlowB1 * 1.5 && NowFlowB1 > OldFlowB1 * 1.1);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 < OldFlowB2 * 1.5 && NowFlowB2 > OldFlowB2 * 1.1);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 < OldFlowB3 * 1.5 &&  NowFlowB3> OldFlowB3 * 1.1);
		assertTrue("NowFlowB1 not near NowFlowB2!", NowFlowB1 < NowFlowB2 * 1.1
				&& NowFlowB2 < NowFlowB1 * 1.1);
		assertTrue("NowFlowB2 not near NowFlowB3!", NowFlowB2 < NowFlowB3 * 1.1
				&& NowFlowB3 < NowFlowB2 * 1.1);

		// restart 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), start, 0));
		log.info("1 ds in cluster A has been restarted!");
		waitto(down_time);

		// check network flow
		assertTrue("network flow on " + dsList1.get(0) + " not with expectations!",
				checkNetworkFlow(dsList1.get(0)) < flow_low);

		// reset ds
		assertEquals("successful", resetServer(csList1.get(0), resetserver));
		assertEquals("", getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
//		assertEquals(check_keyword(csList1.get(0), finish_rebuild, tair_bin
//						+ "logs/config.log"), versionCount);
		waitto(5);
		
		// reset client
		batchSendSignal(clientList, toolname, SIGUSR2);
		batchSendSignal(ycsbList, ycsbname, SIGUSR2);
		
		// check table not changed
		calcCmd = "calc ANew.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		calcCmd = "compare AOld.group ANew.group";
		assertEquals(0, Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd)));
		log.info("check bucket table not changed in group A!");

		// check network flow recover
		waitto(10);
		NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1,
				NowFlowA1 < OldFlowA1 * 1.1 && OldFlowA1 < NowFlowA1 * 1.1);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 < OldFlowA2 * 1.1 && OldFlowA2 < NowFlowA2 * 1.1);
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 < OldFlowA3 * 1.1 && OldFlowA3 < NowFlowA3 * 1.1);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 < OldFlowB1 * 1.1 && OldFlowB1 < NowFlowB1 * 1.1);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 < OldFlowB2 * 1.1 && OldFlowB2 < NowFlowB2 * 1.1);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 < OldFlowB3 * 1.1 && OldFlowB3 < NowFlowB3 * 1.1);

		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));

		// check data
		verifyData(dataArray);
		log.info("end uic failover test case 03");
	}
	
	@Test
	public void testFailover_04_bucket_placement_flag_0() {
		log.info("start uic failover test case 04");
		if (!batch_modify(csList1, tair_bin + groupconf, _bucket_placement_flag, "0"))
			fail("modify configure file failure");
		if (!batch_modify(csList2, tair_bin + groupconf, _bucket_placement_flag, "0"))
			fail("modify configure file failure");
		startCluster();
		String calcCmd = "checkAB";
		assertEquals(0, Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd)));
		log.info("check bucket table on A and B equals when set _bucket_placement_flag 0!");
		calcCmd = "calc AOld.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("calc bucket table on A successful!");
		int[] dataArray = prepareData();

		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(20);

//		int versionCount = check_keyword(csList1.get(0), finish_rebuild,
//				tair_bin + "logs/config.log");

		// remember network flow on every ds
		int OldFlowA1 = checkNetworkFlow(dsList1.get(0));
		int OldFlowA2 = checkNetworkFlow(dsList1.get(1));
		int OldFlowA3 = checkNetworkFlow(dsList1.get(2));
		int OldFlowB1 = checkNetworkFlow(dsList2.get(0));
		int OldFlowB2 = checkNetworkFlow(dsList2.get(1));
		int OldFlowB3 = checkNetworkFlow(dsList2.get(2));

		// close 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), stop, 1));
		log.info("1 ds in cluster A has been closed!");
		waitto(ds_down_time);

		// check tmp_down_server
		assertEquals(dsList1.get(0) + ":" + dsport + ";",
				getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " correct!");

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_A_1DS);
		waitToolRecover(clientList.get(1), RECOVER_B_1DS);

		int NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		int NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		int NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		int NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		int NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		int NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("network flow on " + dsList1.get(0)
				+ " not with expectations!", NowFlowA1 < flow_low);
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1,
				OldFlowA1 > NowFlowA1 * 10);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 > OldFlowA2 && NowFlowA2 < NowFlowB2);
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 > OldFlowA3 && NowFlowA3 < NowFlowB3);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 > OldFlowB1 * 1.3);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 > OldFlowB2);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 > OldFlowB3);
		assertTrue("NowFlowA2 not near NowFlowB2!", NowFlowA2 < NowFlowB2 * 1.1
				&& NowFlowB2 < NowFlowA2 * 1.1);
		assertTrue("NowFlowA3 not near NowFlowB3!", NowFlowA3 < NowFlowB3 * 1.1
				&& NowFlowB3 < NowFlowA3 * 1.1);
		assertTrue("NowFlowB2 not near NowFlowB2!", NowFlowB2 < NowFlowB3 * 1.1
				&& NowFlowB3 < NowFlowB2 * 1.1);
		assertTrue("NowFlowB1 not bigger than NowFlowB2!",
				NowFlowB1 > NowFlowB2 * 1.1);

		// restart 1 ds in cluster A
		assertTrue(control_ds(dsList1.get(0), start, 0));
		log.info("1 ds in cluster A has been restarted!");
		waitto(down_time);

		// check network flow
		assertTrue("network flow on " + dsList1.get(0) + " not with expectations!",
				checkNetworkFlow(dsList1.get(0)) < flow_low);

		// reset ds
		assertEquals("successful", resetServer(csList1.get(0), resetserver));
		assertEquals("", getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
//		assertEquals(check_keyword(csList1.get(0), finish_rebuild, tair_bin
//						+ "logs/config.log"), versionCount);
		waitto(5);
		
		// reset client
		batchSendSignal(clientList, toolname, SIGUSR2);
		batchSendSignal(ycsbList, ycsbname, SIGUSR2);

		// check table not changed
		calcCmd = "calc ANew.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		calcCmd = "compare AOld.group ANew.group";
		assertEquals(0,	Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd)));
		log.info("check bucket table not changed in group A!");

		// check network flow recover
		waitto(10);
		NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1,
				NowFlowA1 < OldFlowA1 * 1.1 && OldFlowA1 < NowFlowA1 * 1.1);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 < OldFlowA2 * 1.1 && OldFlowA2 < NowFlowA2 * 1.1);
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 < OldFlowA3 * 1.1 && OldFlowA3 < NowFlowA3 * 1.1);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 < OldFlowB1 * 1.1 && OldFlowB1 < NowFlowB1 * 1.1);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 < OldFlowB2 * 1.1 && OldFlowB2 < NowFlowB2 * 1.1);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 < OldFlowB3 * 1.1 && OldFlowB3 < NowFlowB3 * 1.1);

		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));

		// check data
		verifyData(dataArray);
		log.info("end uic failover test case 04");
	}
	
	@Test
	public void testFailover_05_restart_group_A() {
		log.info("start uic failover test case 05");
		startCluster();
		String calcCmd = "calc AOld.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		int[] dataArray = prepareData();

		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(40);

		// remember network flow on every ds
		int OldFlowA1 = checkNetworkFlow(dsList1.get(0));
		int OldFlowA2 = checkNetworkFlow(dsList1.get(1));
		int OldFlowA3 = checkNetworkFlow(dsList1.get(2));
		int OldFlowB1 = checkNetworkFlow(dsList2.get(0));
		int OldFlowB2 = checkNetworkFlow(dsList2.get(1));
		int OldFlowB3 = checkNetworkFlow(dsList2.get(2));

		// close all ds on cluster A
		assertTrue(batch_control_ds(dsList1, stop, 1));
		log.info("all ds on cluster A has been closed!");
		waitto(ds_down_time);

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_A_1CLS);
		waitToolRecover(clientList.get(1), RECOVER_B_1CLS);
		
		int NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		int NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		int NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		int NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		int NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		int NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("network flow on " + dsList1.get(0)
				+ " not with expectations!", NowFlowA1 < flow_low);
		assertTrue("network flow on " + dsList1.get(1)
				+ " not with expectations!", NowFlowA2 < flow_low);
		assertTrue("network flow on " + dsList1.get(2)
				+ " not with expectations!", NowFlowA3 < flow_low);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 > OldFlowB1 * 1.3);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 > OldFlowB2 * 1.3);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 > OldFlowB3 * 1.3);
		assertTrue("NowFlowB1 not near NowFlowB2!", NowFlowB1 < NowFlowB2 * 1.1
				&& NowFlowB2 < NowFlowB1 * 1.1);
		assertTrue("NowFlowB2 not near NowFlowB3!", NowFlowB2 < NowFlowB3 * 1.1
				&& NowFlowB3 < NowFlowB2 * 1.1);

		// restart all ds on cluster A
		assertTrue(batch_control_ds(dsList1, start, 0));
		log.info("all ds on cluster A has been restarted!");
		waitto(down_time);
		
		// reset ds
		assertEquals("successful", resetServer(csList1.get(0), resetgroup));
		assertEquals("", getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " were cleared!");
		waitto(5);
		
		// check table not changed
		calcCmd = "calc ANew.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		calcCmd = "compare AOld.group ANew.group";
		assertEquals(0, Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd)));
		log.info("check bucket table not changed in group A!");
		
		// reset client
		batchSendSignal(clientList, toolname, SIGUSR2);
		batchSendSignal(ycsbList, ycsbname, SIGUSR2);

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_ALL);
		waitToolRecover(clientList.get(1), RECOVER_ALL);

		// check network flow recover
		waitto(10);
		NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1,
				NowFlowA1 < OldFlowA1 * 1.2 && OldFlowA1 < NowFlowA1 * 1.2);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 < OldFlowA2 * 1.2 && OldFlowA2 < NowFlowA2 * 1.2);
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 < OldFlowA3 * 1.2 && OldFlowA3 < NowFlowA3 * 1.2);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 < OldFlowB1 * 1.2 && OldFlowB1 < NowFlowB1 * 1.2);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 < OldFlowB2 * 1.2 && OldFlowB2 < NowFlowB2 * 1.2);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 < OldFlowB3 * 1.2 && OldFlowB3 < NowFlowB3 * 1.2);

		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));

		// check data
		verifyData(dataArray);
		log.info("end uic failover test case 05");
	}
	
	@Test
	public void testFailover_06_lazy_clear() {
		log.info("start uic failover test case 06");
		startCluster();
		prepareData();
		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(20);

		// remember network flow on every ds
		int OldFlowA1 = checkNetworkFlow(dsList1.get(0));
		int OldFlowA2 = checkNetworkFlow(dsList1.get(1));
		int OldFlowA3 = checkNetworkFlow(dsList1.get(2));
		int OldFlowB1 = checkNetworkFlow(dsList2.get(0));
		int OldFlowB2 = checkNetworkFlow(dsList2.get(1));
		int OldFlowB3 = checkNetworkFlow(dsList2.get(2));
		
		float oldYcsbRt = getReturnTime(ycsbList.get(0));

		// invoke lazy clear in cluster A
		assertEquals("removeArea: area:0,success", controlGroupCalc(csList1.get(0), "delall"));
		log.info("cluster A has been lazy cleared!");
		
		// wait tool recover
		waitToolRecover(clientList.get(0), RECOVER_ALL);
		waitToolRecover(clientList.get(1), RECOVER_ALL);
		log.info("check tool recover on every client!");
		
		// check ycsb return time
		waitto(20);
		float nowYcsbRt = getReturnTime(ycsbList.get(0));
		assertTrue(nowYcsbRt < oldYcsbRt * 1.5 && oldYcsbRt < nowYcsbRt * 1.5);

		// check network flow
		int NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		int NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		int NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		int NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		int NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		int NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1,
				NowFlowA1 < OldFlowA1 * 1.2 && OldFlowA1 < NowFlowA1 * 1.2);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 < OldFlowA2 * 1.2 && OldFlowA2 < NowFlowA2 * 1.2);
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 < OldFlowA3 * 1.2 && OldFlowA3 < NowFlowA3 * 1.2);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 < OldFlowB1 * 1.2 && OldFlowB1 < NowFlowB1 * 1.2);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 < OldFlowB2 * 1.2 && OldFlowB2 < NowFlowB2 * 1.2);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 < OldFlowB3 * 1.2 && OldFlowB3 < NowFlowB3 * 1.2);
		
		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));
		
		// check data on group A couldn't read out
		verifyData(new int[]{0, Integer.parseInt(put_count)});

		log.info("end uic failover test case 06");
	}
	
	@Test
	public void testFailover_07_dilatation() {
		log.info("start uic failover test case 07");
		startCluster();
		if (!batch_modify(csList1, tair_bin + groupconf,
				"_min_data_server_count", "4"))
			fail("modify configure file failure");
		if (!batch_modify(csList2, tair_bin + groupconf,
				"_min_data_server_count", "4"))
			fail("modify configure file failure");
		String calcCmd = "calc AOld.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		int[] dataArray = prepareData();

		startToolPutGet(clientList.get(0));
		startToolPutGet(clientList.get(1));
		batch_execute_ycsb_tool(ycsbList);
		waitto(20);
		
		// remember network flow on every ds
		int OldFlowA1 = checkNetworkFlow(dsList1.get(0));
		int OldFlowA2 = checkNetworkFlow(dsList1.get(1));
		int OldFlowA3 = checkNetworkFlow(dsList1.get(2));
		int OldFlowB1 = checkNetworkFlow(dsList2.get(0));
		int OldFlowB2 = checkNetworkFlow(dsList2.get(1));
		int OldFlowB3 = checkNetworkFlow(dsList2.get(2));

		// start all add ds
		assertTrue(batch_control_ds(addList, start, 0));
		log.info("all add ds have been started!");
		waitto(ds_down_time);
		
		// add all add ds into group.conf
		addToGroup(csList1.get(0), dsList1.get(2), addList.get(0));
		addToGroup(csList2.get(0), dsList2.get(2), addList.get(1));
		
		// wait for migrate finish
		int waitcnt = 0;
		while (check_keyword(csList1.get(0), finish_migrate, tair_bin
				+ "logs/config.log") != 1) {
			log.debug("check if migration finish on cs " + csList1.get(0));
			waitto(10);
			if (++waitcnt > 60)
				break;
		}
		if (waitcnt > 60)
			fail("down time arrived,but no migration finished!");
		waitcnt = 0;
		log.info("down time arrived,migration finished!");

		// check all group table
		calcCmd = "checkAB";
		int checkDif = Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd));
		assertTrue(checkDif > 1000);
		log.info("check bucket table on A and B not equals: " + checkDif);
		calcCmd = "calc ANow.group";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("invoke calc shell with cmd: " + calcCmd + " successful!");
		calcCmd = "compare AOld.group ANow.group";
		checkDif = Integer.parseInt(controlGroupCalc(csList1.get(0), calcCmd));
		assertTrue(checkDif > 1000);
		log.info("check bucket table on A not equals: " + checkDif);
		
		// check tmp_down_server
		assertEquals("", getKeyword(csList1.get(0), tmp_down_server, tair_bin + groupconf));
		log.info("check tmp_down_server on " + csList1.get(0) + " correct!");
		waitto(10);

		// check if tool recover
		waitToolRecover(clientList.get(0), RECOVER_ALL);
		waitToolRecover(clientList.get(1), RECOVER_ALL);
		
		// check network flow
		int NowFlowA1 = checkNetworkFlow(dsList1.get(0));
		int NowFlowA2 = checkNetworkFlow(dsList1.get(1));
		int NowFlowA3 = checkNetworkFlow(dsList1.get(2));
		int NowFlowA4 = checkNetworkFlow(addList.get(0));
		int NowFlowB1 = checkNetworkFlow(dsList2.get(0));
		int NowFlowB2 = checkNetworkFlow(dsList2.get(1));
		int NowFlowB3 = checkNetworkFlow(dsList2.get(2));
		int NowFlowB4 = checkNetworkFlow(addList.get(1));
		assertTrue("OldFlowA1:" + OldFlowA1 + " NowFlowA1:" + NowFlowA1,
				NowFlowA1 < OldFlowA1 && NowFlowA1 > OldFlowA1 * 0.5);
		assertTrue("OldFlowA2:" + OldFlowA2 + " NowFlowA2:" + NowFlowA2,
				NowFlowA2 < OldFlowA2 && NowFlowA2 > OldFlowA2 * 0.5);
		assertTrue("OldFlowA3:" + OldFlowA3 + " NowFlowA3:" + NowFlowA3,
				NowFlowA3 < OldFlowA3 && NowFlowA3 > OldFlowA3 * 0.5);
		assertTrue("OldFlowB1:" + OldFlowB1 + " NowFlowB1:" + NowFlowB1,
				NowFlowB1 < OldFlowB1 && NowFlowB1 > OldFlowB1 * 0.5);
		assertTrue("OldFlowB2:" + OldFlowB2 + " NowFlowB2:" + NowFlowB2,
				NowFlowB2 < OldFlowB2 && NowFlowB2 > OldFlowB2 * 0.5);
		assertTrue("OldFlowB3:" + OldFlowB3 + " NowFlowB3:" + NowFlowB3,
				NowFlowB3 < OldFlowB3 && NowFlowB3 > OldFlowB3 * 0.5);
		assertTrue("NowFlowA1:" + NowFlowA1 + " NowFlowA2:" + NowFlowA2,
				NowFlowA1 < NowFlowA2 * 1.5 && NowFlowA2 < NowFlowA1 * 1.5);
		assertTrue("NowFlowA2:" + NowFlowA2 + " NowFlowA3:" + NowFlowA3,
				NowFlowA2 < NowFlowA3 * 1.2 && NowFlowA3 < NowFlowA2 * 1.2);
		assertTrue("NowFlowA3:" + NowFlowA3 + " NowFlowA4:" + NowFlowA4,
				NowFlowA3 < NowFlowA4 * 1.2 && NowFlowA4 < NowFlowA3 * 1.2);
		assertTrue("NowFlowB1:" + NowFlowB1 + " NowFlowB2:" + NowFlowB2,
				NowFlowB1 < NowFlowB2 * 1.2 && NowFlowB2 < NowFlowB1 * 1.2);
		assertTrue("NowFlowB2:" + NowFlowB2 + " NowFlowB3:" + NowFlowB3,
				NowFlowB2 < NowFlowB3 * 1.2 && NowFlowB3 < NowFlowB2 * 1.2);
		assertTrue("NowFlowB3:" + NowFlowB3 + " NowFlowB4:" + NowFlowB4,
				NowFlowB3 < NowFlowB4 * 1.2 && NowFlowB4 < NowFlowB3 * 1.2);

		// stop all putget tool on client
		killall_tool_proc(clientList.get(0));
		killall_tool_proc(clientList.get(1));

		// check data
		verifyData(dataArray);
		log.info("end uic failover test case 07");
	}

	@Before
	public void setUp() {
		log.info("clean tool and cluster while setUp!");
		batch_clean_tool(clientList);
		batch_clean_ycsb(ycsbList);
		reset_cluster(csList1, dsList1);
		reset_cluster(csList2, dsList2);
		batch_control_ds(addList, stop, 1);
		batch_clean_data(addList);
		if (!batch_modify(csList1, tair_bin + groupconf,
				"_min_data_server_count", "3"))
			fail("modify configure file failure");
		if (!batch_modify(csList2, tair_bin + groupconf,
				"_min_data_server_count", "3"))
			fail("modify configure file failure");
		if (!batch_modify(csList1, tair_bin + groupconf, tmp_down_server, " "))
			fail("modify configure file failure");
		if (!batch_modify(csList2, tair_bin + groupconf, tmp_down_server, " "))
			fail("modify configure file failure");
		if (!batch_modify(csList1, tair_bin + groupconf, _pre_load_flag, "1"))
			fail("modify configure file failure");
		if (!batch_modify(csList2, tair_bin + groupconf, _pre_load_flag, "1"))
			fail("modify configure file failure");
		if (!batch_modify(csList1, tair_bin + groupconf, _bucket_placement_flag, "1"))
			fail("modify configure file failure");
		if (!batch_modify(csList2, tair_bin + groupconf, _bucket_placement_flag, "1"))
			fail("modify configure file failure");
		if(!"successful".equals( controlGroupCalc(csList1.get(0), "restore")))
			fail("invoke calc shell with cmd: restore failed on " + csList1.get(0) + "!");
		if(!"successful".equals( controlGroupCalc(csList2.get(0), "restore")))
			fail("invoke calc shell with cmd: restore failed on " + csList2.get(0) + "!");
	}

	@After
	public void tearDown() {
		log.info("clean tool and cluster while tearDown!");
		batch_clean_tool(clientList);
		batch_clean_ycsb(ycsbList);
		reset_cluster(csList1, dsList1);
		reset_cluster(csList2, dsList2);
		batch_control_ds(addList, stop, 1);
		batch_clean_data(addList);
	}
}

