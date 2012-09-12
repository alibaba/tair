/**
 * 
 */
package com.taobao.ldbSync;

import static org.junit.Assert.*;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.taobao.uic.FailOverBaseCase;

public class FailOverSyncTest extends FailOverBaseCase {

	private void modify_copy_count(List<String> list1, List<String> list2,
			String value) {
		if (!batch_modify(list1, tair_bin + groupconf, copycount, value))
			fail("modify configure file failure");
		if (!batch_modify(list2, tair_bin + groupconf, copycount, value))
			fail("modify configure file failure");
	}

	private void controlAllCluster(List<String> cslist1, List<String> cslist2,
			List<String> dslist1, List<String> dslist2, String option,
			int forceFlag) {
		if (start.equals(option)) {
			batchControlServer(dslist1, ds, start, forceFlag);
			batchControlServer(dslist2, ds, start, forceFlag);
			waitto(3);
			batchControlServer(cslist1, cs, start, forceFlag);
			batchControlServer(cslist2, cs, start, forceFlag);
		} else if (stop.equals(option)) {
			batchControlServer(cslist1, cs, stop, forceFlag);
			batchControlServer(cslist2, cs, stop, forceFlag);
			batchControlServer(dslist1, ds, stop, forceFlag);
			batchControlServer(dslist2, ds, stop, forceFlag);
		}
	}

	private int execute_on_client(String machine, String actionType,
			boolean incrFirstFlag, int dataSize, String fileName,
			String fileName2, int startNum, int expectNum, boolean migrateFlag) {
		if (!modify_config_file(machine, test_bin + toolconf, filename2,
				fileName2))
			fail("modify configure file failure");
		return execute_on_client(machine, actionType, incrFirstFlag, dataSize,
				fileName, startNum, expectNum, migrateFlag);
	}

	private int execute_on_client(String machine, String actionType,
			boolean incrFirstFlag, int dataSize, String fileName, int startNum,
			int expectNum, boolean migrateFlag) {
		modifyToolConf(machine, actionType, incrFirstFlag, dataSize, fileName,
				startNum);
		execute_data_verify_tool(machine);
		waitToolFinish(machine);
		int sucCount = getToolResult(machine, test_bin, Successful);
		int failCount = getToolResult(machine, test_bin, Fail);
		int migCount = getToolResult(machine, test_bin, migBusyFail);
		if (!migrateFlag)
			assertTrue("successful count not with expected!",
					sucCount == expectNum);
		else {
			if (expectNum > 0) {
				int cntRideCount = 0;
				int valWrongCount = 0;
				if (incr.equals(actionType) || decr.equals(actionType)
						|| prefixIncr.equals(actionType)
						|| prefixDecr.equals(actionType)) {
					cntRideCount = getToolResult(machine, test_bin, cntRideFail);
					valWrongCount = getToolResult(machine, test_bin, valWrongFail);
				}
				assertTrue("successful rate not bigger than 95%!",
						sucCount > expectNum * 0.95);
				assertEquals("fail count not equals migBusyFail count!",
						failCount, migCount + cntRideCount + valWrongCount);
			} else {
				int noneCount = getToolResult(machine, test_bin, noneFail);
				int hiddenCount = getToolResult(machine, test_bin, hiddenFail);
				int valWrongCount = getToolResult(machine, test_bin,
						valWrongFail);
				int serWrongCount = getToolResult(machine, test_bin,
						serWrongFail);
				assertTrue("successful count not with expected!",
						sucCount < dataSize * 0.05);
				assertEquals("fail count not equals migBusyFail + noneFail!",
						failCount, noneCount + migCount + hiddenCount
								+ valWrongCount + serWrongCount);
			}
		}
		log.info(actionType + " data finish on " + machine + " !");
		return sucCount;
	}

	private void execute_on_client(List<String> machineList, String actionType,
			boolean incrFirstFlag, int dataSize, String fileName, int startNum,
			int expectNum, boolean isLocked, boolean migrateFlag) {
		execute_on_client(machineList, actionType, incrFirstFlag, dataSize,
				fileName, startNum, expectNum, migrateFlag);
		if (get.equals(actionType)) {
			int sucCount1 = getToolResult(machineList.get(0), test_bin,
					Successful);
			int lockedCount1 = getToolResult(machineList.get(0), test_bin,
					lockedNum);
			int sucCount2 = getToolResult(machineList.get(1), test_bin,
					Successful);
			int lockedCount2 = getToolResult(machineList.get(0), test_bin,
					lockedNum);
			if (!migrateFlag) {
				if (isLocked) {
					assertEquals("lockedCount1 not equals sucCount1!",
							sucCount1, lockedCount1);
					assertEquals("lockedCount2 not equals sucCount2!",
							sucCount2, lockedCount2);
				} else {
					assertEquals("lockedCount1 not equals 0!", 0, lockedCount1);
					assertEquals("lockedCount2 not equals 0!", 0, lockedCount2);
				}
			} else {
				if (isLocked) {
					assertTrue("locked rate not bigger than 95%!",
							lockedCount1 > expectNum * 0.95);
					assertTrue("locked rate not bigger than 95%!",
							lockedCount2 > expectNum * 0.95);
				} else {
					assertTrue("locked rate bigger than 5%!",
							lockedCount1 < expectNum * 0.05);
					assertTrue("locked rate bigger than 5%!",
							lockedCount2 < expectNum * 0.05);
				}
			}
		}
	}

	private void execute_on_client(List<String> machineList, String actionType,
			boolean incrFirstFlag, int dataSize, String fileName, int startNum,
			int expectNum, boolean migrateFlag) {
		modifyToolConf(machineList.get(0), actionType, incrFirstFlag, dataSize,
				fileName, startNum);
		modifyToolConf(machineList.get(1), actionType, incrFirstFlag, dataSize,
				fileName, startNum);
		execute_data_verify_tool(machineList.get(0));
		execute_data_verify_tool(machineList.get(1));
		waitToolFinish(machineList.get(0));
		waitToolFinish(machineList.get(1));
		int sucCount1 = getToolResult(machineList.get(0), test_bin, Successful);
		int failCount1 = getToolResult(machineList.get(0), test_bin, Fail);
		int migCount1 = getToolResult(machineList.get(0), test_bin, migBusyFail);
		int sucCount2 = getToolResult(machineList.get(1), test_bin, Successful);
		int failCount2 = getToolResult(machineList.get(1), test_bin, Fail);
		int migCount2 = getToolResult(machineList.get(1), test_bin, migBusyFail);
		if (!migrateFlag) {
			assertTrue("successful count not with expected!",
					sucCount1 == expectNum);
			assertTrue("successful count not with expected!",
					sucCount2 == expectNum);
		} else {
			if (expectNum > 0) {
				int noneCount2 = getToolResult(machineList.get(1), test_bin,
						noneFail);
				assertTrue("successful rate not bigger than 95%!",
						sucCount1 > expectNum * 0.95);
				assertEquals("fail count not equals migBusyFail count!",
						failCount1, migCount1);
				assertTrue("successful rate not bigger than 95%!",
						sucCount2 > expectNum * 0.95);
				assertEquals("fail count not with expected!", failCount2, migCount2 + noneCount2);
			} else {
				int noneCount1 = getToolResult(machineList.get(0), test_bin,
						noneFail);
				int hiddenCount1 = getToolResult(machineList.get(0), test_bin,
						hiddenFail);
				int valWrongCount1 = getToolResult(machineList.get(0),
						test_bin, valWrongFail);
				int serWrongCount1 = getToolResult(machineList.get(0),
						test_bin, serWrongFail);
				int noneCount2 = getToolResult(machineList.get(1), test_bin,
						noneFail);
				int hiddenCount2 = getToolResult(machineList.get(1), test_bin,
						hiddenFail);
				int valWrongCount2 = getToolResult(machineList.get(1),
						test_bin, valWrongFail);
				int serWrongCount2 = getToolResult(machineList.get(1),
						test_bin, serWrongFail);
				assertTrue("successful count not with expected!",
						sucCount1 < dataSize * 0.05);
				assertEquals("failCount1 not with expected!", failCount1,
						noneCount1 + migCount1 + hiddenCount1 + valWrongCount1
								+ serWrongCount1);
				// assertTrue(serWrongCount1 < );
				assertTrue("successful count not with expected!",
						sucCount2 < dataSize * 0.05);
				assertEquals("failCount2 not with expected!", failCount2,
						noneCount2 + migCount2 + hiddenCount2 + valWrongCount2
								+ serWrongCount2);
			}
		}

		log.info(actionType + " data finish on " + machineList.get(0) + " !");
		log.info(actionType + " data finish on " + machineList.get(1) + " !");
	}

	private void prepareServerEnvironment(int copyCount, int aAddDs, int bAddDs) {
		// start all cluster
		modify_copy_count(csList1, csList2, new Integer(copyCount).toString());
		if (aAddDs == 1) {
			if (!comment_line(csList1.get(0), tair_bin + groupconf,
					dsList1.get(0), "#"))
				fail("comment " + dsList1.get(0) + " failed!");
			controlAllCluster(csList1, csList2,
					dsList1.subList(1, dsList1.size()), dsList2, start, 0);
		} else if (bAddDs == 1) {
			if (!comment_line(csList2.get(0), tair_bin + groupconf,
					dsList2.get(0), "#"))
				fail("comment " + dsList2.get(0) + " failed!");
			controlAllCluster(csList1, csList2, dsList1,
					dsList2.subList(1, dsList2.size()), start, 0);
		} else if (aAddDs <= 0 && bAddDs <= 0) {
			controlAllCluster(csList1, csList2, dsList1, dsList2, start, 0);
		}
		waitto(down_time);

		// start stress
		String calcCmd = "flowlimit";
		assertEquals("successful", controlGroupCalc(csList1.get(0), calcCmd));
		log.info("set flowlimit on " + csList1.get(0) + " successful!");
		assertEquals("successful", controlGroupCalc(csList2.get(0), calcCmd));
		log.info("set flowlimit on " + csList2.get(0) + " successful!");
		waitto(2);
		batch_execute_ycsb_tool(ycsbList);
		waitto(down_time);

		if (aAddDs == -1)
			assertTrue("stop " + dsList1.get(0) + " failed!",
					control_ds(dsList1.get(0), stop, 0));
		if (bAddDs == -1)
			assertTrue("stop " + dsList2.get(0) + " failed!",
					control_ds(dsList2.get(0), stop, 0));
		if (aAddDs == 1) {
			assertTrue("start " + dsList1.get(0) + " failed!",
					control_ds(dsList1.get(0), start, 0));
			waitto(5);
			if (!uncomment_line(csList1.get(0), tair_bin + groupconf,
					dsList1.get(0), "#"))
				fail("uncomment " + dsList1.get(0) + " failed!");
		}
		if (bAddDs == 1) {
			assertTrue("start " + dsList2.get(0) + " failed!",
					control_ds(dsList2.get(0), start, 0));
			waitto(5);
			if (!uncomment_line(csList2.get(0), tair_bin + groupconf,
					dsList2.get(0), "#"))
				fail("uncomment " + dsList2.get(0) + " failed!");
		}
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), start_migrate, 1, 10);
		else if (bAddDs != 0)
			waitKeyword(csList2.get(0), start_migrate, 1, 10);
	}

	private void operate_func_01_put(int copyCount, int aAddDs, int bAddDs) {

		boolean migFlagA = aAddDs != 0 ? true : false;
		boolean migFlagB = bAddDs != 0 ? true : false;
		boolean migFlag = migFlagA || migFlagB;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// put data on client 1
		int putSuc = execute_on_client(clientList.get(0), put, true, syncCount,
				"put.kv", 0, syncCount, migFlagA);

		// get data on client 1 & 2
		scpFile(clientList.get(0), test_bin, "put.kv", clientList.get(1),
				test_bin);
		execute_on_client(clientList, get, false, syncCount, "put.kv", 0,
				putSuc, migFlag);

		// hide data on client 1
		execute_on_client(clientList.get(0), hide, false, syncCount, "put.kv",
				0, putSuc, migFlagA);

		// get data on client 1 & 2
		execute_on_client(clientList, get, false, syncCount, "put.kv", 0, 0,
				migFlag);

		// getHidden data on client 1 & 2
		execute_on_client(clientList, getHidden, false, syncCount, "put.kv", 0,
				putSuc, migFlag);

		// delete data on client 1
		execute_on_client(clientList.get(0), delete, false, syncCount,
				"put.kv", 0, putSuc, migFlagA);

		// get data on client 1 & 2
		execute_on_client(clientList, get, false, syncCount, "put.kv", 0, 0,
				migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	private void operate_func_02_incr(int copyCount, int aAddDs, int bAddDs) {

		boolean migFlagA = aAddDs != 0 ? true : false;
		boolean migFlagB = bAddDs != 0 ? true : false;
		boolean migFlag = migFlagA || migFlagB;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// incr data on client 1
		int incrSuc = execute_on_client(clientList.get(0), incr, true,
				syncCount, "incr.kv", 0, syncCount, migFlagA);

		// decr data on client 2
		scpFile(clientList.get(0), test_bin, "incr.kv", clientList.get(1),
				test_bin);
		execute_on_client(clientList.get(1), decr, false, syncCount, "incr.kv",
				0, incrSuc, migFlagB);

		// delete data on client 1
		execute_on_client(clientList.get(0), delete, false, syncCount,
				"incr.kv", 0, incrSuc, migFlagA);

		// get data on client 1 & 2
		execute_on_client(clientList, get, false, syncCount, "incr.kv", 0, 0,
				migFlag);

		// decr data on client 1
		int decrSuc = execute_on_client(clientList.get(0), decr, true,
				syncCount, "decr.kv", syncCount * 2, syncCount, migFlagA);

		// incr data on client 2
		scpFile(clientList.get(0), test_bin, "decr.kv", clientList.get(1),
				test_bin);
		execute_on_client(clientList.get(1), incr, false, syncCount, "decr.kv",
				syncCount * 2, decrSuc, migFlagB);

		// delete data on client 1
		execute_on_client(clientList.get(0), delete, false, syncCount,
				"decr.kv", syncCount * 2, decrSuc, migFlagA);

		// get data on client 1 & 2
		execute_on_client(clientList, get, false, syncCount, "decr.kv",
				syncCount * 2, 0, migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	private void operate_func_03_prefixPut(int copyCount, int aAddDs, int bAddDs) {

		boolean migFlagA = aAddDs != 0 ? true : false;
		boolean migFlagB = bAddDs != 0 ? true : false;
		boolean migFlag = migFlagA || migFlagB;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// prefixPut data on client 1
		int prefixPutSuc = execute_on_client(clientList.get(0), prefixPut,
				true, syncCount, "prefixPut.kv", 0, syncCount, migFlagA);

		// prefixGet data on client 1 & 2
		scpFile(clientList.get(0), test_bin, "prefixPut.kv", clientList.get(1),
				test_bin);
		execute_on_client(clientList, prefixGet, false, syncCount,
				"prefixPut.kv", 0, prefixPutSuc, migFlag);

		// prefixHide data on client 1
		execute_on_client(clientList.get(0), prefixHide, false, syncCount,
				"prefixPut.kv", 0, prefixPutSuc, migFlagA);

		// prefixGetHidden data on client 1 & 2
		execute_on_client(clientList, prefixGetHidden, false, syncCount,
				"prefixPut.kv", 0, prefixPutSuc, migFlag);

		// prefixDelete data on client 1
		execute_on_client(clientList.get(0), prefixDelete, false, syncCount,
				"prefixPut.kv", 0, prefixPutSuc, migFlagA);

		// prefixGet data on client 1 & 2
		execute_on_client(clientList, prefixGet, false, syncCount,
				"prefixPut.kv", 0, 0, migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	private void operate_func_04_prefixIncr(int copyCount, int aAddDs,
			int bAddDs) {

		boolean migFlagA = aAddDs != 0 ? true : false;
		boolean migFlagB = bAddDs != 0 ? true : false;
		boolean migFlag = migFlagA || migFlagB;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// prefixIncr data on client 1
		int prefixIncrSuc = execute_on_client(clientList.get(0), prefixIncr,
				true, syncCount, "prefixIncr.kv", 0, syncCount, migFlagA);

		// prefixDecr data on client 2
		scpFile(clientList.get(0), test_bin, "prefixIncr.kv",
				clientList.get(1), test_bin);
		execute_on_client(clientList.get(1), prefixDecr, false, syncCount,
				"prefixIncr.kv", 0, prefixIncrSuc, migFlagB);

		// prefixDelete data on client 1
		execute_on_client(clientList.get(0), prefixDelete, false, syncCount,
				"prefixIncr.kv", 0, prefixIncrSuc, migFlagA);

		// prefixGet data on client 1 & 2
		execute_on_client(clientList, prefixGet, false, syncCount,
				"prefixIncr.kv", 0, 0, migFlag);

		// prefixDecr data on client 1
		int prefixDecrSuc = execute_on_client(clientList.get(0), prefixDecr,
				true, syncCount, "prefixDecr.kv", syncCount * 2, syncCount,
				migFlagA);

		// prefixIncr data on client 2
		scpFile(clientList.get(0), test_bin, "prefixDecr.kv",
				clientList.get(1), test_bin);
		execute_on_client(clientList.get(1), prefixIncr, false, syncCount,
				"prefixDecr.kv", syncCount * 2, prefixDecrSuc, migFlagB);

		// prefixDelete data on client 1
		execute_on_client(clientList.get(0), prefixDelete, false, syncCount,
				"prefixDecr.kv", syncCount * 2, prefixDecrSuc, migFlagA);

		// prefixGet data on client 1 & 2
		execute_on_client(clientList, prefixGet, false, syncCount,
				"prefixDecr.kv", syncCount * 2, 0, migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	private void operate_func_05_setCount(int copyCount, int aAddDs, int bAddDs) {

		boolean migFlagA = aAddDs != 0 ? true : false;
		boolean migFlagB = bAddDs != 0 ? true : false;
		boolean migFlag = migFlagA || migFlagB;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// put data on client 1
		int putSuc = execute_on_client(clientList.get(0), put, true, syncCount,
				"put.kv", 0, syncCount, migFlagA);

		// setCount data on client 1
		int setCountSuc = execute_on_client(clientList.get(0), setCount, false,
				syncCount, "put.kv", "setCount.kv", 0, putSuc, migFlagA);

		// incr data on client 2
		scpFile(clientList.get(0), test_bin, "setCount.kv", clientList.get(1),
				test_bin);
		execute_on_client(clientList.get(1), incr, false, syncCount,
				"setCount.kv", 0, setCountSuc, migFlagB);

		// decr data on client 1
		execute_on_client(clientList.get(0), decr, false, syncCount,
				"setCount.kv", 0, 0, migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	private void operate_func_06_prefixSetCount(int copyCount, int aAddDs,
			int bAddDs) {

		boolean migFlagA = aAddDs != 0 ? true : false;
		boolean migFlagB = bAddDs != 0 ? true : false;
		boolean migFlag = migFlagA || migFlagB;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// prefixPut data on client 1
		int prefixPutSuc = execute_on_client(clientList.get(0), prefixPut,
				true, syncCount, "prefixPut.kv", 0, syncCount, migFlagA);

		// prefixSetCount data on client 1
		int prefixSetCountSuc = execute_on_client(clientList.get(0),
				prefixSetCount, false, syncCount, "prefixPut.kv",
				"prefixSetCount.kv", 0, prefixPutSuc, migFlagA);

		// prefixIncr data on client 2
		scpFile(clientList.get(0), test_bin, "prefixSetCount.kv",
				clientList.get(1), test_bin);
		execute_on_client(clientList.get(1), prefixIncr, false, syncCount,
				"prefixSetCount.kv", 0, prefixSetCountSuc, migFlagB);

		// prefixDecr data on client 1
		execute_on_client(clientList.get(0), prefixDecr, false, syncCount,
				"prefixSetCount.kv", 0, 0, migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	private void operate_func_07_lock(int copyCount, int aAddDs, int bAddDs) {

		boolean migFlag = aAddDs != 0 || bAddDs != 0 ? true : false;

		// prepare server environment
		prepareServerEnvironment(copyCount, aAddDs, bAddDs);

		// put data on client 1
		int putSuc = execute_on_client(clientList.get(0), put, true, syncCount,
				"put.kv", 0, syncCount, migFlag);

		// lock data on client 1
		execute_on_client(clientList.get(0), lock, false, syncCount, "put.kv",
				0, putSuc, migFlag);

		// get data on client 1 & 2
		scpFile(clientList.get(0), test_bin, "put.kv", clientList.get(1),
				test_bin);
		execute_on_client(clientList, get, false, syncCount, "put.kv", 0,
				putSuc, true, migFlag);

		// unlock data on client 1
		execute_on_client(clientList.get(0), unlock, false, syncCount,
				"put.kv", 0, putSuc, migFlag);

		// get data on client 1 & 2
		execute_on_client(clientList, get, false, syncCount, "put.kv", 0,
				putSuc, false, migFlag);

		// wait migrate all done
		if (aAddDs != 0)
			waitKeyword(csList1.get(0), finish_migrate, 1, 100);
		if (bAddDs != 0)
			waitKeyword(csList2.get(0), finish_migrate, 1, 100);
	}

	// normal case with _copy_count 1
	@Test
	public void testFailover_01_put_func_copy_count_1() {
		log.info("start sync put _copy_count_1 test case 01");
		operate_func_01_put(1, 0, 0);
		log.info("end sync put _copy_count_1 test case 01");
	}

	@Test
	public void testFailover_02_incr_func_copy_count_1() {
		log.info("start sync incr _copy_count_1 test case 02");
		operate_func_02_incr(1, 0, 0);
		log.info("end sync incr _copy_count_1 test case 02");
	}

	@Test
	public void testFailover_03_prefixPut_func_copy_count_1() {
		log.info("start sync prefixPut _copy_count_1 test case 03");
		operate_func_03_prefixPut(1, 0, 0);
		log.info("end sync prefixPut _copy_count_1 test case 03");
	}

	@Test
	public void testFailover_04_prefixIncr_func_copy_count_1() {
		log.info("start sync prefixIncr _copy_count_1 test case 04");
		operate_func_04_prefixIncr(1, 0, 0);
		log.info("end sync prefixIncr _copy_count_1 test case 04");
	}

	@Test
	public void testFailover_05_setCount_func_copy_count_1() {
		log.info("start sync setCount _copy_count_1 test case 05");
		operate_func_05_setCount(1, 0, 0);
		log.info("end sync setCount _copy_count_1 test case 05");
	}

	@Test
	public void testFailover_06_lock_func_copy_count_1() {
		log.info("start sync lock _copy_count_1 test case 06");
		operate_func_07_lock(1, 0, 0);
		log.info("end sync lock _copy_count_1 test case 06");
	}

	@Test
	public void testFailover_07_prefixSetCount_func_copy_count_1() {
		log.info("start sync prefixSetCount _copy_count_1 test case 07");
		operate_func_06_prefixSetCount(1, 0, 0);
		log.info("end sync prefixSetCount _copy_count_1 test case 07");
	}

	// normal case with _copy_count 2
	@Test
	public void testFailover_08_put_func_copy_count_2() {
		log.info("start sync put _copy_count_2 test case 08");
		operate_func_01_put(2, 0, 0);
		log.info("end sync put _copy_count_2 test case 08");
	}

	@Test
	public void testFailover_09_incr_func_copy_count_2() {
		log.info("start sync incr _copy_count_2 test case 09");
		operate_func_02_incr(2, 0, 0);
		log.info("end sync incr _copy_count_2 test case 09");
	}

	@Test
	public void testFailover_10_prefixPut_func_copy_count_2() {
		log.info("start sync prefixPut _copy_count_2 test case 10");
		operate_func_03_prefixPut(2, 0, 0);
		log.info("end sync prefixPut _copy_count_2 test case 10");
	}

	@Test
	public void testFailover_11_prefixIncr_func_copy_count_2() {
		log.info("start sync prefixIncr _copy_count_2 test case 11");
		operate_func_04_prefixIncr(2, 0, 0);
		log.info("end sync prefixIncr _copy_count_2 test case 11");
	}

	@Test
	public void testFailover_12_setCount_func_copy_count_2() {
		log.info("start sync setCount _copy_count_2 test case 12");
		operate_func_05_setCount(2, 0, 0);
		log.info("end sync setCount _copy_count_2 test case 12");
	}

	@Test
	public void testFailover_13_lock_func_copy_count_2() {
		log.info("start sync lock _copy_count_2 test case 13");
		operate_func_07_lock(2, 0, 0);
		log.info("end sync lock _copy_count_2 test case 13");
	}

	@Test
	public void testFailover_14_prefixSetCount_func_copy_count_2() {
		log.info("start sync prefixSetCount _copy_count_2 test case 14");
		operate_func_06_prefixSetCount(2, 0, 0);
		log.info("end sync prefixSetCount _copy_count_2 test case 14");
	}

	// migrate case by cluster A add ds with _copy_count 1
	@Test
	public void testFailover_15_put_func_copy_count_1() {
		log.info("start sync put _copy_count_1 while A add 1ds test case 15");
		operate_func_01_put(1, 1, 0);
		log.info("end sync put _copy_count_1 while A add 1ds test case 15");
	}

	@Test
	public void testFailover_16_incr_func_copy_count_1() {
		log.info("start sync incr _copy_count_1 while A add 1ds test case 16");
		operate_func_02_incr(1, 1, 0);
		log.info("end sync incr _copy_count_1 while A add 1ds test case 16");
	}

	@Test
	public void testFailover_17_prefixPut_func_copy_count_1() {
		log.info("start sync prefixPut _copy_count_1 while A add 1ds test case 17");
		operate_func_03_prefixPut(1, 1, 0);
		log.info("end sync prefixPut _copy_count_1 while A add 1ds test case 17");
	}

	@Test
	public void testFailover_18_prefixIncr_func_copy_count_1() {
		log.info("start sync prefixIncr _copy_count_1 while A add 1ds test case 18");
		operate_func_04_prefixIncr(1, 1, 0);
		log.info("end sync prefixIncr _copy_count_1 while A add 1ds test case 18");
	}

	@Test
	public void testFailover_19_setCount_func_copy_count_1() {
		log.info("start sync setCount _copy_count_1 while A add 1ds test case 19");
		operate_func_05_setCount(1, 1, 0);
		log.info("end sync setCount _copy_count_1 while A add 1ds test case 19");
	}

	@Test
	public void testFailover_20_prefixSetCount_func_copy_count_1() {
		log.info("start sync prefixSetCount _copy_count_1 while A add 1ds test case 20");
		operate_func_06_prefixSetCount(1, 1, 0);
		log.info("end sync prefixSetCount _copy_count_1 while A add 1ds test case 20");
	}

	// migrate case by cluster A add ds with _copy_count 2
	@Test
	public void testFailover_21_put_func_copy_count_2() {
		log.info("start sync put _copy_count_2 while A add 1ds test case 21");
		operate_func_01_put(2, 1, 0);
		log.info("end sync put _copy_count_2 while A add 1ds test case 21");
	}

	@Test
	public void testFailover_22_incr_func_copy_count_2() {
		log.info("start sync incr _copy_count_2 while A add 1ds test case 22");
		operate_func_02_incr(2, 1, 0);
		log.info("end sync incr _copy_count_2 while A add 1ds test case 22");
	}

	@Test
	public void testFailover_23_prefixPut_func_copy_count_2() {
		log.info("start sync prefixPut _copy_count_2 while A add 1ds test case 23");
		operate_func_03_prefixPut(2, 1, 0);
		log.info("end sync prefixPut _copy_count_2 while A add 1ds test case 23");
	}

	@Test
	public void testFailover_24_prefixIncr_func_copy_count_2() {
		log.info("start sync prefixIncr _copy_count_2 while A add 1ds test case 24");
		operate_func_04_prefixIncr(2, 1, 0);
		log.info("end sync prefixIncr _copy_count_2 while A add 1ds test case 24");
	}

	@Test
	public void testFailover_25_setCount_func_copy_count_2() {
		log.info("start sync setCount _copy_count_2 while A add 1ds test case 25");
		operate_func_05_setCount(2, 1, 0);
		log.info("end sync setCount _copy_count_2 while A add 1ds test case 25");
	}

	@Test
	public void testFailover_26_prefixSetCount_func_copy_count_2() {
		log.info("start sync prefixSetCount _copy_count_2 while A add 1ds test case 26");
		operate_func_06_prefixSetCount(2, 1, 0);
		log.info("end sync prefixSetCount _copy_count_2 while A add 1ds test case 26");
	}

	// migrate case by cluster A remove ds with _copy_count 2
	@Test
	public void testFailover_27_put_func_copy_count_2() {
		log.info("start sync put _copy_count_2 while A remove 1ds test case 27");
		operate_func_01_put(2, -1, 0);
		log.info("end sync put _copy_count_2 while A remove 1ds test case 27");
	}

	@Test
	public void testFailover_28_incr_func_copy_count_2() {
		log.info("start sync incr _copy_count_2 while A remove 1ds test case 28");
		operate_func_02_incr(2, -1, 0);
		log.info("end sync incr _copy_count_2 while A remove 1ds test case 28");
	}

	@Test
	public void testFailover_29_prefixPut_func_copy_count_2() {
		log.info("start sync prefixPut _copy_count_2 while A remove 1ds test case 29");
		operate_func_03_prefixPut(2, -1, 0);
		log.info("end sync prefixPut _copy_count_2 while A remove 1ds test case 29");
	}

	@Test
	public void testFailover_30_prefixIncr_func_copy_count_2() {
		log.info("start sync prefixIncr _copy_count_2 while A remove 1ds test case 30");
		operate_func_04_prefixIncr(2, -1, 0);
		log.info("end sync prefixIncr _copy_count_2 while A remove 1ds test case 30");
	}

	@Test
	public void testFailover_31_setCount_func_copy_count_2() {
		log.info("start sync setCount _copy_count_2 while A remove 1ds test case 31");
		operate_func_05_setCount(2, -1, 0);
		log.info("end sync setCount _copy_count_2 while A remove 1ds test case 31");
	}

	@Test
	public void testFailover_32_prefixSetCount_func_copy_count_2() {
		log.info("start sync prefixSetCount _copy_count_2 while A remove 1ds test case 32");
		operate_func_06_prefixSetCount(2, -1, 0);
		log.info("end sync prefixSetCount _copy_count_2 while A remove 1ds test case 32");
	}

	// migrate case by cluster B add ds with _copy_count 1
	@Test
	public void testFailover_33_put_func_copy_count_1() {
		log.info("start sync put _copy_count_1 while B add 1ds test case 33");
		operate_func_01_put(1, 0, 1);
		log.info("end sync put _copy_count_1 while B add 1ds test case 33");
	}

	@Test
	public void testFailover_34_incr_func_copy_count_1() {
		log.info("start sync incr _copy_count_1 while B add 1ds test case 34");
		operate_func_02_incr(1, 0, 1);
		log.info("end sync incr _copy_count_1 while B add 1ds test case 34");
	}

	@Test
	public void testFailover_35_prefixPut_func_copy_count_1() {
		log.info("start sync prefixPut _copy_count_1 while B add 1ds test case 35");
		operate_func_03_prefixPut(1, 0, 1);
		log.info("end sync prefixPut _copy_count_1 while B add 1ds test case 35");
	}

	@Test
	public void testFailover_36_prefixIncr_func_copy_count_1() {
		log.info("start sync prefixIncr _copy_count_1 while B add 1ds test case 36");
		operate_func_04_prefixIncr(1, 0, 1);
		log.info("end sync prefixIncr _copy_count_1 while B add 1ds test case 36");
	}

	@Test
	public void testFailover_37_setCount_func_copy_count_1() {
		log.info("start sync setCount _copy_count_1 while B add 1ds test case 37");
		operate_func_05_setCount(1, 0, 1);
		log.info("end sync setCount _copy_count_1 while B add 1ds test case 37");
	}

	@Test
	public void testFailover_38_prefixSetCount_func_copy_count_1() {
		log.info("start sync prefixSetCount _copy_count_1 while B add 1ds test case 38");
		operate_func_06_prefixSetCount(1, 0, 1);
		log.info("end sync prefixSetCount _copy_count_1 while B add 1ds test case 38");
	}

	// migrate case by cluster B add ds with _copy_count 2
	@Test
	public void testFailover_39_put_func_copy_count_2() {
		log.info("start sync put _copy_count_2 while B add 1ds test case 39");
		operate_func_01_put(2, 0, 1);
		log.info("end sync put _copy_count_2 while B add 1ds test case 39");
	}

	@Test
	public void testFailover_40_incr_func_copy_count_2() {
		log.info("start sync incr _copy_count_2 while B add 1ds test case 40");
		operate_func_02_incr(2, 0, 1);
		log.info("end sync incr _copy_count_2 while B add 1ds test case 40");
	}

	@Test
	public void testFailover_41_prefixPut_func_copy_count_2() {
		log.info("start sync prefixPut _copy_count_2 while B add 1ds test case 41");
		operate_func_03_prefixPut(2, 0, 1);
		log.info("end sync prefixPut _copy_count_2 while B add 1ds test case 41");
	}

	@Test
	public void testFailover_42_prefixIncr_func_copy_count_2() {
		log.info("start sync prefixIncr _copy_count_2 while B add 1ds test case 42");
		operate_func_04_prefixIncr(2, 0, 1);
		log.info("end sync prefixIncr _copy_count_2 while B add 1ds test case 42");
	}

	@Test
	public void testFailover_43_setCount_func_copy_count_2() {
		log.info("start sync setCount _copy_count_2 while B add 1ds test case 43");
		operate_func_05_setCount(2, 0, 1);
		log.info("end sync setCount _copy_count_2 while B add 1ds test case 43");
	}

	@Test
	public void testFailover_44_prefixSetCount_func_copy_count_2() {
		log.info("start sync prefixSetCount _copy_count_2 while B add 1ds test case 44");
		operate_func_06_prefixSetCount(2, 0, 1);
		log.info("end sync prefixSetCount _copy_count_2 while B add 1ds test case 44");
	}

	// migrate case by cluster B remove ds with _copy_count 2
	@Test
	public void testFailover_45_put_func_copy_count_2() {
		log.info("start sync put _copy_count_2 while B remove 1ds test case 45");
		operate_func_01_put(2, 0, -1);
		log.info("end sync put _copy_count_2 while B remove 1ds test case 45");
	}

	@Test
	public void testFailover_46_incr_func_copy_count_2() {
		log.info("start sync incr _copy_count_2 while B remove 1ds test case 46");
		operate_func_02_incr(2, 0, -1);
		log.info("end sync incr _copy_count_2 while B remove 1ds test case 46");
	}

	@Test
	public void testFailover_47_prefixPut_func_copy_count_2() {
		log.info("start sync prefixPut _copy_count_2 while B remove 1ds test case 47");
		operate_func_03_prefixPut(2, 0, -1);
		log.info("end sync prefixPut _copy_count_2 while B remove 1ds test case 47");
	}

	@Test
	public void testFailover_48_prefixIncr_func_copy_count_2() {
		log.info("start sync prefixIncr _copy_count_2 while B remove 1ds test case 48");
		operate_func_04_prefixIncr(2, 0, -1);
		log.info("end sync prefixIncr _copy_count_2 while B remove 1ds test case 48");
	}

	@Test
	public void testFailover_49_setCount_func_copy_count_2() {
		log.info("start sync setCount _copy_count_2 while B remove 1ds test case 49");
		operate_func_05_setCount(2, 0, -1);
		log.info("end sync setCount _copy_count_2 while B remove 1ds test case 49");
	}

	@Test
	public void testFailover_50_prefixSetCount_func_copy_count_2() {
		log.info("start sync prefixSetCount _copy_count_2 while B remove 1ds test case 50");
		operate_func_06_prefixSetCount(2, 0, -1);
		log.info("end sync prefixSetCount _copy_count_2 while B remove 1ds test case 50");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		batch_clean_tool(clientList);
		batch_clean_ycsb(ycsbList);
		controlAllCluster(csList1, csList2, dsList1, dsList2, stop, 1);
		batch_clean_data(dsList1);
		batch_clean_data(dsList2);
		batch_uncomment(csList1, tair_bin + groupconf, dsList1, "#");
		batch_uncomment(csList2, tair_bin + groupconf, dsList2, "#");
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
		batch_clean_tool(clientList);
		batch_clean_ycsb(ycsbList);
		controlAllCluster(csList1, csList2, dsList1, dsList2, stop, 1);
		batch_clean_data(dsList1);
		batch_clean_data(dsList2);
		batch_uncomment(csList1, tair_bin + groupconf, dsList1, "#");
		batch_uncomment(csList2, tair_bin + groupconf, dsList2, "#");
	}
}

