package com.taobao.function.areaTest;

import java.util.Calendar;

import com.taobao.tairtest.FailOverBaseCase;

public class Area_04_x_areaQuotaChange_Test extends AreaTestBaseCase {
	/*
	 * 512大小数据的实际设置的key和value大小为：448 1024大小的数据实际设置的key和value大小为：960
	 */
	final static int D512B = 448;
	final static int D1024B = 960;
	final static int D1MB = 1000000;

	// 1M
	final static int SIZE1M512 = 2331;

	public void testQuotaChanged_01_enlarge_quota() {
		log.error("start area quota changed test case 01");

		log.error("**modify conf**");
		// set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(), 0, 1 * 1024 * 1024);
		log.error("quota has been setted!");

		log.error("**start server**");
		// start cluster
		if (!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);

		log.error("**perpare data**");
		log.error("area0");
		// put 1 data of 512B size to area0
		putDate(0, 0, SIZE1M512, D512B, 0);
		assertTrue(verifySuccess(SIZE1M512, Put));

		getDate(0, 0, SIZE1M512);
		assertTrue(verifySuccess(SIZE1M512, Get));

		putDate(0, SIZE1M512, SIZE1M512 + 1, D512B, 0);
		assertTrue(verifySuccess(1, Put));

		getDate(0, 0, SIZE1M512 + 1);
		assertTrue(verifySuccess(SIZE1M512, Get));
		log.error("area0 data is ready,area0 is full");

		log.error("change area quota ");
		changeAreaQuota(csList.get(0).toString(), 0, 2 * 1024 * 1024);
		log.error("quota has been changed to 2M!");

		log.error("**verify area quota size**");

		putDate(0, SIZE1M512 + 1, 2 * SIZE1M512 + 1, D512B, 0);
		assertTrue(verifySuccess(SIZE1M512, Put));

		// get the else data
		getDate(0, 1, 2 * SIZE1M512 + 1);
		assertTrue(verifySuccess(SIZE1M512 * 2 - 1, Get));
		log.error("area0's quota has been changed successfully");

		log.error("end area quota changed test case 01");
	}

	public void testQuotaChanged_02_reduce_quota_less_then_datasize() {
		log.error("start area quota changed test case 02");

		log.error("**modify conf**");
		// set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(), 0, 50 * 1024 * 1024);
		log.error("quota has been setted!");

		log.error("**start server**");
		// start cluster
		if (!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);

		log.error("**perpare data**");
		log.error("area0");
		// put 1 data of 512B size to area0
		int num = 50;
		putDate(0, 0, num, D1MB, 0);
		assertTrue(verifySuccess(num, Put));

		getDate(0, 0, num);
		assertTrue(verifySuccess(num, Get));

		log.error("area0 data is ready");

		log.error("change area quota ");
		changeAreaQuota(csList.get(0).toString(), 0, 512);
		log.error("quota has been changed to 512B!");

		log.error("sed slab hour range");
		Calendar c = Calendar.getInstance();
		int startHour = c.get(Calendar.HOUR_OF_DAY);
		assertTrue(changeHourRange(dsList.get(0).toString(), "slab", startHour, startHour + 1));
		log.error("slab time has been changed successful");
		
		log.error("stop ds");
		if(!batch_control_ds(dsList, AreaTestBaseCase.stop, 0))fail("stop ds failed!");
	
		waitto(AreaTestBaseCase.down_time);
		if(!batch_control_ds(dsList, AreaTestBaseCase.start, 0))fail("start ds failed!");
		//touch group.conf
		touch_file((String) csList.get(0), AreaTestBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		waitto(60*AreaTestBaseCase.down_time);

		log.error("**verify  quota size**");
		getDate(0, 0, num);
		assertTrue(verifySuccess(0, Get));
		log.error("area0's quota has been changed successfully");

		log.error("end area quota changed test case 02");
	}
	public void testQuotaChanged_03_reduce_quota_equals_datasize() {
		log.error("start area quota changed test case 03");

		log.error("**modify conf**");
		// set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(), 0, 50 * 1024 * 1024);
		log.error("quota has been setted!");

		log.error("**start server**");
		// start cluster
		if (!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);

		log.error("**perpare data**");
		log.error("area0");
		// put 1 data of 512B size to area0
		int num = 50;
		putDate(0, 0, num, D1MB, 0);
		assertTrue(verifySuccess(num, Put));

		getDate(0, 0, num);
		assertTrue(verifySuccess(num, Get));

		log.error("area0 data is ready");

		log.error("change area quota ");
		changeAreaQuota(csList.get(0).toString(), 0, 1024*1024);
		log.error("quota has been changed to 512B!");

		log.error("sed slab hour range");
		Calendar c = Calendar.getInstance();
		int startHour = c.get(Calendar.HOUR_OF_DAY);
		assertTrue(changeHourRange(dsList.get(0).toString(), "slab", startHour, startHour + 1));
		log.error("slab time has been changed successful");
		
		log.error("stop ds");
		if(!batch_control_ds(dsList, AreaTestBaseCase.stop, 0))fail("stop ds failed!");
	
		waitto(AreaTestBaseCase.down_time);
		if(!batch_control_ds(dsList, AreaTestBaseCase.start, 0))fail("start ds failed!");
		//touch group.conf
		touch_file((String) csList.get(0), AreaTestBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		waitto(60*AreaTestBaseCase.down_time);

		log.error("**verify  quota size**");
		getDate(0, 0, num);
		assertTrue(verifySuccess(1, Get));
		log.error("area0's quota has been changed successfully");

		log.error("end area quota changed test case 03");
	}
	public void testQuotaChanged_04_reduce_quota_bigger_than_onedatasize_less_than_twodatasize() {
		log.error("start area quota changed test case 04");

		log.error("**modify conf**");
		// set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(), 0, 50 * 1024 * 1024);
		log.error("quota has been setted!");

		log.error("**start server**");
		// start cluster
		if (!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);

		log.error("**perpare data**");
		log.error("area0");
		// put 1 data of 512B size to area0
		int num = 50;
		putDate(0, 0, num, D1MB, 0);
		assertTrue(verifySuccess(num, Put));

		getDate(0, 0, num);
		assertTrue(verifySuccess(num, Get));

		log.error("area0 data is ready");

		log.error("change area quota ");
		changeAreaQuota(csList.get(0).toString(), 0, (int)(1.5*1024*1024));
		log.error("quota has been changed to 512B!");

		log.error("sed slab hour range");
		Calendar c = Calendar.getInstance();
		int startHour = c.get(Calendar.HOUR_OF_DAY);
		assertTrue(changeHourRange(dsList.get(0).toString(), "slab", startHour, startHour + 1));
		log.error("slab time has been changed successful");
		
		log.error("stop ds");
		if(!batch_control_ds(dsList, AreaTestBaseCase.stop, 0))fail("stop ds failed!");
	
		waitto(AreaTestBaseCase.down_time);
		if(!batch_control_ds(dsList, AreaTestBaseCase.start, 0))fail("start ds failed!");
		//touch group.conf
		touch_file((String) csList.get(0), AreaTestBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		waitto(60*AreaTestBaseCase.down_time);

		log.error("**verify  quota size**");
		getDate(0, 0, num);
		assertTrue(verifySuccess(1, Get));
		log.error("area0's quota has been changed successfully");

		log.error("end area quota changed test case 04");
	}
	// 把系统都启动起来
	public void setUp() {
		log.error("clean tool and cluster!");
		clean_tool("local");
		reset_cluster(csList, dsList);

	}

	// 清理
	public void tearDown() {
		log.error("clean tool and cluster!");
		clean_tool("local");
		reset_cluster(csList, dsList);
		assertTrue(changeHourRange(dsList.get(0).toString(), "slab", 5, 7));
		assertTrue(changeHourRange(dsList.get(0).toString(), "expired", 2, 4));

	}

}
