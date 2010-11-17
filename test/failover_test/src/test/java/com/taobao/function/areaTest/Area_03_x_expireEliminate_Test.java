package com.taobao.function.areaTest;

import java.sql.Date;
import java.util.Calendar;

import junit.framework.Assert;

public class Area_03_x_expireEliminate_Test extends AreaTestBaseCase {
	/*
	 * 512大小数据的实际设置的key和value大小为：448 1024大小的数据实际设置的key和value大小为：960
	 */
	final static int D512B = 448;
	final static int D1024B = 960;
	final static int num = 40000;

	public void testExpireEliminate_01_expireDate_eliminated_ontime() {
		log.error("start expire eliminate test case 01");

		log.error("**modify conf**");
		// set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(), 0, 128 * 1024 * 1024);
		changeAreaQuota(csList.get(0).toString(), 1, 128 * 1024 * 1024);
		log.error("quota has been setted!");

		log.error("**start server**");
		// start cluster
		if (!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))
			fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);

		log.error("**perpare expire data**");
		log.error("area0");
		// put 40000 data of 512B size to area0
		putDate(0, 0, num, D512B, 0);
		assertTrue(verifySuccess(num, Put));

		// put 40000 expired data of 512B size to area0
		putDate(0, num, 2 * num, D512B, 1);
		assertTrue(verifySuccess(num, Put));

		// put 40000 expire data of 1024B size to area0
		putDate(0, 2 * num, 3 * num, D1024B, 1);
		assertTrue(verifySuccess(num, Put));

		log.error("area0 data is ready");

		log.error("area1");
		// put 1 data of 512B size to area1
		// put 40000 data of 512B size to area0
		putDate(1, 0, num, D512B, 0);
		assertTrue(verifySuccess(num, Put));

		// put 40000 expired data of 512B size to area0
		putDate(1, num, 2 * num, D512B, 1);
		assertTrue(verifySuccess(num, Put));

		// put 40000 expire data of 1024B size to area0
		putDate(1, 2 * num, 3 * num, D1024B, 1);
		assertTrue(verifySuccess(num, Put));
		log.error("area1 data is ready,area1 is full");

		log.error("sed expire hour range");
		Calendar c = Calendar.getInstance();
		int startHour = c.get(Calendar.HOUR_OF_DAY);
		assertTrue(changeHourRange(dsList.get(0).toString(), "expired", startHour, startHour + 1));
		log.error("expire time has been changed successful");
		
		log.error("stop ds");
		if(!batch_control_ds(dsList, AreaTestBaseCase.stop, 0))fail("stop ds failed!");
	
		waitto(AreaTestBaseCase.down_time);
		if(!batch_control_ds(dsList, AreaTestBaseCase.start, 0))fail("start ds failed!");
		//touch group.conf
		touch_file((String) csList.get(0), AreaTestBaseCase.tair_bin+"etc/group.conf");
		log.error("change group.conf and touch it");
		waitto(60*AreaTestBaseCase.down_time);

		log.error("start verify data");
		log.error("verify area0");
		getDate(0, 0, num);
		assertTrue(verifySuccess(num, Get));
		log.error("area0's fist data has been readed successfully");

		getDate(0, num, 3 * num);
		assertTrue(verifySuccess(0, Get));
		log.error("area0's else data has been readed successfully");

		int result = check_keyword("local", "expired", "/home/admin/baoni/function/Get.log");

		Assert.assertEquals(0, result);
		log.error("area0's all expird data has been removed");
		result = 0;

		log.error("verify area1");
		getDate(1, 0, num);
		assertTrue(verifySuccess(num, Get));
		log.error("area0's fist data has been readed successfully");

		getDate(1, num, 3 * num);
		assertTrue(verifySuccess(0, Get));
		log.error("area0's else data has been readed successfully");

		result = check_keyword("local", "expired", "/home/admin/baoni/function/Get.log");

		Assert.assertEquals(0, result);
		log.error("area1's all expird data has been removed");

		log.error("end expire eliminate test case 01");
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
