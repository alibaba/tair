package com.taobao.function.areaTest;

import java.util.Calendar;
import java.util.List;

import com.taobao.tairtest.FailOverBaseCase;

import junit.framework.Assert;

public class Area_02_x_slabBalance_Test extends AreaTestBaseCase {
	/*
	 * 512大小数据的实际设置的key和value大小为：448 1024大小的数据实际设置的key和value大小为：960
	 */
	final static int D512B = 448;
	final static int D1024B = 960;
	final static int D1MB = 1000000;
	final static int SLAB_512_INDEX = 19;
	final static int SLAB_1024_INDEX = 26;

	// 1M
	final static int SIZE1M512 = 2331;

	public void testSlabBalance_01_slab_balance() {
		log.error("start slab balance test case 01");

		log.error("**modify conf**");
		// set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(), 0, 256 * 1024 * 1024);
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
		putDate(0, 0, 100000, D1024B, 0);
		assertTrue(verifySuccess(100000, Put));
		Assert.assertEquals(checkSlab(dsList.get(0).toString(),SLAB_1024_INDEX), 97185);


		putDate(0, 100000, 500000, D512B, 0);
		assertTrue(verifySuccess(400000, Put));
		Assert.assertEquals(checkSlab(dsList.get(0).toString(),SLAB_1024_INDEX), 97185);
		Assert.assertEquals(checkSlab(dsList.get(0).toString(),SLAB_512_INDEX), 2047);

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
		
		log.error("**verify slab state**");
		assertTrue(checkSlab(dsList.get(0).toString(),SLAB_1024_INDEX)< 97185);
		Assert.assertEquals(checkSlab(dsList.get(0).toString(),SLAB_512_INDEX), 2047);

		log.error("end slab balance test case 01");
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
//		clean_tool("local");
//		reset_cluster(csList, dsList);
		assertTrue(changeHourRange(dsList.get(0).toString(), "slab", 5, 7));
		assertTrue(changeHourRange(dsList.get(0).toString(), "expired", 2, 4));

	}

}
