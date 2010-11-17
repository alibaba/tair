package com.taobao.function.areaTest;


public class Area_00_x_size_Test extends AreaTestBaseCase {
	/*
	 * 512大小数据的实际设置的key和value大小为：448
	 * 1024大小的数据实际设置的key和value大小为：960
	 */
	final static int D512KB=448;
	final static int D1024KB=960;
	final static int D1MB=1000000;
	
//	final static int D512KB=512;
//	final static int D1024KB=1024;
//	final static int D1MB=1048576;
	
	public void test_01_quota_512KB_data_512B()
	{
		log.error("start size test case 01");
		
		int waitcnt=0;
		
		log.error("**modify conf**，set quota 512kb");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		//put 1 data of 512B size to area0
		putDate(0,0,1024,D512KB,0);
		assertTrue(verifySuccess(1024,Put));
		getDate(0,0,1024);
		assertTrue(verifySuccess(1024,Get));
		log.error("put 1024 data of 512B");
		
		for(int i=0;i<1024;i++){
			putDate(0,1024+i,1025+i,D512KB,0);
			assertTrue(verifySuccess(1,Put));
			getDate(0,0,1025+i);
			assertTrue(verifySuccess(1025+i,Get));
			
		}
		log.error("end cluster test failover case 01");
	}
	public void test_02_quota_512KB_data_1024B()
	{
		log.error("start size test case 02");
		
		int waitcnt=0;
		
		log.error("**modify conf**，set quota 512kb");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		//put 1 data of 512B size to area0
		putDate(0,0,512,D1024KB,0);
		assertTrue(verifySuccess(512,Put));
		getDate(0,0,512);
		assertTrue(verifySuccess(512,Get));
		log.error("put 512 data of 1024B");
		
		for(int i=0;i<1024;i++){
			putDate(0,512+i,513+i,D1024KB,0);
			assertTrue(verifySuccess(1,Put));
			getDate(0,0,513+i);
			assertTrue(verifySuccess(513+i,Get));
			
		}
		log.error("end cluster test failover case 02");
	}
	public void test_03_quota_512KB_data_512B_and_data_1024()
	{
		log.error("start size test case 03");
		
		int waitcnt=0;
		
		log.error("**modify conf**，set quota 512kb");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		putDate(0,0,510,D1024KB,0);
		assertTrue(verifySuccess(510,Put));
		getDate(0,0,510);
		assertTrue(verifySuccess(510,Get));
		log.error("put 510 data of 1024B");
		
		//put 1 data of 512B size to area0
		putDate(0,510,514,D512KB,0);
		assertTrue(verifySuccess(4,Put));
		getDate(0,0,514);
		assertTrue(verifySuccess(514,Get));
		log.error("put 4 data of 512B");
		
		for(int i=0;i<1024;i++){
			putDate(0,514+i,515+i,D512KB,0);
			assertTrue(verifySuccess(1,Put));
			getDate(0,0,515+i);
			assertTrue(verifySuccess(515+i,Get));
			
		}
		log.error("end cluster test failover case 03");
	}
	
	
	public void test_04_quota_256MB_data_1025B_full()
	{
		log.error("start size test case 04");
		
		int waitcnt=0;
		
		log.error("**modify conf**，set quota 512kb");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,256*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		putDate(0,0,256*1024,D1024KB,0);
		assertTrue(verifySuccess(262214,Put));
		getDate(0,262214,510);
		assertTrue(verifySuccess(262214,Get));
		log.error("put 262214 data of 1024B");
		
		
		for(int i=0;i<61024;i++){
			putDate(0,226614+i,226615+i,D1024KB,0);
			assertTrue(verifySuccess(1,Put));
			getDate(0,0,226615+i);
			assertTrue(verifySuccess(226615+i,Get));
			
		}
		log.error("end cluster test failover case 04");
	}
	public void test_05_quota_1MB_data_512B()
	{
		log.error("start size test case 05");
		
		int waitcnt=0;
		
		log.error("**modify conf**，set quota 512kb");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		//put 1 data of 512B size to area0
		putDate(0,0,2048,D512KB,0);
		assertTrue(verifySuccess(2048,Put));
		getDate(0,0,2048);
		assertTrue(verifySuccess(2048,Get));
		log.error("put 2048 data of 512B");
		
		for(int i=0;i<1024;i++){
			putDate(0,2048+i,2049+i,D512KB,0);
			assertTrue(verifySuccess(1,Put));
			getDate(0,0,2049+i);
			assertTrue(verifySuccess(2049+i,Get));
			
		}
		log.error("end cluster test failover case 05");
	}
	
	 //把系统都启动起来
	public void setUp()
	{
		log.error("clean tool and cluster!");
		clean_tool("local");
		reset_cluster(csList,dsList);
        
	}
	
	//清理
	public void tearDown()
	{
		log.error("clean tool and cluster!");
	    clean_tool("local");
	    reset_cluster(csList,dsList);
		
	}

}
