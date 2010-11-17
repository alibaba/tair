package com.taobao.function.areaTest;


public class Area_01_x_areaEliminate_Test extends AreaTestBaseCase {
	/*
	 * 512大小数据的实际设置的key和value大小为：448
	 * 1024大小的数据实际设置的key和value大小为：960
	 */
	final static int D512B = 448;
	final static int D1024B = 960;
	final static int D1MB = 1000000;
	
	//quota 512kb
	final static int SIZE512 = 1166;
	final static int SIZE1024 = 545;
	final static int SIZEFULL = 262144;
	
	final static int SIZECOMBINE512 = 75;
	final static int SIZECOMBINE1024 = 510;
	final static int SIZECOMBINE = SIZECOMBINE1024 + SIZECOMBINE512;
	
	//quota 1M
	final static int SIZE1M512=2331;
	
	
	public void testAreaEliminate_01_area_full_expireDate_eliminated()
	{
		log.error("start area eliminate test case 01");
		
		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 1 data of 512B size to area0
		putDate(0,0,1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		
		//put 1 expired data of 512B size to area0
		putDate(0,1,2,D512B,1);
		assertTrue(verifySuccess(1,Put));
		
		//put the else data of 512B size to area0
		putDate(0,2,SIZECOMBINE512,D512B,0);
		assertTrue(verifySuccess(SIZECOMBINE512-2,Put));
		
		//put 510 data of 1024B size to area0
		putDate(0,SIZECOMBINE512,SIZECOMBINE,D1024B,0);
		assertTrue(verifySuccess(SIZECOMBINE1024,Put));

		getDate(0,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE-1,Get));

		log.error("area0 data is ready,area0 is full,12Date"+SIZECOMBINE512+"1024Date:"+SIZECOMBINE1024);
		
		log.error("area1");
		//put 1 data of 512B size to area1
		putDate(1,0,1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		
		//put 1 expired data of 512B size to area1
		putDate(1,1,2,D512B,1);
		assertTrue(verifySuccess(1,Put));
	
		//put 1 data of 512B size to area1
		putDate(1,2,SIZECOMBINE512,D512B,0);
		assertTrue(verifySuccess(SIZECOMBINE512-2,Put));
		
		
		//put 511 data of 1024B size to area1
		putDate(1,SIZECOMBINE512,SIZECOMBINE,D1024B,0);
		verifySuccess(SIZECOMBINE1024,Put);
	
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE-1,Get));
		log.error("area1 data is ready,area1 is full");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,SIZECOMBINE,SIZECOMBINE+1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the else data
		getDate(0,0,SIZECOMBINE+1);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area0's other data has been readed successfully");
		
		putDate(0,SIZECOMBINE+1,SIZECOMBINE+2,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the else data
		getDate(0,0,SIZECOMBINE+2);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area0's other data has been readed successfully");
		
		//get the else data
		getDate(0,0,1);
		assertTrue(verifySuccess(0,Get));
		log.error("area0's the next oldest data has been eliminated");
		
		//get the area1's data
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE-1,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 01");
	}
	public void testAreaEliminate_02_area_full_no_slab_system_isnotfull()
	{
		log.error("start area eliminate test case 02");
		
		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster faiNEd!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 512 data of 1024B size to area0
		putDate(0,0,SIZE1024,D1024B,0);
		assertTrue(verifySuccess(SIZE1024,Put));
		getDate(0,0,SIZE1024);
		assertTrue(verifySuccess(SIZE1024,Get));
		log.error("area0 data is ready,area0 is full");
		
		log.error("area1");
		//put 511 data of 1024B size to area1
		putDate(1,0,SIZECOMBINE1024,D1024B,0);
		verifySuccess(SIZECOMBINE1024,Put);
		//put 2 data of 512B size to area1
		putDate(1,SIZECOMBINE1024,SIZECOMBINE,D512B,0);
		assertTrue(verifySuccess(SIZECOMBINE512,Put));	
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area1 data is ready,area1 is full");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,SIZE1024,SIZE1024+1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//vesify the data of area0 and area2
		getDate(0,0,SIZE1024+1);
		assertTrue(verifySuccess(SIZE1024+1,Get));
		log.error("area0's other data has been readed successfully");
		
		putDate(0,SIZE1024+1,SIZE1024+2,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put the second data to area0 ");
		
		getDate(0,0,SIZE1024+2);
		assertTrue(verifySuccess(SIZE1024+1,Get));
		log.error("area0's other data has been readed successfully");
		
		//get the else data
		getDate(0,SIZE1024,SIZE1024+1);
		assertTrue(verifySuccess(0,Get));
		log.error("the first data has been eliminated");
		
		//get the area1's data
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 02");
	}
	public void testAreaEliminate_03_area_full_no_slab_system_full()
	{
		log.error("start area eliminate test case 03");

		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,2*1024*1024);
		changeAreaQuota(csList.get(0).toString(),2,256*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 512 data of 1024B size to area0
		putDate(0,0,SIZE1024,D1024B,0);
		assertTrue(verifySuccess(SIZE1024,Put));
		getDate(0,0,SIZE1024);
		assertTrue(verifySuccess(SIZE1024,Get));
		log.error("area0 data is ready,area0 is full");
		
		log.error("area1");
		//put 511 data of 1024B size to area1
		putDate(1,0,SIZE1M512,D512B,0);
		verifySuccess(SIZE1M512,Put);
		getDate(1,0,SIZE1M512);
		assertTrue(verifySuccess(SIZE1M512,Get));
		log.error("area1 data is ready,area1 has 1M data of 512B size");
		
		log.error("area2");
		//put data ，until system is full
		putDate(2,0,SIZEFULL,D1024B,0);
		putDate(2,SIZEFULL,SIZEFULL+SIZE1M512,D512B,0);
		log.error("system mem has beeb full");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,SIZE1024,SIZE1024+1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//vesify the data of area0 and area2
		getDate(0,0,SIZE1024+1);
		assertTrue(verifySuccess(SIZE1024+1,Get));
		log.error("area0's other data has been readed successfully");
		
		//get the area1's data
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE-1,Get));
		log.error("area1's data has been readed successfully");
		getDate(1,0,1);
		assertTrue(verifySuccess(0,Get));
		log.error("area1's firse data has been eliminated");
		
		putDate(0,SIZE1024+1,SIZE1024+2,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put the second data to area0 ");
		
		getDate(0,0,SIZE1024+2);
		assertTrue(verifySuccess(SIZE1024+1,Get));
		log.error("area0's other data has been readed successfully");
		getDate(0,SIZE1024,SIZE1024+1);
		assertTrue(verifySuccess(0,Get));
		log.error("area0's first put data has been eliminated");
		
		log.error("end area eliminate test case 03");
	}
	
	public void testAreaEliminate_04_area_isnotfull_system_isnotfull()
	{
		log.error("start area eliminate test case 04");
		
		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,2*1024*1024);
		changeAreaQuota(csList.get(0).toString(),1,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		log.error("area0 has no data");
		
		log.error("area1");
		//put 511 data of 1024B size to area1
		putDate(1,0,SIZECOMBINE1024,D1024B,0);
		verifySuccess(SIZECOMBINE1024,Put);
		//put 2 data of 512B size to area1
		putDate(1,SIZECOMBINE1024,SIZECOMBINE,D512B,0);
		assertTrue(verifySuccess(SIZECOMBINE512,Put));	
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area1 data is ready,area1 is full");
		
		log.error("**verify start**");
		//put 2M data to area0
		putDate(0,0,3*SIZE1024,D1024B,0);
		assertTrue(verifySuccess(3*SIZE1024,Put));
		log.error("put data to area0 ");
		
		//get the area0's data
		getDate(0,0,3*SIZE1024);
		assertTrue(verifySuccess(3*SIZE1024,Get));
		log.error("area0's data has been readed successfully");
		
		//get the area1's data
		getDate(1,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 04");
	}
	
	public void testAreaEliminate_05_area_isnotfull_has_slabdata_system_isfull()
	{
		log.error("start area eliminate test case 05");

		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,2*1024*1024);
		changeAreaQuota(csList.get(0).toString(),2,256*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 1 data of 512B size to area0
		putDate(0,0,2,D512B,0);
		assertTrue(verifySuccess(2,Put));
		
		//put 510 data of 1024B size to area0
		putDate(0,2,3,D1024B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("area0's data is ready ,area0 is not full");
	
		log.error("area1");
		//put 511 data of 1024B size to area1
		putDate(1,0,SIZE1M512,D512B,0);
		verifySuccess(SIZE1M512,Put);
		getDate(1,0,SIZE1M512);
		assertTrue(verifySuccess(SIZE1M512,Get));
		log.error("area1 data is ready,area1 has 1M data of 512B size");
		
		log.error("area2");
		//put data ，until system is full
		putDate(2,0,SIZEFULL,D1024B,0);
		putDate(2,SIZEFULL,SIZEFULL+SIZE1M512,D512B,0);
		log.error("system mem has beeb full");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,3,4,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the area0's data
		getDate(0,0,4);
		assertTrue(verifySuccess(3,Get));
		log.error("area0's data has been readed successfully");
		
		getDate(0,0,1);
		assertTrue(verifySuccess(0,Get));
		log.error("area0's fiset data of 512B size has been eliminated");
		
		//get the area1's data
		getDate(1,0,SIZE1M512);
		assertTrue(verifySuccess(SIZE1M512,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 05");
	}
	public void testAreaEliminate_06_area_isnotfull_hasno_slabdata_system_isfull()
	{
		log.error("start area eliminate test case 06");

		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,2*1024*1024);
		changeAreaQuota(csList.get(0).toString(),2,256*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");	
		//put 510 data of 1024B size to area0
		putDate(0,0,1,D1024B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("area0's data is ready ,area0 is not full");
	
		log.error("area1");
		//put 511 data of 1024B size to area1
		putDate(1,0,SIZE1M512,D512B,0);
		verifySuccess(SIZE1M512,Put);
		getDate(1,0,SIZE1M512);
		assertTrue(verifySuccess(SIZE1M512,Get));
		log.error("area1 data is ready,area1 has 1M data of 512B size");
		
		log.error("area2");
		//put data ，until system is full
		putDate(2,0,SIZEFULL,D1024B,0);
		putDate(2,SIZEFULL,SIZEFULL+SIZE1M512,D512B,0);
		log.error("system mem has beeb full");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,1,2,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");		
		//get the area0's data
		getDate(0,0,2);
		assertTrue(verifySuccess(2,Get));
		log.error("area0's data has been readed successfully");
		
		//put 1 data of 512B size to area0
		putDate(0,2,3,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put the second data to area0 ");
		
		getDate(0,0,3);
		assertTrue(verifySuccess(2,Get));
		log.error("area0's data  has been readed successfully");
		
		getDate(0,1,2);
		assertTrue(verifySuccess(0,Get));
		log.error("area0's fiset data of 512B size has been eliminated");
		
		
		//get the area1's data
		getDate(1,0,SIZE1M512);
		assertTrue(verifySuccess(SIZE1M512-1,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 06");
	}
	public void testAreaEliminate_07_area_bigger_than_quota_area_isfull_has_slabdate()
	{
		log.error("start area eliminate test case 07");

		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,1*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 1 data of 512B size to area0
		putDate(0,0,SIZECOMBINE1024,D1024B,0);
		assertTrue(verifySuccess(SIZECOMBINE1024,Put));
		
		getDate(0,0,SIZECOMBINE1024);
		assertTrue(verifySuccess(SIZECOMBINE1024,Get));
		log.error("area0's data is ready ,area0 is not full");
	
		log.error("area1");
		//put 1 data of 1024B size to area1
		putDate(1,0,1,D1024B,0);
		assertTrue(verifySuccess(1,Put));
		//put 1 data of 1024KB size to area1
		putDate(1,1,2,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		getDate(1,0,2);
		assertTrue(verifySuccess(2,Get));
		log.error("area1's data has been ready");
		
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,SIZECOMBINE1024,SIZECOMBINE1024+1,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the area0's data
		getDate(0,0,SIZECOMBINE1024+1);
		assertTrue(verifySuccess(SIZECOMBINE1024+1,Get));
		log.error("area0's data has been readed successfully");
		
		//put 1 data of 512B size to area0
		putDate(0,SIZECOMBINE1024+1,SIZECOMBINE1024+2,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put the second data to area0 ");
		
		getDate(0,0,SIZECOMBINE1024+2);
		assertTrue(verifySuccess(SIZECOMBINE1024+1,Get));
		log.error("area0's data  has been readed successfully");
		
		getDate(0,SIZECOMBINE1024,SIZECOMBINE1024+1);
		assertTrue(verifySuccess(0,Get));
		log.error("area0's fiset data of 1024KB size has been eliminated");
		
		
		//get the area1's data
		getDate(1,0,2);
		assertTrue(verifySuccess(2,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 07");
	}
	public void testAreaEliminate_08_area_bigger_than_quota_area_isfull_has_no_slabdate()
	{
		log.error("start area eliminate test case 08");

		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,1*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 512 data of 512B size to area0
		putDate(0,0,SIZE1024,D1024B,0);
		assertTrue(verifySuccess(SIZE1024,Put));
		
		getDate(0,0,SIZE1024);
		assertTrue(verifySuccess(SIZE1024,Get));
		log.error("area0's data is ready ,area0 is full");
	
		log.error("area1");
		//put 1 data of 1024B size to area1
		putDate(1,0,1,D1024B,0);
		assertTrue(verifySuccess(1,Put));
		//put 1 data of 1024KB size to area1
		putDate(1,1,2,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		getDate(1,0,2);
		assertTrue(verifySuccess(2,Get));
		log.error("area1's data has been ready");
		
		log.error("area2");
		//put data ，until system is full
		putDate(2,0,SIZEFULL,D1024B,0);
		putDate(2,SIZEFULL,SIZEFULL+SIZE1M512,D512B,0);
		log.error("system mem has beeb full");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,SIZE1024,SIZE1024+1,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the area0's data
		getDate(0,0,SIZE1024+1);
		assertTrue(verifySuccess(SIZE1024+1,Get));
		log.error("area0's data has been readed successfully");
		
		putDate(0,SIZE1024+1,SIZE1024+2,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the area0's data
		getDate(0,0,SIZE1024+2);
		assertTrue(verifySuccess(SIZE1024+1,Get));
		log.error("area0's data has been readed successfully");
		
		//get the area1's data
		getDate(1,0,2);
		assertTrue(verifySuccess(1,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 08");
	}
	public void testAreaEliminate_09_area_bigger_than_quota_area_isnotfull_has_no_slabdate_system_nofull()
	{
		log.error("start area eliminate test case 09");
		
		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,512*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 512 data of 512B size to area0
		putDate(0,0,SIZECOMBINE1024,D1024B,0);
		assertTrue(verifySuccess(SIZECOMBINE1024,Put));
		
		getDate(0,0,SIZECOMBINE1024);
		assertTrue(verifySuccess(SIZECOMBINE1024,Get));
		log.error("area0's data is ready ,area0 is full");
	
		log.error("area1");
		//put 1 data of 1024B size to area1
		putDate(1,0,1,D1024B,0);
		assertTrue(verifySuccess(1,Put));
		//put 1 data of 1024KB size to area1
		putDate(1,1,2,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		getDate(1,0,2);
		assertTrue(verifySuccess(2,Get));
		log.error("area1's data has been ready");
		
		log.error("**verify start**");
		//put 1 data of 512B size to area0
		putDate(0,SIZECOMBINE1024,SIZECOMBINE1024+1,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the area0's data
		getDate(0,0,SIZECOMBINE1024+1);
		assertTrue(verifySuccess(SIZECOMBINE1024+1,Get));
		log.error("area0's data has been readed successfully");
		
		remDate(0,SIZECOMBINE1024,SIZECOMBINE1024+1);
		assertTrue(verifySuccess(1,Rem));
		log.error("rem one data to area0 ");
		putDate(0,SIZECOMBINE1024,SIZECOMBINE,D512B,0);
		assertTrue(verifySuccess(SIZECOMBINE512,Put));
		log.error("put one data to area0 ");
		
		getDate(0,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area0's data has been readed successfully");
		
		putDate(0,SIZECOMBINE,SIZECOMBINE+1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		getDate(0,0,SIZECOMBINE+1);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area0's quota has not changed");
		
		//get the area1's data
		getDate(1,0,2);
		assertTrue(verifySuccess(2,Get));
		log.error("area1's data has been readed successfully");
		
		log.error("end area eliminate test case 09");
	}
	
	public void testAreaEliminate_10_area_bigger_than_quota_area_isnotfull_has_no_slabdate_system_full()
	{
		log.error("start area eliminate test case 10");

		log.error("**modify conf**");
		//set quota 0:512K 1:512K
		changeAreaQuota(csList.get(0).toString(),0,512*1024);
		changeAreaQuota(csList.get(0).toString(),1,2*1024*1024);
		changeAreaQuota(csList.get(0).toString(),2,256*1024*1024);
		log.error("quota has been setted!");
		
		log.error("**start server**");
		//start cluster
		if(!control_cluster(csList, dsList, AreaTestBaseCase.start, 0))fail("start cluster failed!");
		log.error("start cluster successful!");
		
		waitto(AreaTestBaseCase.down_time);
		
		log.error("**perpare data**");
		log.error("area0");
		//put 512 data of 512B size to area0
		putDate(0,0,SIZECOMBINE1024,D1024B,0);
		assertTrue(verifySuccess(SIZECOMBINE1024,Put));
		
		getDate(0,0,SIZECOMBINE1024);
		assertTrue(verifySuccess(SIZECOMBINE1024,Get));
		log.error("area0's data is ready ,area0 is not full");
	
		log.error("area1");
		//put 1 data of 1024B size to area1
		putDate(1,0,1,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		//put 1 data of 1024KB size to area1
		putDate(1,1,SIZE1M512+1,D512B,0);
		assertTrue(verifySuccess(SIZE1M512,Put));
		getDate(1,0,SIZE1M512+1);
		assertTrue(verifySuccess(SIZE1M512+1,Get));
		log.error("area1's data has been ready");
		
		log.error("area2");	
		//put data ，until system is full
		putDate(2,0,SIZEFULL/1024,D1MB,0);
		log.error("system mem has beeb full");
		
		log.error("**verify start**");
		//put 1 data of 1MB size to area0
		putDate(0,SIZECOMBINE1024,SIZECOMBINE1024+1,D1MB,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		//get the area0's data
		getDate(0,0,SIZECOMBINE1024+1);
		assertTrue(verifySuccess(SIZECOMBINE1024+1,Get));
		log.error("area0's data has been readed successfully");
		
		remDate(0,SIZECOMBINE1024,SIZECOMBINE1024+1);
		assertTrue(verifySuccess(1,Rem));
		log.error("rem one data to area0 ");
		
		//clean the system catch
		remDate(2,0,SIZEFULL/1024+SIZE1M512);
		log.error("rem system data of area2 ");
		
		putDate(0,SIZECOMBINE1024,SIZECOMBINE,D512B,0);
		assertTrue(verifySuccess(SIZECOMBINE512,Put));
		log.error("put one data to area0 ");
		
		getDate(0,0,SIZECOMBINE);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area0's data has been readed successfully");
		
		putDate(0,SIZECOMBINE,SIZECOMBINE+1,D512B,0);
		assertTrue(verifySuccess(1,Put));
		log.error("put one data to area0 ");
		
		getDate(0,0,SIZECOMBINE+1);
		assertTrue(verifySuccess(SIZECOMBINE,Get));
		log.error("area0's quota has not changed");
		
		//get the area1's data
		getDate(1,0,SIZE1M512+1);
		assertTrue(verifySuccess(SIZE1M512,Get));
		log.error("area1's data has been readed successfully");
		getDate(1,0,1);
		assertTrue(verifySuccess(0,Get));
		log.error("area1's first 1MB data has been eliminated");
		
		log.error("end area eliminate test case 10");
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
