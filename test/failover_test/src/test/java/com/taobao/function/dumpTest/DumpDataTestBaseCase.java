package com.taobao.function.dumpTest;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.ibm.staf.STAFResult;
import com.taobao.function.areaTest.AreaTestBaseCase;
import com.taobao.tairtest.BaseTestCase;

public class DumpDataTestBaseCase extends BaseTestCase {
	//directory
	final static String tair_bin="/home/admin/tair_bin/";
	final static String test_bin="/home/admin/baoni/function/";
	//mechine
	final String csarr[]=new String[]{"10.232.4.26","10.232.4.27"};
	final String dsarr[]=new String[]{"10.232.4.26"};
	final List csList=Arrays.asList(csarr);
	final List dsList=Arrays.asList(dsarr);
	//Parameters
	final static int down_time=4;
	//Server Operation
	final static String start="start";
	final static String stop="stop";
	//Tool Option
	final static String put="put";
	final static String get="get";
	final static String rem="rem";
	
	final static String Put="Put";
	final static String Get="Get";
	final static String Rem="Rem";
	//插入数据
	/**
	 * @param area namespace值
	 * @param startIndex 开始的key值
	 * @param endIndex 结束的key值
	 * @param operation 操作类型（put、incr、decr、putItem）
	 * @return
	 */
	public boolean putDate(int area,int startIndex,int endIndex,int expire,int operation)
	{
		log.debug("start put data");
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./putDumpData.sh "+area+" "+startIndex+" "+endIndex+" "+expire+" "+operation;
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	//area全局dump数据
	
	public boolean setDumpInfo(int area,String startDate,String EndDate)
	{
		log.debug("sed dump info");
		//读取dump数据并排序
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./prepareDumpInfo.sh"+area+" \""+startDate+"\" "+EndDate+"\"";
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}

	/**
	 * @param area namespace值
	 * @param startDate 开始时间
	 * @param EndDate 结束时间
	 * @return
	 */
	public boolean areaDump()
	{
		log.debug("start dump data");
		//读取dump数据并排序
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./dumpData.sh";
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	
	public boolean verifySuccessNum(int area,int num)
	{
		int ret=0;
		String verify="cd "+test_bin+" && ";
		verify+="cat dumpData.log|grep \"area = 0\"|wc -l";
		STAFResult result=executeShell(stafhandle, "local", verify);
		if(result.rc!=0)
		{
			ret=-1;
		}
		else
		{
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
				log.debug("num="+ret);
			} catch (Exception e) {
				log.debug("get verify exception: "+stdout);
				ret = -1;
			}
		}
		if(ret==num)
			return true;
		else
		{
			log.debug("except= "+num+" actul="+ret);
			return false;
		}
	}
	public boolean verifySuccess(int successNum){
		int ret=0;
		String verify="cd "+test_bin+" && ";
		verify+="tail -4 Put.log|grep Put|awk -F \" \" \'{print $2}\'";
		STAFResult result=executeShell(stafhandle, "local", verify);
		if(result.rc!=0)
		{
			ret=-1;
		}
		else
		{
			String stdout = getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
				log.debug("num="+ret);
			} catch (Exception e) {
				log.debug("get verify exception: "+stdout);
				ret = -1;
			}
		}
		if(ret==successNum)
			return true;
		else
		{
			log.debug("except= "+successNum+" actul="+ret);
			return false;
		}
	}
	public boolean verifyDate(int area,int startIndex,int endIndex,String operation)
	{
		log.debug("start verify tool");
		//读取dump数据并排序
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./batchData.sh";
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	/**
	 * @param machine
	 * @param opID
	 * @param type 0:normal 1:force
	 * @return
	 */
	public boolean control_cs(String machine,String opID, int type)
	{
		log.debug("control cs:"+machine+" "+opID+" type="+type);
		boolean ret=false;
		String cmd="cd "+tair_bin+" && ";
		cmd+="./tair.sh "+opID+"_cs && ";
		cmd+="sleep 5";
		if(opID.equals(DumpDataTestBaseCase.stop)&&type==1)
			cmd="killall -9 tair_cfg_svr && sleep 2";
		executeShell(stafhandle, machine, cmd);
		cmd="ps -ef|grep tair_cfg_svr|wc -l";
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
		{
			log.debug("cs rc!=0");
			ret=false;
		}
		else
		{
			String stdout=getShellOutput(result);
			log.debug("------------cs ps result--------------"+stdout);
			if (opID.equals(DumpDataTestBaseCase.start)&&(new Integer(stdout.trim())).intValue()!=3)
			{
				ret=false;
			}
			else if(opID.equals(DumpDataTestBaseCase.stop)&&(new Integer(stdout.trim())).intValue()!=2)
			{
				ret=false;
			}
			else
			{
				ret=true;
			}
		}
		return ret;
	}
	/**
	 * @param machine
	 * @param opID
	 * @param type 0:normal 1:force
	 * @return
	 */
	public boolean control_ds(String machine,String opID, int type)
	{
		log.debug("control ds:"+machine+" "+opID+" type="+type);
		boolean ret=false;
		String cmd="cd "+tair_bin+" && ";
		cmd+="./tair.sh "+opID+"_ds && ";
		cmd+="sleep 5";
		if(opID.equals(DumpDataTestBaseCase.stop)&&type==1)
			cmd="killall -9 tair_server && sleep 2";
		executeShell(stafhandle, machine, cmd);
		cmd="ps -ef|grep tair_server";
		STAFResult result=executeShell(stafhandle, machine, cmd);
		log.debug("*********ps s*********");
		log.debug(getShellOutput(result).trim());
		log.debug("*********ps e*********");
		cmd="ps -ef|grep tair_server|wc -l";
		result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
		{
			log.debug("ds rc!=0");
			ret=false;
		}
		else
		{
			String stdout=getShellOutput(result);
			log.debug("------------ds ps result--------------"+stdout);
			if (opID.equals(DumpDataTestBaseCase.start)&&(new Integer(stdout.trim())).intValue()!=3)
			{
				ret=false;
			}
			else if(opID.equals(DumpDataTestBaseCase.stop)&&(new Integer(stdout.trim())).intValue()!=2)
			{
				ret=false;
			}
			else
			{
				ret=true;
			}
		}
		return ret;
	}
	/**
	 * @param cs_group
	 * @param opID
	 * @param type 0:normal 1:force
	 * @return
	 */
	public boolean batch_control_cs(List cs_group,String opID, int type)
	{
		boolean ret=false;
		for(Iterator it=cs_group.iterator();it.hasNext();)
		{
			if(!control_cs((String)it.next(), opID, type))
			{
				ret=false;
				break;
			}
			else
				ret=true;
		}
		return ret;
	}
	/**
	 * @param ds_group
	 * @param opID
	 * @param type 0:normal 1:force
	 * @return
	 */
	public boolean batch_control_ds(List ds_group,String opID, int type)
	{
		boolean ret=false;
		for(Iterator it=ds_group.iterator();it.hasNext();)
		{
			if(!control_ds((String)it.next(), opID, type))
			{
				ret=false;
				break;
			}
			else
				ret=true;
		}
		return ret;
	}
	/**
	 * @param cs_group
	 * @param ds_group
	 * @param opID
	 * @param type 0:normal 1:force
	 * @return
	 */
	public boolean control_cluster(List cs_group,List ds_group,String opID, int type)
	{
		boolean ret=false;
		if(!batch_control_ds(ds_group, opID, type)||!batch_control_cs(cs_group, opID, type))
			ret=false;
		else
			ret=true;
		return ret;
	}
	public boolean clean_data(String machine)
	{
		boolean ret=false;
		String cmd="cd "+tair_bin+" && ";
		cmd+="./tair.sh clean";
		STAFResult rst=executeShell(stafhandle, machine, cmd);
		if(rst.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	public boolean clean_tool(String machine)
	{
		boolean ret=false;
		killall_tool_proc();
		String cmd="cd "+test_bin+" && ";
		cmd+="./dumpClean.sh";
		STAFResult rst=executeShell(stafhandle,machine ,cmd );
		if(rst.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	public boolean killall_tool_proc()
	{
		log.debug("force kill all data tool process");
		boolean ret=false;
		String cmd="killall -9 tair3test";
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else 
			ret=true;
		return ret;
	}
	public boolean reset_cluster(List csList,List dsList)
	{
		boolean ret=false;
		log.debug("stop and clean cluster!");
		if(control_cluster(csList, dsList, stop, 1)&&batch_clean_data(csList)&&batch_clean_data(dsList))
			ret=true;
		return ret;
	}
	public boolean batch_clean_data(List machines)
	{
		boolean ret=true;
		for(Iterator it=machines.iterator();it.hasNext();)
		{
			if(!clean_data((String)it.next()))ret=false;
		}
		return ret;
	}
}
