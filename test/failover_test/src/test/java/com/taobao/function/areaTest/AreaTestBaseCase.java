package com.taobao.function.areaTest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.ibm.staf.STAFResult;
import com.taobao.tairtest.BaseTestCase;
import com.taobao.tairtest.FailOverBaseCase;


public class AreaTestBaseCase extends BaseTestCase {
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
	/**
	 * @param area 
	 * @param startIndex
	 * @param endIndex
	 * @param datasize
	 * @param expire
	 * @return
	 */
	public boolean putDate(int area,int startIndex,int endIndex,int datasize,int expire)
	{
		log.debug("start put data");
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./putData.sh "+area+" "+startIndex+" "+endIndex+" "+datasize+" "+expire;
		STAFResult result=executeShell(stafhandle, "local", cmd);

		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	
	/**
	 * @param area 
	 * @param startIndex
	 * @param endIndex
	 * @return
	 */
	public boolean getDate(int area,int startIndex,int endIndex)
	{
		log.debug("start get data");
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./getData.sh "+area+" "+startIndex+" "+endIndex;
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	/**
	 * @param area 
	 * @param startIndex
	 * @param endIndex
	 * @return
	 */
	public boolean remDate(int area,int startIndex,int endIndex)
	{
		log.debug("start rem data");
		boolean ret=false;
		String cmd="cd "+test_bin+" && ";
		cmd+="./remData.sh "+area+" "+startIndex+" "+endIndex;
		STAFResult result=executeShell(stafhandle, "local", cmd);
		if(result.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	public boolean verifySuccess(int successNum,String PutOrGetOrRem){
		int ret=0;
		String verify="cd "+test_bin+" && ";
		verify+="tail -4 "+PutOrGetOrRem+".log|grep \""+PutOrGetOrRem+"\"|awk -F \" \" \'{print $2}\'";
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
	public boolean changeAreaQuota(String machine,int area,int value)
	{
		String confname=tair_bin+"etc/group.conf";
		String key="_areaCapacity_list="+area+",";
		log.debug("change file:"+confname+" on "+machine+" key="+key+" value="+value);
		boolean ret=false;
		String cmd="sed -i \"s/"+key+".*$/"+key+value+";/\" "+confname+" && grep \""+key+".*$\" "+confname+"|awk -F\",\" \'{print $2}\'";
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
			ret=false;
		else
		{
			String stdout=getShellOutput(result);
			if(stdout.trim().equals(value+";"))
				ret=true;
			else
				ret=false;
		}
		return ret;
	}
	
	public boolean changeHourRange(String machine,String expired_slab,int startHour,int endHour)
	{
		String confname=tair_bin+"etc/dataserver.conf";
		String key="check_"+expired_slab+"_hour_range=";
		log.debug("change file:"+confname+" on "+machine+" key="+key+" value="+startHour+"-"+endHour);
		boolean ret=false;
		String cmd="sed -i \"s/"+key+".*$/"+key+startHour+"-"+endHour+"/\" "+confname+" && grep \""+key+".*$\" "+confname+"|awk -F\"=\" \'{print $2}\'";
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
			ret=false;
		else
		{
			String stdout=getShellOutput(result);
			if(stdout.trim().equals(startHour+"-"+endHour))
				ret=true;
			else
				ret=false;
		}
		return ret;
	}
	
	public int checkSlab(String machine,int slab_size)
	{
		int ret=0;
		String cmd="cd /home/admin/tair_bin/bin && ./mdbshm_reader.py -f /dev/shm/mdb_shm_path01|grep \"^"+slab_size+"\"|awk -F \" \" \'{print $4}\'";
		log.debug("check slab item count:"+cmd+" on "+machine+" in slab:"+slab_size);
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
			ret=-1;
		else
		{
			String stdout=getShellOutput(result);
			try{
			ret=(new Integer(stdout.trim())).intValue();
			log.error("time="+ret);
			}catch(Exception e)
			{
				log.debug("get verify exception: "+stdout);
				ret=-1;
			}
		}
		return ret;
	}
	/**
	 * @param machine machine which you want to operate
	 * @param confname file which you want to modify , should include full path
	 * @param key key which you want to modify
	 * @param value value which you want to change 
	 * @return
	 */
	public boolean modify_config_file(String machine,String confname,String key,String value)
	{
		log.debug("change file:"+confname+" on "+machine+" key="+key+" value="+value);
		boolean ret=false;
		String cmd="sed -i \"s/"+key+"=.*$/"+key+"="+value+"/\" "+confname+" && grep \""+key+"=.*$\" "+confname+"|awk -F\"=\" \'{print $2}\'";
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
			ret=false;
		else
		{
			String stdout=getShellOutput(result);
			if(stdout.trim().equals(value))
				ret=true;
			else
				ret=false;
		}
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
		if(opID.equals(AreaTestBaseCase.stop)&&type==1)
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
			if (opID.equals(AreaTestBaseCase.start)&&(new Integer(stdout.trim())).intValue()!=3)
			{
				ret=false;
			}
			else if(opID.equals(AreaTestBaseCase.stop)&&(new Integer(stdout.trim())).intValue()!=2)
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
		if(opID.equals(AreaTestBaseCase.stop)&&type==1)
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
			if (opID.equals(AreaTestBaseCase.start)&&(new Integer(stdout.trim())).intValue()!=3)
			{
				ret=false;
			}
			else if(opID.equals(AreaTestBaseCase.stop)&&(new Integer(stdout.trim())).intValue()!=2)
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
		cmd+="./clean.sh";
		STAFResult rst=executeShell(stafhandle,machine ,cmd );
		if(rst.rc!=0)
			ret=false;
		else
			ret=true;
		return ret;
	}
	public boolean execute_data_verify_tool()
	{
		log.debug("start verify tool");
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
	public int check_process(String machine,String prname)
	{
		log.debug("check process "+prname+" on "+machine);
		int ret=0;
		String cmd="ps -ef|grep "+prname+"|wc -l";
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if (result.rc!=0)
			ret=-1;
		else
		{
			String stdout=getShellOutput(result);
			try {
				ret = (new Integer(stdout.trim())).intValue();
			} catch (Exception e) {
				log.debug("get verify exception: "+stdout);
				ret=-1;
			}
		}
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
	public int check_keyword(String machine,String keyword,String logfile)
	{
		int ret=0;
		String cmd="grep \""+keyword+"\" "+logfile+" |wc -l";
		log.debug("check keyword:"+cmd+" on "+machine+" in file:"+logfile);
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
			ret=-1;
		else
		{
			String stdout=getShellOutput(result);
			try{
			ret=(new Integer(stdout.trim())).intValue();
			log.error("time="+ret);
			}catch(Exception e)
			{
				log.debug("get verify exception: "+stdout);
				ret=-1;
			}
		}
		return ret;
	}
	public int touch_file(String machine, String file) {
		int ret=0;
		String cmd="touch " + file;
		log.debug("touch file:"+file+" on "+machine);
		STAFResult result=executeShell(stafhandle, machine, cmd);
		if(result.rc!=0)
			ret=-1;
		return ret;
	}
}
