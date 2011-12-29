package com.taobao.ldbtest;
import org.junit.Test;
import java.util.Date;

public class Failover_Migrate_Performance_tools extends FailOverBaseCase
{
  /*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    @Test
   public void testFailover_ldbtest_delete_namespace()
   {
     log.error("start the test case of ldbtest_delete_namespace");
     int waitcnt=0;
     if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0)) fail("start cluster failed!");
     log.error("wait system initialize ...");
     waitto(10);
     log.error("Start Cluster Successful!");

     //write the data
     //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     //change test tool's configuration
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
       fail("modify configure file failed");
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
       fail("modify configure file failed");
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
       fail("modify configure file failed");
                   
     execute_data_verify_tool();
                                 
     //check verify
     while(check_process("local", "DataDebug")!=2)
     {
      waitto(2);
      if(++waitcnt>150)break;
     }
     if(waitcnt>150)fail("put data time out!");
     waitcnt=0;


     // verify get result
     int datacnt=getVerifySuccessful();
     assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);  
     log.error("Write data over!");
     //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


     //read the data
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
       fail("modify configure file failed");
     //need to check data 
     log.error("after delete the namespace to check the data");
     execute_data_verify_tool();
     //check verify
     while(check_process("local", "DataDebug")!=2)
     {
       waitto(5);
       if(++waitcnt>300)break;
      }
     if(waitcnt>300)fail("Read data time out!");
     waitcnt=0;
     log.error("Read data over!");
                
     //verify get result
     log.error("datacnt value:" +datacnt + "getVerifySuccessful value:" + getVerifySuccessful());
     assertEquals("verify data failed!",datacnt, getVerifySuccessful());
     log.error("Successfully verified data!");
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

     
     //delete the namespace
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     log.error("start to delete the namespace");
     if(!ldb_delete_namespace((String) csList.get(0))) fail("delete namespace failure");
     log.error("delete the namespace successful");
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            
     
     //read the data
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     log.error("after delete the namespace to check the data");
     execute_data_verify_tool();
     //check verify
     while(check_process("local", "DataDebug")!=2)
     {
       waitto(5);
       if(++waitcnt>300)break;
     }
     if(waitcnt>300)fail("Read data time out!");
     waitcnt=0;
     log.error("Read data over!");

     //verify get result
     log.error("datacnt value:" +datacnt + "getVerifySuccessful value:" + getVerifySuccessful());
     //assertEquals("verify data failed!",0, getVerifySuccessful());
     log.error("Successfully verified data!");
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     

     //write the data
     //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     //change test tool's configuration
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "put"))
       fail("modify configure file failed");
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "datasize", FailOverBaseCase.put_count))
       fail("modify configure file failed");
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "filename", "read.kv"))
       fail("modify configure file failed");
                         
     execute_data_verify_tool();
                      
     //check verify
     while(check_process("local", "DataDebug")!=2)
     {
       waitto(2);
       if(++waitcnt>150)break;
     }
     if(waitcnt>150)fail("put data time out!");
     waitcnt=0;
               
                             
     // verify get result
     datacnt=getVerifySuccessful();
     log.error("datacnt=" + datacnt);
     assertTrue("put successful rate small than 90%!",datacnt/FailOverBaseCase.put_count_float>0.9);
     log.error("Write data over!");
     //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

     //read the data
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
     if(!modify_config_file("local", FailOverBaseCase.test_bin+"DataDebug.conf", "actiontype", "get"))
       fail("modify configure file failed");
     //need to check data 
     log.error("after delete the namespace to check the data");
     execute_data_verify_tool();
     //check verify
     while(check_process("local", "DataDebug")!=2)
     {
      waitto(5);
      if(++waitcnt>300)break;
     }
     if(waitcnt>300)fail("Read data time out!");
     waitcnt=0;
     log.error("Read data over!");
  
     //verify get result
     log.error("datacnt value:" +datacnt + "getVerifySuccessful value:" + getVerifySuccessful());
     assertEquals("verify data failed!",datacnt, getVerifySuccessful());
     log.error("Successfully verified data!");
     //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

     //end test
     log.error("end test case of ldbtest_delete_namespace");
   }
    
   >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> */
    
    @Test
   public void testFailover__Migrate_Performance_Test()
   {
     log.error("Start the Migrate_Performance_tools!");
     //if add_ds=1 mine add ds!
     int add_ds = 1;
     String init_recordcount="10485760";
     String init_operationcount="10485760";
     String ycsb_workload_fieldlength="1024";
     String Data_bach_size="1048576";
     int loop_count=0;
     while(loop_count<3)
      {
        loop_count=loop_count+1;
        String ycsb_workload_recordcount=Integer.parseInt(init_recordcount)*loop_count+"";
        String ycsb_workload_operationcount=Integer.parseInt(init_operationcount)*loop_count+"";
	    //print the ycsb config
        log.error("loop_count=" +loop_count);
        log.error("recordcount=" +ycsb_workload_recordcount);
        log.error("operationcount=" +ycsb_workload_operationcount);
        log.error("fieldlength=" +ycsb_workload_fieldlength);

         //modify the workload file
         //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        log.error("start to modify workload keyword");
        if(!modify_config_file("local", FailOverBaseCase.ycsb_workload_path, "recordcount", ycsb_workload_recordcount))
        {
          log.error("modify workload key word of [recordcount]!");
          fail("modify workload key word of [recordcount]!");
         }
         log.error("Need to modify workload key word of [operationcount]");
         if(!modify_config_file("local", FailOverBaseCase.ycsb_workload_path, "operationcount", ycsb_workload_operationcount))
         {
           log.error("modify workload key word of [operationcount] failure!");
           fail("modify workload key word of [operationcount] failure!");
           }
         log.error("Need to modify workload key word of [fieldlength]");
         if(!modify_config_file("local", FailOverBaseCase.ycsb_workload_path, "fieldlength", ycsb_workload_fieldlength))
         {
           log.error("modify workload key word of [fieldlength] failure!");
           fail("modify workload key word of [fieldlength] failure");
         }
         log.error("modify the keyword successful");
         //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        int set_count=0; 
        while(set_count<3)
        {
          set_count=set_count+1;
          //modify the dataconf
          //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          log.error("start to modify the dataconf");
          String temp_bach_size="";
          if(set_count==1) temp_bach_size= Integer.parseInt(Data_bach_size)*1 + "";
          if(set_count==2) temp_bach_size= Integer.parseInt(Data_bach_size)*3 + "";
          if(set_count==3) temp_bach_size= Integer.parseInt(Data_bach_size)*5 + "";
          log.error("temp_bach_size=" + temp_bach_size);
          
          //modify the workload file
          log.error("Need to modify dataconf keyword");
          int temp_i;
          for(temp_i=0; temp_i<4; temp_i++)
          {
            if(!modify_config_file((String) dsList.get(temp_i), FailOverBaseCase.tair_bin_dataconf, "ldb_migrate_batch_size",temp_bach_size))
            {
              log.error((String) dsList.get(temp_i) + ":modify dataconf key word failure");
              fail("modify workload key word of failure!");
            }
          }
          log.error("modify the dataconf successful");
          //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


          //closed the group machines
          //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          log.error("Need close all the DS and CS!");
	      if(!control_cluster(csList, dsList, FailOverBaseCase.stop, 0))fail("start cluster failed!");
	      log.error("All the DS and CS have closed successful!");
          //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	      //move the ycsb log file
          //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          if(loop_count>1)
           {
            String log_name =loop_count-1 + "_" + set_count + "_start.res";
            if(add_ds==0) log_name=loop_count-1 + "_" + set_count +  "_stop.res";
            log.error("start to move the log file!");
            if(!ycsb_move_log("local",log_name)) fail("move ycsb log failure!");
            log.error("Move the ycsb log successful!");
           }
           waitto(5);     
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

           //clean all the data at the group machines
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	       log.error("Need to clean the group data!"); 
	       if(!ycsb_clean_data("local")) fail("clean group data failure!");
           waitto(5);
           if(!ycsb_delete_log("local")) fail("delete ycsb logfile failure!");
	       log.error("Clean the data successful!");
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	       //start the group machines then closed a ds
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	       log.error("start the group data!"); 
	       //if(!ycsb_start_group("local")) fail("start the group machines failure!");
           // waitto(10);
           reset_cluster(csList,dsList);
           if(!control_cluster(csList, dsList, FailOverBaseCase.start, 0))fail("start cluster failed!");
	       log.error("Start the group machines successful!");
           //stop a DS
           if(add_ds==1)
           {
            log.error("Need to stop a ds");
            if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("stop ds failure!");
            log.error("stop ds successful!");
           }
           waitto(5);
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	       //write the data into all the ds from ycsb tools
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	       log.error("start to Write the data from ycsb!");
	       if(!ycsb_write_data("local")) fail("ycsb write_data failure!"); 
	       log.error("check the key word in ycsb log");
           int waitcnt=0;
           waitto(10);
           while(check_keyword("local",FailOverBaseCase.ycsb_end_str,FailOverBaseCase.ycsb_log_path)<1)
           {
             waitto(5);
             waitcnt=waitcnt+1;
             if (waitcnt==10000) break;
           }
           if (waitcnt==10000)
           {
             log.error("5*10000time out,YCSB write data failure!");
             fail("5*10000time out,YCSB write data failure!");
           }
           log.error("write data done!");
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
           

	       //start or close a ds,if add_ds=0 stop a DS,if add_ds=1 start a DS
           //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
           if(add_ds==0)
           {
	         log.error("start to stop a ds");
	         if(!control_ds((String) dsList.get(1), FailOverBaseCase.stop, 0)) fail("stop ds failure!");
	         log.error("stop ds successful!");
           }
           else
           {
             log.error("start to start a ds");
             if(!control_ds((String) dsList.get(1), FailOverBaseCase.start, 0)) fail("start ds failure!");
             log.error("star the ds successful!");
           }
           //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	       //Chexking the key word of need migrate
           //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	       waitcnt=0;
	       log.error("Chexking the key word:" + FailOverBaseCase.start_migrate);
	       while(check_keyword((String)csList.get(0), FailOverBaseCase.start_migrate, FailOverBaseCase.tair_bin+"logs/config.log")<1)
	       {
	         waitto(1);
	         if(++waitcnt>600)break;
	       }
	       if (waitcnt==600)
	       {
	         log.error("The time waitfor key word of [need migrate] out");
	         fail("The time waitfor key word of [need migrate] out");
	       }
	       Date start_migrate_time=new Date();
	       log.error("start_migrate_time is:" + start_migrate_time);
           //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

	       //Chexking the key word of finish migrate
           //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	       waitcnt=0;
	       log.error("start to Check the key word:" + FailOverBaseCase.finish_migrate);
	       while(check_keyword((String)csList.get(0), FailOverBaseCase.finish_migrate, FailOverBaseCase.tair_bin+"logs/config.log")<1)
	       {
	          waitto(1);
	          if(++waitcnt>36000) break;
	        }
	       if (waitcnt==36000)
	       {
	         log.error("The time waitfor key word of [migrate all done] out");
	         fail("The time waitfor key word of [migrate all done] out");
	       }
           log.error("check the key word successful");
           //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
           

	       Date finish_migrate_time=new Date();
	       log.error("finish_migrate_time is:" + finish_migrate_time);
       
           long long_recordcount = new Long(ycsb_workload_recordcount);
           long long_fieldlength = new Long(ycsb_workload_fieldlength);
	       long all_data_count= (long_recordcount*long_fieldlength)/(1024*1024);
	       long all_time_count= (finish_migrate_time.getTime()-start_migrate_time.getTime())/1000;
	   
	       log.error("###############################################################################");
           log.error("run version:" + loop_count + "_" + set_count);
	       log.error("all data count:" + all_data_count + "M");
	       log.error("all time count:" + all_time_count+ " Second");
	       log.error("###############################################################################");
          }
        }
          log.error("end the Migrate_Performance_tools!");
   }
       public void setUp()
       {
         log.error("clean tool and cluster!");
         clean_tool("local");
         reset_cluster(csList,dsList);
         if(!batch_modify(csList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
         fail("modify configure file failure");
         if(!batch_modify(dsList, FailOverBaseCase.tair_bin+"etc/group.conf", "_copy_count", "2"))
         fail("modify configure file failure");
        }
         
       public void tearDown()
       {
         clean_tool("local");
         log.error("clean tool and cluster!");
         batch_uncomment(csList, FailOverBaseCase.tair_bin+"etc/group.conf", dsList, "#");
       }
} 








