#include <iostream> 
#include <string> 
#include <cstdlib> 
#include <fstream> 

using namespace std; 

#include "file_list_factory.hpp"
#include "sync_packet.hpp"
#include "sync_packet_list.hpp"

using namespace tair;
using namespace tair::common; 

int test_record_file(int argc,char *argv[])
{
  char pkey[1024];

  RecordLogFile<request_sync> m_disklist_file;
  m_disklist_file.start("sync.list");

  for(int i=0;i<100;i++)
  {
    sprintf(pkey,"test_key_%d",i);
    int pkeysize = strlen(pkey);
    data_entry key(pkey, pkeysize, false);
    request_sync node(0,pkey);
    m_disklist_file.append(&node);
  }
  cout << "file size=" << m_disklist_file.get_count() << endl;

  request_sync   _pNode;
  do
  {
    if(!m_disklist_file.get(_pNode)) break;
    cout << "key=" << _pNode.key.get_data()<< endl;
  }while(1);

  return 0;
}

int main(int argc,char *argv[])
{
  int success=0;
  int failed=0;
  sync_packet_list m_tosend_queue;
  if(!m_tosend_queue.enable_disk_save("/tmp"))
  {
    cout << "enable disk save failed" << endl;
    return -1;
  }

  char pkey[1024];
  for(int i=0;i<1000;i++)
  {
    if(i==10)
    {
      cout << "do switch " << endl;
    }

    sprintf(pkey,"test_key_%d",i);
    int pkeysize = strlen(pkey);
    data_entry key(pkey, pkeysize, false);
    request_sync *pnode=new request_sync(0,pkey);
    int rt=m_tosend_queue.put(pnode);
    if(0==rt)
    {
      success++;
    }
    else
    {
      failed++;
      log_info("put node failed :%d\n", rt);
    }
  }

  fprintf(stderr,"success:%d,failed:%d\n", success,failed);
}
