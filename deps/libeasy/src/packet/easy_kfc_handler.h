#ifndef EASY_KFC_HANDLER_H_
#define EASY_KFC_HANDLER_H_

#include <easy_define.h>

/**
 * 类似KFC的方式
 */
EASY_CPP_START

#include <easy_io.h>
#include <easy_client.h>
#include <easy_connection.h>
#include <easy_summary.h>

#define EASY_ERR_NO_MEM         (-101)
#define EASY_ERR_NO_SERVER      (-102)
#define EASY_ERR_ALL_DOWN       (-103)
#define EASY_ERR_SERVER_BUSY    (-104)
#define EASY_ERR_NO_SEND        (-105)
#define EASY_ERR_TIMEOUT        (-106)
#define EASY_KFC_HIST_CNT       6
#define EASY_KFC_CHOICE_RR      0
#define EASY_KFC_CHOICE_RT      1

typedef struct easy_kfc_t        easy_kfc_t;
typedef struct easy_kfc_group_t  easy_kfc_group_t;
typedef struct easy_kfc_node_t   easy_kfc_node_t;
typedef struct easy_kfc_agent_t  easy_kfc_agent_t;
typedef struct easy_kfc_packet_t easy_kfc_packet_t;
typedef struct easy_kfc_saddr_t  easy_kfc_saddr_t;
typedef struct easy_kfc_server_t easy_kfc_server_t;
typedef struct easy_kfc_client_t easy_kfc_client_t;
typedef int (easy_kfc_choice_server_pt)(easy_kfc_agent_t *agent);

struct easy_kfc_t {
    easy_pool_t             *pool;
    easy_io_t               *eio;
    easy_io_handler_pt      chandler;
    easy_atomic32_t         gen_chid;
    uint64_t                version;
    easy_atomic_t           lock;
    easy_array_t            *node_array;
    easy_hash_t             *node_list;
    easy_hash_t             *group_list;
    int                     iocnt;
    int                     hist_idx;
    ev_timer                hist_watcher;
    easy_summary_t          *hist[EASY_KFC_HIST_CNT];
    uint32_t                noping : 1;
};

struct easy_kfc_server_t {
    uint64_t                group_id;
    easy_thread_pool_t      *etp;
    easy_io_process_pt      *process;
    easy_request_process_pt *cproc;
    void                    *args;
    easy_hash_t             *client_ip;
    int                     client_allow;
    pthread_rwlock_t        rwlock;
};

struct easy_kfc_client_t {
    uint64_t                ip : 63;
    uint64_t                allow : 1;
    easy_hash_list_t        node;
};

struct easy_kfc_group_t {
    uint64_t                group_id;
    easy_addr_t             server_addr;
    easy_kfc_server_t       *server;
    easy_hash_t             *server_list;
    easy_hash_t             *client_list;
    easy_hash_list_t        node;
    int                     role;
};

struct easy_kfc_node_t {
    easy_addr_t             addr;
    easy_hash_list_t        node;
    easy_hash_list_t        node_list;
    int16_t                 status;
    int16_t                 connected;
    uint32_t                rt;
    uint64_t                lastrt;
};

struct easy_kfc_saddr_t {
    uint32_t                cur, cnt;
    easy_kfc_node_t         **addr;
};

struct easy_kfc_agent_t {
    easy_pool_t             *pool;
    uint64_t                group_id;
    uint64_t                version;
    easy_kfc_saddr_t        slist;
    easy_session_t          *s;
    easy_kfc_t              *kfc;
    easy_client_wait_t      wobj;
    int                     offset;
    int                     status;
    easy_kfc_choice_server_pt *choice_server;
    easy_kfc_node_t         *last;
};

struct easy_kfc_packet_t {
    easy_buf_t              *b;
    char                    *data;
    int32_t                 len;
    uint32_t                chid;
    uint64_t                group_id;
    char                    buffer[0];
};

/**
 * 创建一个kfc
 *
 * @param: ip_list - ip的列表
 * @param: iocnt   - io线程数, client端一般用1个就够了
 * @return: easy_kfc_t的结构, 如果返回NULL说明创建出错了
 */
easy_kfc_t *easy_kfc_create(const char *ip_list, int iocnt);
/**
 * 起动kfc线程
 *
 * @param: kfc    - easy_kfc_t的结构
 * @return: int   - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_start(easy_kfc_t *kfc);
/**
 * 等kfc线程退出, 相当于pthread_join
 *
 * @param: kfc    - easy_kfc_t的结构
 * @return: int   - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_wait(easy_kfc_t *kfc);
/**
 * 销毁easy_kfc_t对象
 *
 * @param: kfc    - easy_kfc_t的结构
 */
void easy_kfc_destroy(easy_kfc_t *kfc);

/**
 * 动态更新ip列表
 *
 * @param: kfc     - easy_kfc_t的结构
 * @param: ip_list - ip的列表
 * @return: int    - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_set_iplist(easy_kfc_t *kfc, const char *ip_list);
/**
 * 用request上的pool分配出一个kfc_packet出来
 *
 * @param:  easy_request_t - request
 * @param:  size           - packet中data大小
 * @return: easy_kfc_packet_t - packet, 如果失败返回NULL
 */
easy_kfc_packet_t *easy_kfc_packet_rnew(easy_request_t *r, int size);
/**
 * 注册服务器
 *
 * @param:  kfc            - easy_kfc_t的结构
 * @param:  group_name     - 组名, 不能大于8字节
 * @param:  process        - 处理函数
 * @return: int            - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_join_server(easy_kfc_t *kfc, const char *group_name, easy_io_process_pt *process);
/**
 * 注册服务器
 *
 * @param:  kfc            - easy_kfc_t的结构
 * @param:  group_name     - 组名, 不能大于8字节
 * @param:  process        - 处理函数
 * @param:  process        - 处理函数的参数
 * @return: int            - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_join_server_args(easy_kfc_t *kfc, const char *group_name, easy_request_process_pt *process, void *args);
/**
 * 注册服务器, 使用工作线程
 *
 * @param:  kfc            - easy_kfc_t的结构
 * @param:  group_name     - 组名, 不能大于8字节
 * @param:  request_cnt    - 工作线程数
 * @param:  process        - 工作线程函数
 * @return: int            - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_join_server_async(easy_kfc_t *kfc, const char *group_name,
                               int request_cnt, easy_request_process_pt *request_process);
/**
 * 连接到一个组上
 *
 * @param:  kfc            - easy_kfc_t的结构
 * @param:  group_name     - 组名, 不能大于8字节
 * @return: easy_kfc_agent_t - agent结构
 */
easy_kfc_agent_t *easy_kfc_join_client(easy_kfc_t *kfc, const char *group_name);
/**
 * 断开
 *
 * @param:  easy_kfc_agent_t - agent结构
 * @return: int              - EASY_OK 成功
 */
int easy_kfc_leave_client(easy_kfc_agent_t *agent);
/**
 * 发送数据到组上
 *
 * @param:  easy_kfc_agent_t - agent结构
 * @param:  data             - 数据
 * @param:  len              - 数据大小
 * @param:  timeout          - 超时时间(单位: ms)
 * @return: int              - EASY_OK 成功, EASY_ERROR 失败
 */
int easy_kfc_send_message(easy_kfc_agent_t *agent, char *data, int len, int timeout);
/**
 * 接收数据
 *
 * @param:  easy_kfc_agent_t - agent结构
 * @param:  data             - buffer
 * @param:  len              - 最大buffer大小
 * @return: int              - 接收到的数据长度, EASY_ERROR(-1) 失败
 */
int easy_kfc_recv_message(easy_kfc_agent_t *agent, char *data, int len);

/**
 * 接收,不进行memcpy,直接使用session上的内存
 *
 * 例子:
 *  int len;
 *  char *data = NULL;
 *  // 发送
 *  while (easy_kfc_send_message(agent, "", 0, 0) == EASY_OK) {
 *       // 等待响应, 把buffer的指针放到data上
 *       if ((len = easy_kfc_recv_buffer(agent, &data)) > 0) {
 *           process....
 *       }
 *       // 清掉buffer
 *       easy_kfc_clear_buffer(agent);
 *   }
 *
 * @param:  easy_kfc_agent_t - agent结构
 * @param:  data             - 返回的buffer指针
 * @return: int              - 接收到的数据长度, EASY_ERROR(-1) 失败
 */
int easy_kfc_recv_buffer(easy_kfc_agent_t *agent, char **data);

/**
 * 把agent中的sesion数据清掉
 *
 * @param:  easy_kfc_agent_t - agent结构
 */
void easy_kfc_clear_buffer(easy_kfc_agent_t *agent);

/**
 * 增加client到ip上
 *
 * @param: easy_kfc_t       - kfc
 * @param: group_name       - 组名
 * @param: client           - ip或hostname
 * @param: deny             - 1 - deny, 0 - allow
 */
void easy_kfc_allow_client(easy_kfc_t *kfc, char *group_name, char *client, int deny);

/**
 * 选择server的算法类型, rt, rr
 *
 * @param:  easy_kfc_agent_t - agent结构
 * @param:  type             - 类型, EASY_KFC_CHOICE_RR, EASY_KFC_CHOICE_RT
 */
void easy_kfc_choice_scheduler(easy_kfc_agent_t *agent, int type);

EASY_CPP_END

#endif
