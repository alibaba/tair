/**
 * 通过Easy KFC 接口批量测试 Eas 接口性能
 * 主要指标计算公式:
 *   QPS:     成功的query数目 / 测试耗时
 *   max RT:  最大的成功的query RT
 *   min RT:  最小的成功的query RT
 *   mean RT: 成功的query RT总和 / 成功的query数目
 *
 * Usage: ./easy_kfc_load [<options>]
 */

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <math.h>
#include <algorithm>
//get ip adrress head
#include <stdio.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>

#include "easy_kfc_handler.h"


using namespace         std;

static const char       *DEFAULT_GROUP     = "test2";     // 缺省server组名
static const char       *DEFAULT_HOSTNAME  = "127.0.0.1"; // 缺省hostname
static const char       *DEFAULT_PORT      = "2200";     // 缺省port
static const int        DEFAULT_TIMEOUT   = 300;      // 缺省超时毫秒数

static int              DEFAULT_MSG_LEN   = 1024;     // 缺省最大消息长度
static bool             g_verbose = false;      // 是否详细输出
static unsigned         g_req     = 0xFFFFFFFF; // 请求次数
static unsigned         g_rt_threshold = 2000;  // long rt threshhold 点，2000 微秒
static int              g_childNum = 0;         // 实际的子进程数
static int              g_exit    = false;      // 进程是否退出

// query统计信息
struct SQueryStatis {
    time_t                  beforeTestTime;        // 测试前计时
    time_t                  afterTestTime;         // 测试后计时

    struct timeval          totalResponseTime;// query的总耗时
    unsigned                maxTime;             // 成功query最大耗时，微秒数
    unsigned                minTime;             // 成功query最小耗时，微秒数

    unsigned                totalCount;            // query次数
    unsigned                successCount;          // 成功query数目
    unsigned                failedCount;         // 失败query数目，是指通讯成功，但返回的状态码不是0
    unsigned                sendTimeoutCount;    // 发送超时数目
    unsigned                recvTimeoutCount;    // 接收超时数目
    unsigned                unknownErrorCount;   // 未知错误数目

    double                  RTStandardDeviation;
    unsigned                longRTCount;
    uint64_t                RT_50, RT_66, RT_75, RT_80, RT_90, RT_95, RT_98, RT_99;

    // 初始化统计信息
    SQueryStatis() :
        maxTime(0), minTime(1000000), totalCount(0), successCount(0), failedCount(0),
        sendTimeoutCount(0), recvTimeoutCount(0), unknownErrorCount(0), RTStandardDeviation(0), longRTCount(0),
        RT_50(0), RT_66(0), RT_75(0), RT_80(0), RT_90(0), RT_95(0), RT_98(0), RT_99(0)

    {
        totalResponseTime.tv_usec = 0;
        totalResponseTime.tv_sec  = 0;
    }
};

// 时间到
static void timeToStop(int sig)
{
    g_exit = true;
}

// Get Local IP Address
static string getIpAddress()
{
    uint64_t                address[16];
    easy_addr_t             addr;
    char                    buffer[64];
    int                     i, cnt;

    buffer[0] = '\0';
    cnt = easy_inet_hostaddr(address, 16, 0);
    memset(&addr, 0, sizeof(addr));

    for(i = 0; i < cnt; i++) {
        addr.u.addr = address[i];
        easy_inet_addr_to_str(&addr, buffer, 64);
    }

    return buffer;
}

//Send Queries to Easy KFC Server. timeout is sending timeout value.
static void testQueries(easy_kfc_agent_t *ka,  int timeout, SQueryStatis &statis)
{
    //unsigned count = 100000;
    //g_req = count;

    char                    data1[DEFAULT_MSG_LEN];
    char                    data2[DEFAULT_MSG_LEN];
    memset(data1, 'A', DEFAULT_MSG_LEN);
    data1[DEFAULT_MSG_LEN - 1] = '\0';


    // 开始查询
    //pid_t pid = getpid();

    struct timeval          beforeQueryTime, afterQueryTime, responseTime;
    //int i = 0;

    vector<uint64_t> mRTVector; //store all the response time;

    while (!g_exit && statis.totalCount < g_req) {
        statis.totalCount++;

        bool                    isOK = false;
        gettimeofday(&beforeQueryTime, NULL);

        if (easy_kfc_send_message(ka, data1, DEFAULT_MSG_LEN, timeout) == EASY_OK) {
            if (g_verbose) {
                cout << "Send MSG: " << string(data1) << endl;
            }

            if (easy_kfc_recv_message(ka, data2, DEFAULT_MSG_LEN) > 0) {
                if (g_verbose) {
                    cout << "Recv MSG: " << string(data2) << endl;
                }
            } else {
                cout << "error when receiving msg" << endl;
                statis.unknownErrorCount++;
                statis.failedCount++;
                continue;
            }
        } else {
            cout << "error when sending msg" << endl;

            if (ka->kfc->eio->stoped) break;

            statis.unknownErrorCount++;
            statis.failedCount++;
            continue;
        }

        statis.successCount++;
        isOK = true;

        gettimeofday(&afterQueryTime,  NULL);

        timersub(&afterQueryTime, &beforeQueryTime, &responseTime);

        if (isOK) {
            timeradd(&(statis.totalResponseTime), &responseTime, &(statis.totalResponseTime));
            mRTVector.push_back(responseTime.tv_sec * 1000000 + responseTime.tv_usec);

            unsigned                timeUsed = responseTime.tv_sec * 1000000 + responseTime.tv_usec;

            if (statis.maxTime < timeUsed) statis.maxTime = timeUsed;

            if (statis.minTime > timeUsed) statis.minTime = timeUsed;

            if (timeUsed > g_rt_threshold)
                statis.longRTCount++;
        } // end of isOK
    }//end of while

    uint64_t                mTotalRT = 0;

    for (vector<uint64_t>::iterator iter = mRTVector.begin(); iter != mRTVector.end(); ++iter)
        mTotalRT += *iter;

    uint64_t                averageRT = (uint64_t)(mTotalRT / (double)statis.successCount);
    uint64_t                totalDeviation = 0;

    for (vector<uint64_t>::iterator iter = mRTVector.begin(); iter != mRTVector.end(); ++iter) {
        totalDeviation += (*iter - averageRT) * (*iter - averageRT);
    }

    statis.RTStandardDeviation = sqrt(totalDeviation / (double)statis.successCount);
    sort(mRTVector.begin(), mRTVector.end());
    vector<uint64_t>::size_type v_size = mRTVector.size();

    if (v_size <= 1)
        return;

    statis.RT_50 = mRTVector[v_size / 2 - 1];
    statis.RT_66 = mRTVector[(int)(v_size * 0.66) - 1];
    statis.RT_75 = mRTVector[(int)(v_size * 0.75) - 1];
    statis.RT_80 = mRTVector[(int)(v_size * 0.8) - 1];
    statis.RT_90 = mRTVector[(int)(v_size * 0.9) - 1];
    statis.RT_95 = mRTVector[(int)(v_size * 0.95) - 1];
    statis.RT_98 = mRTVector[(int)(v_size * 0.98) - 1];
    statis.RT_99 = mRTVector[(int)(v_size * 0.99) - 1];
}



// 用法显示
static void usage(const char *exe)
{
    cout << "Usage: " << exe << " [<options>]" << endl;
    cout << "Where <options> are:" << endl;
    cout << " -h <server hostname>     The hostname/ip of the server, the default hostname is \"127.0.0.1\"" << endl;
    cout << " -p <server port>         The port of the server listen, the default port is \"2200\"" << endl;
    cout << " -g <server group>        The group name of the server, the default group name is \"test2\"" << endl;

    cout << " -b <bytes to send>       The bytes of send message size, the default is 1024" << endl;
    cout << " -t <timeout>             The timeout in milliseconds, the default is 300" << endl;
    cout << " -c <concurrency>         Number of multiple requests to make, default is 1" << endl;
    cout << " -m <munites limit>       Limit the running time in munites" << endl;
    cout << " -n <number  limit>       Limit the query requests in number of every process, default is 0xFFFFFFFF " << endl;
    cout << " -l <threshold limit>     Threshold value of response time, RT bigger than this value will be counted as long RT. Default is 2000" << endl;
    cout << " -v                       Show verbose detail of process" << endl;
}

// 文件名
inline static void genFileName(char f[], pid_t pid)
{
    sprintf(f, "/tmp/%u.dat", pid);
}

// 输出一个进程的统计结果，以pid命名
static void writeStatis(SQueryStatis &statis, double totalTestTime, double totalResponseTime)
{
    char                    f[80];
    genFileName(f, getpid());
    ofstream fOut(f);

    if (fOut) {
        fOut.write((const char *)&statis.totalCount,         sizeof(statis.totalCount));
        fOut.write((const char *)&statis.successCount,       sizeof(statis.successCount));
        fOut.write((const char *)&statis.failedCount,      sizeof(statis.failedCount));
        fOut.write((const char *)&statis.sendTimeoutCount, sizeof(statis.sendTimeoutCount));
        fOut.write((const char *)&statis.recvTimeoutCount, sizeof(statis.recvTimeoutCount));
        fOut.write((const char *)&statis.unknownErrorCount, sizeof(statis.unknownErrorCount));
        fOut.write((const char *)&totalTestTime,  sizeof(totalTestTime));
        fOut.write((const char *)&totalResponseTime, sizeof(totalResponseTime));
        fOut.write((const char *)&statis.maxTime, sizeof(statis.maxTime));
        fOut.write((const char *)&statis.minTime, sizeof(statis.minTime));

        fOut.write((const char *)&statis.longRTCount, sizeof(statis.longRTCount));
        fOut.write((const char *)&statis.RTStandardDeviation, sizeof(statis.RTStandardDeviation));
        fOut.write((const char *)&statis.RT_50, sizeof(statis.RT_50));
        fOut.write((const char *)&statis.RT_66, sizeof(statis.RT_66));
        fOut.write((const char *)&statis.RT_75, sizeof(statis.RT_75));
        fOut.write((const char *)&statis.RT_80, sizeof(statis.RT_80));
        fOut.write((const char *)&statis.RT_90, sizeof(statis.RT_90));
        fOut.write((const char *)&statis.RT_95, sizeof(statis.RT_95));
        fOut.write((const char *)&statis.RT_98, sizeof(statis.RT_98));
        fOut.write((const char *)&statis.RT_99, sizeof(statis.RT_99));

    } else {
        cout << "** Failed to write file " << f << " **" << endl;
    }
}

// 总的统计信息
static SQueryStatis     g_statis;        // 包括所有子进程的总的统计信息
static double           g_totalTestTime  = 0;  // 总的测试时间
static double           g_totalResponseTime = 0;  // 总的query耗时

// 合并统计信息
static void mergeStatis(pid_t pid)
{
    char                    f[80];
    genFileName(f, pid);
    ifstream fIn(f);

    if (fIn) {
        // 读入某个进程的统计数据
        SQueryStatis            statis;
        double                  totalTestTime, totalResponseTime;

        fIn.read((char *)&statis.totalCount,         sizeof(statis.totalCount));
        fIn.read((char *)&statis.successCount,       sizeof(statis.successCount));
        fIn.read((char *)&statis.failedCount,      sizeof(statis.failedCount));
        fIn.read((char *)&statis.sendTimeoutCount, sizeof(statis.sendTimeoutCount));
        fIn.read((char *)&statis.recvTimeoutCount, sizeof(statis.recvTimeoutCount));
        fIn.read((char *)&statis.unknownErrorCount, sizeof(statis.unknownErrorCount));
        fIn.read((char *)&totalTestTime,  sizeof(totalTestTime));
        fIn.read((char *)&totalResponseTime, sizeof(totalResponseTime));
        fIn.read((char *)&statis.maxTime, sizeof(statis.maxTime));
        fIn.read((char *)&statis.minTime, sizeof(statis.minTime));

        fIn.read((char *)&statis.longRTCount, sizeof(statis.longRTCount));
        fIn.read((char *)&statis.RTStandardDeviation, sizeof(statis.RTStandardDeviation));
        fIn.read((char *)&statis.RT_50, sizeof(statis.RT_50));
        fIn.read((char *)&statis.RT_66, sizeof(statis.RT_66));
        fIn.read((char *)&statis.RT_75, sizeof(statis.RT_75));
        fIn.read((char *)&statis.RT_80, sizeof(statis.RT_80));
        fIn.read((char *)&statis.RT_90, sizeof(statis.RT_90));
        fIn.read((char *)&statis.RT_95, sizeof(statis.RT_95));
        fIn.read((char *)&statis.RT_98, sizeof(statis.RT_98));
        fIn.read((char *)&statis.RT_99, sizeof(statis.RT_99));

        fIn.close();
        remove(f);

        // 合并
        g_statis.totalCount       += statis.totalCount;
        g_statis.successCount     += statis.successCount;
        g_statis.failedCount      += statis.failedCount;
        g_statis.sendTimeoutCount += statis.sendTimeoutCount;
        g_statis.recvTimeoutCount += statis.recvTimeoutCount;
        g_statis.unknownErrorCount += statis.unknownErrorCount;
        g_totalResponseTime       += totalResponseTime;
        g_statis.longRTCount      += statis.longRTCount;
        g_statis.RTStandardDeviation += statis.RTStandardDeviation;

        if (g_statis.maxTime < statis.maxTime)
            g_statis.maxTime = statis.maxTime;

        if (g_statis.minTime > statis.minTime)
            g_statis.minTime = statis.minTime;

        if (g_totalTestTime  < totalTestTime)
            g_totalTestTime  = totalTestTime;

        if (g_statis.RT_50 < statis.RT_50) g_statis.RT_50 = statis.RT_50;

        if (g_statis.RT_66 < statis.RT_66) g_statis.RT_66 = statis.RT_66;

        if (g_statis.RT_75 < statis.RT_75) g_statis.RT_75 = statis.RT_75;

        if (g_statis.RT_80 < statis.RT_80) g_statis.RT_80 = statis.RT_80;

        if (g_statis.RT_90 < statis.RT_90) g_statis.RT_90 = statis.RT_90;

        if (g_statis.RT_95 < statis.RT_95) g_statis.RT_95 = statis.RT_95;

        if (g_statis.RT_98 < statis.RT_98) g_statis.RT_98 = statis.RT_98;

        if (g_statis.RT_99 < statis.RT_99) g_statis.RT_99 = statis.RT_99;

    } else {
        cout << "-- Failed to read file " << f << " --" << endl;
    }
}

// 子进程退出处理
static void childExitHandler(int sig)
{
    int                     pid, status;

    signal(sig, childExitHandler);

    while (1) {
        pid = waitpid (WAIT_ANY, &status, WNOHANG);

        switch (pid) {
        case -1:
            if (errno != ECHILD) {
                cout << "waitpid error: " << errno << endl;
            }

            /* goto case 0 */
        case 0:
            /* no child exits */
            return;

        default:
            // 有子进程退出了
            mergeStatis(pid);
            g_childNum--;
            break;
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    // 取参数
    const char              *group          = DEFAULT_GROUP;
    const char              *hostname       = DEFAULT_HOSTNAME;
    const char              *port           = DEFAULT_PORT;

    int                     timeout        = DEFAULT_TIMEOUT;
    int                     maxMsgSize     = 0;
    int                     childNum       = 0;
    int                     limit          = 0;
    unsigned                number         = 0;

    while (true) {
        int                     c = getopt(argc, argv, "g:b:t:c:m:h:p:n:l:v");

        if (c == -1) break;

        switch (c) {
        case 'h':
            hostname = optarg;
            break;

        case 'p':
            port   = optarg;
            break;

        case 'n':
            number = strtoul(optarg, NULL, 10);
            g_req  = number;
            break;

        case 'g':
            group   = optarg;
            break;

        case 'b':
            maxMsgSize      = atoi(optarg);
            DEFAULT_MSG_LEN = maxMsgSize;
            break;

        case 't':
            timeout = atoi(optarg);
            break;

        case 'c':
            childNum = atoi(optarg);
            break;

        case 'm':
            limit    = atoi(optarg);
            break;

        case 'l':
            g_rt_threshold    = atoi(optarg);
            break;

        case 'v':
            g_verbose = true;
            break;

        case '?':
        case ':':
        default:
            break;
        }
    }

    char                    config[1024];

    //config file from options.
    /*
       strcpy(config, "127.0.0.1 role=server group=test2 port=2200\n"
            "127.0.0.1 role=client group=test2 port=2200\n");
     */
    sprintf(config, "%s role=server group=%s port=%s\n%s role=client group=%s port=%s\n",
            hostname, group, port, getIpAddress().c_str(), group, port);
    printf("config is:\n%s", config);

    if (childNum > 1) {
        // fork子进程
        signal(SIGCHLD, childExitHandler);
        bool                    isChild = false;

        for (int i = 0; i < childNum; i++) {
            pid_t                   pid = fork();

            if (pid == 0) {
                // child
                isChild = true;
                break;
            } else if ((int)pid < 0) {
                // error
                cout << "Failed to fork #" << i << endl;
                break;
            } else {
                // parent
                g_childNum++;
            }
        }

        if (!isChild) {
            // 父进程，等待子进程退出，进行数据统计
            childExitHandler(SIGCHLD); // 处理在设置handler之前退出的子进程

            while (g_childNum > 0) {
                sleep(1);
            }

            // 显示统计结果
            cout << "Test total count :  " << g_statis.totalCount   << endl;
            cout << "Test success: " << g_statis.successCount << endl;
            cout << "Test failed:  " << g_statis.failedCount << endl;
            cout << "Send timeout: " << g_statis.sendTimeoutCount << endl;
            cout << "Recv timeout: " << g_statis.recvTimeoutCount << endl;
            cout << "Unknow error: " << g_statis.unknownErrorCount << endl;
            cout << "Test time:    " << g_totalTestTime     << " seconds." << endl;
            cout << "Query time:   " << g_totalResponseTime    << " seconds." << endl;
            //cout << "QPS:          " << g_statis.successCount/g_totalResponseTime << endl;
            cout << "QPS:          " << g_statis.successCount / g_totalTestTime << endl;

            printf("Bandwidth:  %.2f MB/s\n", ((uint64_t)g_statis.successCount * DEFAULT_MSG_LEN) / (1024 * 1024 * g_totalTestTime));
            printf("Timeout rate: %.2f%%\n", (double)g_statis.recvTimeoutCount * 100 / g_statis.totalCount);
            printf("Error rate:   %.2f%%\n", (double)g_statis.failedCount * 100 / g_statis.totalCount);

            if (g_statis.successCount > 0) {
                printf("RTS max:  0.%06u   RTS min:  0.%06u   RTS mean:  %.6f  seconds  Long RT:%d.\n",
                       g_statis.maxTime, g_statis.minTime, g_totalResponseTime / g_statis.successCount, g_statis.longRTCount);
                cout << "RT Standard Deviation: " << (double)g_statis.RTStandardDeviation / 1000000 << " seconds." << endl;
            } else {
                cout << "** NO RT data available **" << endl;
            }

            printf("Percentage of the requests served within a certain time (s)\n");
            cout << "50\%   " << (double)g_statis.RT_50 / 1000000 << endl;
            cout << "66\%   " << (double)g_statis.RT_66 / 1000000 << endl;
            cout << "75\%   " << (double)g_statis.RT_75 / 1000000 << endl;
            cout << "80\%   " << (double)g_statis.RT_80 / 1000000 << endl;
            cout << "90\%   " << (double)g_statis.RT_90 / 1000000 << endl;
            cout << "95\%   " << (double)g_statis.RT_95 / 1000000 << endl;
            cout << "98\%   " << (double)g_statis.RT_98 / 1000000 << endl;
            cout << "99\%   " << (double)g_statis.RT_99 / 1000000 << endl;
            cout << "100\%  " << (double)g_statis.maxTime / 1000000 << "(longest request)" << endl;
            exit(0);
        }
    }

    // 设置限时
    if (limit > 0) {
        signal(SIGALRM, timeToStop);
        limit                   *= 60;
        alarm(limit);
    }


    easy_kfc_t              *kfc;
    kfc = easy_kfc_create(config, 1);
    easy_kfc_start(kfc);

    easy_kfc_agent_t        *ka = easy_kfc_join_client(kfc, "test2");

    if (ka == NULL) {
        printf("Failed to join client");
        return -1;
    }

    // 测试
    SQueryStatis            statis; // 统计结果
    time(&statis.beforeTestTime);
    testQueries(ka, timeout, statis);
    time(&statis.afterTestTime);

    easy_eio_stop(kfc->eio);
    easy_kfc_wait(kfc);
    easy_kfc_destroy(kfc);

    double                  totalTestTime = difftime(statis.afterTestTime, statis.beforeTestTime);
    double                  totalResponseTime = statis.totalResponseTime.tv_sec + statis.totalResponseTime.tv_usec / 1000000.0;

    if (childNum > 1) {
        // 子进程输出统计结果
        writeStatis(statis, totalTestTime, totalResponseTime);
    } else {
        // 显示统计结果
        cout << "Test total count :  " << statis.totalCount   << endl;
        cout << "Test success: " << statis.successCount << endl;
        cout << "Test failed:  " << statis.failedCount << endl;
        cout << "Send timeout: " << statis.sendTimeoutCount << endl;
        cout << "Recv timeout: " << statis.recvTimeoutCount << endl;
        cout << "Unknow error: " << statis.unknownErrorCount << endl;
        cout << "Test time:    " << totalTestTime     << " seconds." << endl;
        cout << "Query time:   " << totalResponseTime    << " seconds." << endl;
        //cout << "QPS:          " << statis.successCount/totalResponseTime << endl;
        cout << "QPS:          " << statis.successCount / totalTestTime << endl;

        printf("Bandwidth:  %.2f MB/s\n", ((uint64_t)statis.successCount * DEFAULT_MSG_LEN) / (1024 * 1024 * totalTestTime));
        printf("Timeout rate: %.2f%%\n", (double)statis.recvTimeoutCount * 100 / statis.totalCount);
        printf("Error rate:   %.2f%%\n", (double)statis.failedCount * 100 / statis.totalCount);

        if (statis.successCount > 0) {
            //printf("max RT:  %.6f   min RT:  %.6f   mean RT:  %.6f  seconds.\n", statis.maxTime/1000000.0, statis.minTime/1000000.0, totalResponseTime/statis.successCount);
            printf("RTS max:  0.%06u   RTS min:  0.%06u   RTS mean:  %.6f  seconds  Long RT: %d.\n",
                   statis.maxTime, statis.minTime, totalResponseTime / statis.successCount, statis.longRTCount);
            cout << "RT Standard Deviation: " << (double)statis.RTStandardDeviation / 1000000 << " seconds." << endl;
        } else {
            cout << "** NO RT data available **" << endl;
        }

        printf("Percentage of the requests served within a certain time (s)\n");
        cout << "50\%   " << (double)statis.RT_50 / 1000000 << endl;
        cout << "66\%   " << (double)statis.RT_66 / 1000000 << endl;
        cout << "75\%   " << (double)statis.RT_75 / 1000000 << endl;
        cout << "80\%   " << (double)statis.RT_80 / 1000000 << endl;
        cout << "90\%   " << (double)statis.RT_90 / 1000000 << endl;
        cout << "95\%   " << (double)statis.RT_95 / 1000000 << endl;
        cout << "98\%   " << (double)statis.RT_98 / 1000000 << endl;
        cout << "99\%   " << (double)statis.RT_99 / 1000000 << endl;
        cout << "100\%  " << (double)statis.maxTime / 1000000 << "(longest request)" << endl;
    }

    return 0;
}

