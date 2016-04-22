#include <getopt.h>
#include <easy_io.h>
#include <easy_time.h>
#include "easy_kfc_handler.h"
#include <string.h>

#include <time.h>
#include <sys/time.h>

#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
using namespace         std;


static string           g_servername    = "127.0.0.1";
static string           g_clientname    = "127.0.0.1";
static string           g_role          = "server";
static string           g_port          = "2200";
static string           g_group         = "test2";
static string           g_config        = "";
static string           g_setiplist     = "";

static int              g_msgsize       = 1024;
static int              g_timeout       = 300;
static int              g_sleep         = 0;
static bool             g_verbose       = false;
static bool             g_recvbuff      = false;
static bool             g_serverasync   = false;

static uint64_t         g_recvnum       = 0;
static uint64_t         g_maxcount      = 0;


static int server_process(easy_request_t *r)
{
    if(g_sleep > 0) {
        usleep(g_sleep);
    }

    easy_kfc_packet_t       *ipacket = (easy_kfc_packet_t *)r->ipacket;
    easy_kfc_packet_t       *opacket;

    opacket = easy_kfc_packet_rnew(r, ipacket->len);

    memcpy(opacket->data, ipacket->data, ipacket->len);
    //*((uint64_t *)opacket->data) = ipacket->chid;
    opacket->len = ipacket->len;
    opacket->chid = ipacket->chid;
    r->opacket = opacket;

    if(g_verbose) {
        fprintf(stdout, "Get MSG Length %d \n", ipacket->len);
        fprintf(stdout, "Get MSG: %s \n", ipacket->data);
        fprintf(stdout, "Send MSG: %s \n", opacket->data);
    }

    g_recvnum ++;

    if(g_recvnum % 1000 == 0) {
        fprintf(stdout, "Got %" PRIdFAST64 " MSGs. \n", g_recvnum);
    }

    return EASY_OK;
}
static int server_process_async(easy_request_t *r, void *args)
{
    if(g_verbose) {
        fprintf(stdout, "\nserver process aync mode\n");
    }

    return server_process(r);
}

void do_server(easy_kfc_t *kfc)
{
    int                     ret;

    if(g_serverasync) {
        ret = easy_kfc_join_server_async(kfc, g_group.c_str(), 5, server_process_async);
    } else {
        ret = easy_kfc_join_server(kfc, g_group.c_str(), server_process);
    }

    if (ret == EASY_OK) {
        easy_kfc_start(kfc);
    } else {
        fprintf(stderr, "join server error!!!");
    }
}


void *do_client(void *args)
{
    easy_kfc_t              *kfc = (easy_kfc_t *)args;
    easy_kfc_agent_t        *ka = easy_kfc_join_client(kfc, g_group.c_str());

    if (ka == NULL) {
        fprintf(stderr, "join client error!!!");
        exit(-1);
    }

    char                    data1[g_msgsize];
    char                    data2[g_msgsize];
    memset(data1, '\0', g_msgsize);
    memset(data2, '\0', g_msgsize);
    data1[g_msgsize - 1] = '\0';

    char                    *data3 = NULL;
    uint64_t                count = 0;
    int                     ret;

    while(kfc->eio->stoped == 0 && count < g_maxcount) {
        struct timeval          beforetime, senttime, aftertime, responsetime;
        gettimeofday(&beforetime, NULL);

        std::stringstream ss;
        ss << count;
        const char              *data = ss.str().c_str();
        strcpy(data1, data);

        if (easy_kfc_send_message(ka, data1, g_msgsize, g_timeout) == EASY_OK) {
            gettimeofday(&senttime, NULL);

            if (g_recvbuff == true) {
                ret = easy_kfc_recv_buffer(ka, &data3 );
            } else {
                ret = easy_kfc_recv_message(ka, data2, g_msgsize);
            }

            if (ret > 0) {
                gettimeofday(&aftertime, NULL);

                if(string(data1) != string(data2))
                    cout << "SEND MSG IS DIFFERENT WITH RECV MSG" << endl;

                if(g_verbose) {
                    printf("Sent MSG:\t%s\n", data1);
                    cout << "Recv MSG:\t" << string(data2) << endl;

                    printf("ret is:\t%d\n", ret);
                    printf("Sent/Recv MSG LEN:\t%u\n", g_msgsize);

                    timersub(&aftertime, &beforetime, &responsetime);
                    unsigned                timeused = responsetime.tv_sec * 1000000 + responsetime.tv_usec;
                    printf("time used:\t%u\n", timeused);

                    timersub(&senttime, &beforetime, &responsetime);
                    unsigned                timesent = responsetime.tv_sec * 1000000 + responsetime.tv_usec;
                    printf("time sent:\t%u\n", timesent);

                    timersub(&aftertime, &senttime, &responsetime);
                    unsigned                timerecv = responsetime.tv_sec * 1000000 + responsetime.tv_usec;
                    printf("time recv:\t%u\n", timerecv);
                }
            } else {
                printf("easy_kfc_recv_message return value:\t%d\n", ret);
                //printf("easy_kfc_recv_message return value < 0");
            }
        }

        count ++;

        if(g_setiplist != "" && count == g_maxcount / 2 ) {
            char                    config[1024];
            snprintf(config, 1024, "%s",  g_setiplist.c_str() );
            printf("new config is:\n%s\n", config);

            if(EASY_ERROR == easy_kfc_set_iplist(kfc, config)) {
                fprintf(stderr, "easy kfc set iplist ERROR!!!");
                exit(-1);
            }
        }
    }

    easy_kfc_leave_client(ka);

    if (g_recvbuff == true)
        easy_kfc_clear_buffer(ka);

    return (void *)NULL;
}

static void getconfig(string &config)
{
    std::replace(config.begin(), config.end(), ';', '\n');
}

static void usage()
{
    cout << "Usage: [<options>]" << endl;
    cout << "Where <options> are:" << endl;
    cout << " -r <role server|client>  The role of running tool, the default role is server" << endl;
    cout << " -s <server hostname>     The hostname/ip of the server, the default hostname is \"127.0.0.1\"" << endl;
    cout << " -c <client hostname>     The hostname/ip of the client, the default hostname is \"127.0.0.1\"" << endl;
    cout << " -p <server port>         The port of the server listen, the default port is \"2200\"" << endl;
    cout << " -g <server group>        The group name of the server, the default group name is \"test2\"" << endl;

    cout << " -b <bytes to send>       The bytes of send message size, the default is 1024" << endl;
    cout << " -t <timeout>             The timeout in milliseconds, the default is 300" << endl;
    cout << " -n <request number>      Request count number, default is 1000" << endl;
    cout << " -l <server sleep time>   Server sleep time in micro-second, default is 0" << endl;
    cout << " -v                       Show verbose detail of process" << endl;
    cout << " -a                       Server process in async mode" << endl;
    cout << " -f <config file string>  config file, like \"127.0.0.1 role=server group=test2 port=2200;127.0.0.1 role=client group=test2 port=2200\" " << endl;
    cout << " -e <config file string>  config file, setiplist, like \"127.0.0.1 role=server group=test2 port=2200;127.0.0.1 role=client group=test2 port=2200\" " << endl;
    exit(-1);
}

int main(int argc, char **argv)
{
    char                    config[1024];
    easy_kfc_t              *kfc;

    if (argc < 3) {
        usage();
        return 1;
    }

    while (true) {
        int                     c = getopt(argc, argv, "r:s:c:p:g:b:t:n:l:f:e:vah");

        if (c == -1) break;

        switch (c) {
        case 'r':
            g_role = optarg;
            break;

        case 's':
            g_servername = optarg;
            break;

        case 'c':
            g_clientname = optarg;
            break;

        case 'p':
            g_port   = optarg;
            break;

        case 'g':
            g_group   = optarg;
            break;

        case 'b':
            g_msgsize      = atoi(optarg);
            break;

        case 't':
            g_timeout = atoi(optarg);
            break;

        case 'n':
            g_maxcount = atoi(optarg);
            break;

        case 'l':
            g_sleep = atoi(optarg);
            break;

        case 'f':
            g_config = string(optarg);
            getconfig(g_config);
            break;

        case 'e':
            g_setiplist = string(optarg);
            getconfig(g_setiplist);
            break;

        case 'a':
            g_serverasync = true;
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

    cout << "msg size is:\t"    << g_msgsize << endl;
    cout << "max count is:\t"   << g_maxcount << endl;
    cout << "timeout value:\t"  << g_timeout << endl;
    cout << "server sleep time:\t"  << g_sleep << endl;

    // config
    snprintf(config, 1024, "%s role=server group=%s port=%s\n"
             "%s role=client group=%s port=%s\n",
             g_servername.c_str(), g_group.c_str(), g_port.c_str(),
             g_clientname.c_str(), g_group.c_str(), g_port.c_str() );

    if(g_config != "")
        snprintf(config, 1024, "%s",  g_config.c_str() );

    printf("config is:\n%s\n", config);

    // server
    if (g_role == "server") {
        if ((kfc = easy_kfc_create(config, 0)) == NULL) {
            fprintf(stderr, "easy kfc create ERROR!!!\n");
            return -1;
        }

        do_server(kfc);
    } else if (g_role == "client") {
        if ((kfc = easy_kfc_create(config, 1)) == NULL) {
            fprintf(stderr, "easy kfc create ERROR!!!\n");
            return -1;
        }

        easy_kfc_start(kfc);

        do_client(kfc);
        easy_eio_stop(kfc->eio);
    } else {
        usage();
    }

    easy_kfc_wait(kfc);
    easy_kfc_destroy(kfc);

    return 0;
}
