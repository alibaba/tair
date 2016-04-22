#include <getopt.h>
#include <easy_io.h>
#include <easy_time.h>
#include "easy_kfc_handler.h"

/*************************************************************************************************
 * 函数定义部分
 *************************************************************************************************/
#define DATA_LEN 128
easy_atomic_t           process_cnt = 0;

static int server_process(easy_request_t *r)
{
    easy_kfc_packet_t       *ipacket = (easy_kfc_packet_t *)r->ipacket;
    easy_kfc_packet_t       *opacket;

    opacket = easy_kfc_packet_rnew(r, ipacket->len);
    memcpy(opacket->data, ipacket->data, ipacket->len);
    *((uint64_t *)opacket->data) = ipacket->chid;
    opacket->len = ipacket->len;
    opacket->chid = ipacket->chid;
    r->opacket = opacket;

    return EASY_OK;
}

void do_server(easy_kfc_t *kfc)
{
    if (easy_kfc_join_server(kfc, "test2", server_process) == EASY_OK) {
        easy_kfc_allow_client(kfc, "test2", "*", 1);
        easy_kfc_start(kfc);
    }
}

void *do_client(void *args)
{
    easy_kfc_t              *kfc = (easy_kfc_t *)args;
    easy_kfc_agent_t        *ka = easy_kfc_join_client(kfc, "test2");

    if (ka == NULL) {
        easy_error_log("easy_kfc_join_client is failure.");
        return (void *)NULL;
    }

    easy_kfc_choice_scheduler(ka, EASY_KFC_CHOICE_RT);
    char                    data1[DATA_LEN];
    char                    data2[DATA_LEN];
    memset(data1, 'A', DATA_LEN);

    while(kfc->eio->stoped == 0) {
        if (easy_kfc_send_message(ka, data1, DATA_LEN, 1000) == EASY_OK)
            if (easy_kfc_recv_message(ka, data2, DATA_LEN) > 0)
                easy_atomic_inc(&process_cnt);
    }

    easy_kfc_leave_client(ka);
    return (void *)NULL;
}

void *do_stat(void *args)
{
    easy_kfc_t              *kfc = (easy_kfc_t *)args;
    int64_t                 e, s = easy_time_now();

    while(kfc->eio->stoped == 0) {
        sleep(1);
        e = easy_time_now();
        double                  speed = process_cnt * 1000000 / (e - s);
        fprintf(stderr, "QPS: %.2f\n", speed);
    }

    return (void *)NULL;
}

int main(int argc, char **argv)
{
    char                    config[256];
    char                    buffer[64];
    easy_kfc_t              *kfc;
    int                     i, cnt;
    uint64_t                address[16];
    easy_addr_t             addr;

    if (argc != 2) {
        fprintf(stderr, "%s server|client_cnt\n", argv[0]);
        return 1;
    }

    // config
    cnt = easy_inet_hostaddr(address, 16, 0);
    memset(&addr, 0, sizeof(addr));

    for(i = 0; i < cnt; i++) {
        easy_inet_atoe(&address[i], &addr);
        easy_inet_addr_to_str(&addr, buffer, 64);
        lnprintf(config, 256, "%s role=server group=test2 port=2200\n"
                 "%s role=client group=test2 port=2200\n", buffer, buffer);
    }

    // server
    if (strcmp(argv[1], "server") == 0) {
        kfc = easy_kfc_create(config, 0);
        do_server(kfc);
    } else {
        kfc = easy_kfc_create(config, 1);
        easy_kfc_start(kfc);

        cnt = atoi(argv[1]);

        if (cnt <= 0) cnt = 1;

        pthread_t tids[cnt + 1];

        for(i = 0; i < cnt; i++) {
            pthread_create(&tids[i], NULL, do_client, kfc);
        }

        pthread_create(&tids[i], NULL, do_stat, kfc);

        for(i = 0; i < cnt; i++) {
            pthread_join(tids[i], NULL);
        }

        easy_eio_stop(kfc->eio);
        pthread_join(tids[cnt], NULL);
    }

    easy_kfc_wait(kfc);
    easy_kfc_destroy(kfc);

    return 0;
}
