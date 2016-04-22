#include "easy_io.h"
#include <easy_test.h>
#include <easy_time.h>
#include <easy_kfc_handler.h>
#include <sys/types.h>
#include <sys/wait.h>

/**
 * 测试 easy_kfc_handler
 */
///////////////////////////////////////////////////////////////////////////////////////////////////
TEST(easy_kfc_handler, easy_kfc_set_iplist)
{
    char                    *config = "10.1[7-8][1-3,985-5].4.1[1-3,5]6 role=server group=group1 port=80";
    easy_kfc_t              *kfc = easy_kfc_create(config, 0);

    if (kfc) {
        easy_kfc_destroy(kfc);
    }
}

