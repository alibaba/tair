#include <stdio.h>
#include <easy_io.h>
#include <easy_uthread.h>

int uthread_main(int argc, char **argv)
{
    easy_error_log("uthread_main, done.\n");
    return 0;
}

EASY_UTHREAD_RUN_MAIN(uthread_main);
