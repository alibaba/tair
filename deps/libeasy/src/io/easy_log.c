#include <easy_log.h>
#include <easy_time.h>
#include <pthread.h>
#include "easy_io.h"

easy_log_print_pt       easy_log_print = easy_log_print_default;
easy_log_format_pt      easy_log_format = easy_log_format_default;
easy_log_level_t        easy_log_level = EASY_LOG_WARN;

/**
 * 设置log的打印函数
 */
void  easy_log_set_print(easy_log_print_pt p)
{
    easy_log_print = p;
    ev_set_syserr_cb(easy_log_print);
}

/**
 * 设置log的格式函数
 */
void  easy_log_set_format(easy_log_format_pt p)
{
    easy_log_format = p;
}

/**
 * 加上日志
 */
void easy_log_format_default(int level, const char *file, int line, const char *function, const char *fmt, ...)
{
    static __thread ev_tstamp   oldtime = 0.0;
    static __thread char        time_str[32];
    ev_tstamp                   now;
    int                         len;
    char                        buffer[4096];

    // 从loop中取
    if (easy_baseth_self && easy_baseth_self->loop) {
        now = ev_now(easy_baseth_self->loop);
    } else {
        now = time(NULL);
    }

    if (oldtime != now) {
        time_t                  t;
        struct tm               tm;
        oldtime = now;
        t = (time_t) now;
        easy_localtime((const time_t *)&t, &tm);
        lnprintf(time_str, 32, "[%04d-%02d-%02d %02d:%02d:%02d.%03d]",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, (int)((now - t) * 1000));
    }

    // print
    len = lnprintf(buffer, 128, "%s %s:%d(tid:%lx) ", time_str, file, line, pthread_self());
    va_list                 args;
    va_start(args, fmt);
    len += easy_vsnprintf(buffer + len, 4090 - len,  fmt, args);
    va_end(args);

    // 去掉最后'\n'
    while(buffer[len - 1] == '\n') len --;

    buffer[len++] = '\n';
    buffer[len] = '\0';

    easy_log_print(buffer);
}

/**
 * 打印出来
 */
void easy_log_print_default(const char *message)
{
    write(2, message, strlen(message));
}

void __attribute__((constructor)) easy_log_start_()
{
    char                    *p = getenv("easy_log_level");

    if (p) easy_log_level = (easy_log_level_t)atoi(p);
}

