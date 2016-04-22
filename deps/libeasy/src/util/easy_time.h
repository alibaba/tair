#ifndef EASY_TIME_H_
#define EASY_TIME_H_

/**
 * time的通用函数
 */
#include "easy_define.h"

EASY_CPP_START

int easy_localtime(const time_t *t, struct tm *tp);
int64_t easy_time_now();

EASY_CPP_END

#endif
