#include <easy_time.h>
#include <sys/time.h>

/**
 * localtime删除tzset,一起调用一次tzset
 */
int easy_localtime (const time_t *t, struct tm *tp)
{
    static const unsigned short int mon_yday[2][13] = {
        /* Normal years.  */
        { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 },
        /* Leap years.  */
        { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 }
    };

#define SECS_PER_HOUR   (60 * 60)
#define SECS_PER_DAY    (SECS_PER_HOUR * 24)
#define ISLEAP(year)    ((year) % 4 == 0 && ((year) % 100 != 0 || (year) % 400 == 0))
#define DIV(a, b)       ((a) / (b) - ((a) % (b) < 0))
#define LEAPS_THRU_END_OF(y) (DIV (y, 4) - DIV (y, 100) + DIV (y, 400))

    long int                days, rem, y;
    const unsigned short int *ip;

    days = *t / SECS_PER_DAY;
    rem = *t % SECS_PER_DAY;
    rem -= timezone;

    while (rem < 0) {
        rem += SECS_PER_DAY;
        --days;
    }

    while (rem >= SECS_PER_DAY) {
        rem -= SECS_PER_DAY;
        ++days;
    }

    tp->tm_hour = rem / SECS_PER_HOUR;
    rem %= SECS_PER_HOUR;
    tp->tm_min = rem / 60;
    tp->tm_sec = rem % 60;
    /* January 1, 1970 was a Thursday.  */
    tp->tm_wday = (4 + days) % 7;

    if (tp->tm_wday < 0)
        tp->tm_wday += 7;

    y = 1970;

    while (days < 0 || days >= (ISLEAP (y) ? 366 : 365)) {
        /* Guess a corrected year, assuming 365 days per year.  */
        long int                yg = y + days / 365 - (days % 365 < 0);

        /* Adjust DAYS and Y to match the guessed year.  */
        days -= ((yg - y) * 365
                 + LEAPS_THRU_END_OF (yg - 1)
                 - LEAPS_THRU_END_OF (y - 1));
        y = yg;
    }

    tp->tm_year = y - 1900;

    if (tp->tm_year != y - 1900) {
        return 0;
    }

    tp->tm_yday = days;
    ip = mon_yday[ISLEAP(y)];

    for (y = 11; days < (long int) ip[y]; --y)
        continue;

    days -= ip[y];
    tp->tm_mon = y;
    tp->tm_mday = days + 1;
    return 1;
}

void __attribute__((constructor)) easy_time_start_()
{
    tzset();
}

int64_t easy_time_now()
{
    struct timeval          tv;
    gettimeofday (&tv, 0);
    return __INT64_C(1000000) * tv.tv_sec + tv.tv_usec;
}
