#ifndef __TEST_NBQUEUE__
#define __TEST_NBQUEUE__

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>

#include "NBQueue.h"

NBQueue<int> queue(100);

void* push(void* argv) {
    int i = 1;
    while(i) {
        if(queue.isFull()) {
            //usleep(1000);
            continue;
        }
        queue.push(i++);        
    }
    return NULL;
}

void* pop(void* argv) {
    int last = 0;
    int i = 0;
    while(true) {
        if(queue.isEmpty()) {
            //usleep(1000);
            continue;
        }
        i = queue.pop();
        if(i != last + 1) {
            fprintf(stderr, "fuck\n");
            break;
        }
        last++;
        fprintf(stdout, "%d\n", i++);
    }
    return NULL;
}


int main(int argc, char** argv) {
    pthread_t push_pthread;
    pthread_t pop_pthread;

    int flag = pthread_create(&push_pthread, NULL, push, NULL);
    if (flag < 0) {
        fprintf(stderr, "%s\n", strerror(errno));
        return 1;
    }

    flag = pthread_create(&pop_pthread, NULL, pop, NULL);
    if (flag < 0) {
        fprintf(stderr, "%s\n", strerror(errno));
        return 1;
    }

    pthread_join(push_pthread, NULL);
    pthread_join(pop_pthread, NULL);

    return 0;
}

#endif
