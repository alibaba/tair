/*
 * (C) 2007-2017 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * See the AUTHORS file for names of contributors.
 *
 */

#ifndef __NBQUEUE__
#define __NBQUEUE__

template<class T>
class NBQueue {
public:
    NBQueue() {}

    NBQueue(int size);

    explicit NBQueue(const NBQueue &queue);

    ~NBQueue();

    int isFull();

    int isEmpty();

    T pop();

    void push(T data);

private:
    int _start;
    int _end;
    int _size;
    T *_buff;
};

template<class T>
NBQueue<T>::NBQueue(int size) {
    _start = 0;
    _end = 0;
    _size = size + 1;
    _buff = new T[_size];
}

template<class T>
NBQueue<T>::~NBQueue() {
    _start = -1;
    _end = -1;
    _size = -1;
    delete[]_buff;
}

template<class T>
int NBQueue<T>::isFull() {
    return (_end + 1) % _size == _start;
}

template<class T>
int NBQueue<T>::isEmpty() {
    return _end % _size == _start;
}

template<class T>
T NBQueue<T>::pop() {
    T data = _buff[_start];
    _start = (_start + 1) % _size;
    return data;
}

template<class T>
void NBQueue<T>::push(T job) {
    _buff[_end] = job;
    _end = (_end + 1) % _size;
}

#endif
