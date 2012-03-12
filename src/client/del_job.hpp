#ifndef __DELJOB_H__
#define __DELJOB_H__

class DelJob {
public:
    DelJob(int keynum);
    DelJob(const DelJob & job);
    void addKey(char* key, int size);
    int getSize();
    char* getKey(int index);
    char** getAllKey();
    ~DelJob();

private:
    char** _keys;
    int _index;
    int _keynum;
};

DelJob::DelJob(int keynum) {
    _keys = new char*[keynum];
    _keynum = keynum;
    _index = 0;
}

DelJob::~DelJob() {
    if (_keys != NULL) {
        for(int i = 0; i < _index; i++) {
            if (_keys[i] != NULL) {
                delete [](_keys[i]);
            }
        }
        delete []_keys;
    }
}

void DelJob::addKey(char* key, int size) {
    if (size <= 0 || key == NULL) {
        return;
    }
    _keys[_index] = new char[size];
    if (_keys[_index] == NULL) {
        return;
    }
    memcpy(_keys[_index++], key, size);
}

char* DelJob::getKey(int index) {
    return _keys[index];
}

char** DelJob::getAllKey() {
    return _keys;
}

int DelJob::getSize() {
    return _index;       
}

#endif
