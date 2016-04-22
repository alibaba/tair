/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong
 *
 */

#include "filequeue.h"

namespace tbsys {
    /**
     * rootPath: queue的root路径
     * queueName: 
     * maxFileSize: 最大文件大小
     */
    CFileQueue::CFileQueue(char *rootPath, char *queueName, int maxFileSize)
    {
        char tmp[256];
        sprintf(tmp, "%s/%s", rootPath, queueName);
        m_queuePath = strdup(tmp);
        m_maxFileSize = maxFileSize;
        m_infoFd = -1;
        m_readFd = -1;
        m_writeFd = -1;
        
        // 创建目录
        if (CFileUtil::mkdirs(m_queuePath) == false) {
            TBSYS_LOG(ERROR, "创建目录失败: %s", m_queuePath);
            return;
        }
        
        // 头文件
        sprintf(tmp, "%s/header.dat", m_queuePath);
        m_infoFd = open(tmp, O_RDWR|O_CREAT, 0600);
        if (m_infoFd == -1) {
            TBSYS_LOG(ERROR, "打开文件失败: %s", tmp);
            return;
        }
        
        // 读进头信息
        if (read(m_infoFd, &m_head, sizeof(m_head)) != sizeof(m_head)) {
            memset(&m_head, 0, sizeof(m_head));
            m_head.read_seqno = 1;
            m_head.write_seqno = 1;
        }
        
        // 打开写
        if (openWriteFile() == EXIT_FAILURE) {
            return;
        }
        
        struct stat st;
        if (fstat(m_writeFd, &st) == 0) {
            m_head.write_filesize = st.st_size;
        }

        // 打开读
        if (openReadFile() == EXIT_FAILURE) {
            close(m_writeFd);
            m_writeFd = -1;
            return;
        }

        // 恢复记录
        if (m_head.exit_status == 0) {
            recoverRecord();
        }
        m_head.exit_status = 0;         
    }
    /**
     * 析构
     */
    CFileQueue::~CFileQueue(void)
    {
        m_head.exit_status = 1;
        memset(&m_head.pos[0], 0, TBFQ_MAX_THREAD_COUNT*sizeof(unsettle));
        writeHead();        
        if (m_queuePath) {
            free(m_queuePath);
            m_queuePath = NULL;
        }
        if (m_infoFd != -1) {
            close(m_infoFd);
            m_infoFd = -1;
        }
        if (m_readFd != -1) {
            close(m_readFd);
            m_readFd = -1;
        }
        if (m_writeFd != -1) {
            close(m_writeFd);
            m_writeFd = -1;
        }
    }
    
    /**
     * 写入一记录
     */
    int CFileQueue::push(void *data, int len)
    {
        if (m_writeFd == -1) {
            openWriteFile();
            if (m_writeFd == -1) {
                TBSYS_LOG(WARN, "文件没打开: %s:%u", m_queuePath, m_head.write_seqno);
                return EXIT_FAILURE;
            }
        }
        if (data == NULL || len == 0) {
            TBSYS_LOG(WARN, "data: %p, len: %d", data, len);
            return EXIT_FAILURE;
        }
        int size = sizeof(int) + sizeof(queue_item) + len;
        if (size > TBFQ_MAX_FILE_SIZE) {
            TBSYS_LOG(WARN, "size: %d", size);
            return EXIT_FAILURE;
        }
        char *buffer = (char*)malloc(size);
        assert(buffer != NULL);
        *((int*)buffer) = size;
        queue_item *item = (queue_item*) (buffer + sizeof(int));
        item->len = len;
        item->flag = TBFQ_FILE_QUEUE_FLAG;
        memcpy(&(item->data[0]), data, len);
        
        // 如果文件大于了重新打开一个
        if (m_head.write_filesize >= m_maxFileSize) {
            m_head.write_seqno ++;
            m_head.write_filesize = 0;
            openWriteFile();
            writeHead();
        }
        item->pos.seqno = m_head.write_seqno;
        item->pos.offset = m_head.write_filesize;
        int ret = write(m_writeFd, buffer, size);
        if (ret > 0) {
            m_head.write_filesize += size;
        }
        free(buffer);
        if (ret != size) { // 写失败
            TBSYS_LOG(WARN, "写失败: %s, fd: %d, len: %d, %d<>%d", 
                m_queuePath, m_writeFd, len, ret, size);
            ret = size - ret;
            if (ret>0 && ret<=size && size<m_maxFileSize) {
                TBSYS_LOG(WARN, "跳过%d个字节写", ret);
                lseek(m_writeFd, ret, SEEK_CUR);
            }
            return EXIT_FAILURE;
        }
        m_head.queue_size ++;
        return EXIT_SUCCESS;
    }
    
    /**
     * 读出一记录, 调用程序要负责释放
     */
    queue_item *CFileQueue::pop(uint32_t index)
    {
        if (m_readFd == -1) {
            openReadFile();
            if (m_readFd == -1) {
                TBSYS_LOG(WARN, "文件没打开: %s:%d", m_queuePath, m_head.read_seqno);
                return NULL;
            }
        }
        int ret, size = 0;
        queue_item *item = NULL;
        int retryReadCount = 0;
        index %= TBFQ_MAX_THREAD_COUNT;
        
        while(retryReadCount<3) {
            int retSize = read(m_readFd, &size, sizeof(int));
            if (retSize < 0) {
                TBSYS_LOG(ERROR, "读出错, m_readFd:%d, %s(%d)", m_readFd, strerror(errno), errno);
                break;
            }
            if (retSize == sizeof(int)) {
                size -= sizeof(int);
                // 检查size
                if (size < (int)sizeof(queue_item) || size > (int)TBFQ_MAX_FILE_SIZE) {
                    int curPos = (int)lseek(m_readFd, 0-retSize, SEEK_CUR);
                    TBSYS_LOG(WARN, "读出的size不正确:%d,curPos:%d,readOff:%d,retry:%d,seqno:%u,m_readFd:%d", 
                        size, curPos, m_head.read_offset, retryReadCount, m_head.read_seqno,m_readFd);
                    retryReadCount ++;
                    if (m_writeFd != -1 && m_head.read_seqno == m_head.write_seqno) {
                        fsync(m_writeFd);
                    }
                    continue;
                }
                // 分配内存
                item = (queue_item*) malloc(size);
                assert(item != NULL);
                if ((ret = read(m_readFd, item, size)) != size) {
                    // 重新读
                    int64_t curPos = lseek(m_readFd, 0-ret-retSize, SEEK_CUR);
                    TBSYS_LOG(WARN, "读文件不正确:%d<>%d,curPos:%d,readOff:%d,retry:%d,seqno:%u,m_readFd:%d", 
                        ret, size, curPos, m_head.read_offset, retryReadCount, m_head.read_seqno,m_readFd);
                    retryReadCount ++;
                    free(item);
                    item = NULL;
                    if (m_writeFd != -1 && m_head.read_seqno == m_head.write_seqno) {
                        fsync(m_writeFd);
                    }
                    continue;
                }
                // 检查flag
                if (item->flag != TBFQ_FILE_QUEUE_FLAG) {
                     // 重新读
                    int64_t curPos = lseek(m_readFd, 0-ret-retSize, SEEK_CUR);
                    TBSYS_LOG(WARN, "flag不正确:item->flag(%d)<>FLAG(%d),curPos:%d,readOff:%d,retry:%d,seqno:%u,m_readFd:%d", 
                        item->flag, TBFQ_FILE_QUEUE_FLAG, curPos, m_head.read_offset, 
                        retryReadCount, m_head.read_seqno,m_readFd);
                    retryReadCount ++;
                    free(item);
                    item = NULL;                    
                    if (m_writeFd != -1 && m_head.read_seqno == m_head.write_seqno) {
                        fsync(m_writeFd);
                    }
                    continue;
                }
                // 检查len
                if (item->len + (int)sizeof(queue_item) != size) {
                    // 重新读
                    int64_t curPos = lseek(m_readFd, 0-ret-retSize, SEEK_CUR);
                    TBSYS_LOG(WARN, "读出的len不正确:%d<>%d,curPos:%d,readOff:%d,retry:%d,seqno:%u,m_readFd:%d", 
                        item->len + sizeof(queue_item), size, curPos, m_head.read_offset, 
                        retryReadCount, m_head.read_seqno,m_readFd);
                    retryReadCount ++;
                    free(item);
                    item = NULL;
                    if (m_writeFd != -1 && m_head.read_seqno == m_head.write_seqno) {
                        fsync(m_writeFd);
                    }
                    continue;
                }
                
                retryReadCount = 0;
                m_head.pos[index].seqno = m_head.read_seqno;
                m_head.pos[index].offset = m_head.read_offset;
                m_head.read_offset += (size + sizeof(int));
                break;
            } else if (m_head.write_seqno > m_head.read_seqno) {
                // 删除处理完的文件
                deleteReadFile();
                m_head.read_seqno ++;
                m_head.read_offset = 0;
                openReadFile();
                if (m_readFd == -1) {
                    m_head.queue_size = 0;
                    break;
                }
            } else {
                if (retSize > 0) {
                    TBSYS_LOG(WARN, "IO忙,retSize:%d,seqno:%u", retSize, m_head.read_seqno);
                    lseek(m_readFd, 0-retSize, SEEK_CUR); 
                }
                m_head.queue_size = 0;
                break;
            }
        }
        if (retryReadCount>=3) { // 连接失败了三次,移到最后
            backup(index);
            if (m_head.write_seqno > m_head.read_seqno)
            {
                deleteReadFile();
                m_head.read_seqno ++;
                m_head.read_offset = 0;
                openReadFile();
            } else {
                clear();
            }
        }
        writeHead();
        return item;
    }
    
    /**
     * 清空队列
     */
    int CFileQueue::clear()
    {
        m_head.write_seqno ++;
        m_head.read_seqno = m_head.write_seqno;
        m_head.read_offset = 0;
        m_head.write_filesize = 0;
        m_head.queue_size = 0;
        openWriteFile();
        openReadFile();
        return EXIT_SUCCESS;
    }
    
    /**
     * 是否空了
     */
    int CFileQueue::isEmpty()
    {
        return (m_head.queue_size == 0 ? 1 : 0);
    }
            
    /**
     * 己处理完了
     */
    void CFileQueue::finish(uint32_t index) {
        unsettle *pos = &(m_head.pos[index % TBFQ_MAX_THREAD_COUNT]);
        pos->seqno = 0;
        pos->offset = 0;
    }
    
    /**
     * 备份文件，进行后面分析
     */
    void CFileQueue::backup(uint32_t index) {
        int curPos = (int)lseek(m_readFd, 0, SEEK_CUR);
        char cmd[512];
        sprintf(cmd, "cp -f %s/%08u.dat %s/%08u.dat.backup >/dev/null 2>&1 0>&1", 
            m_queuePath, m_head.read_seqno,
            m_queuePath, m_head.read_seqno);
        int rest = system(cmd);
        TBSYS_LOG(WARN, "%s, readpos: %u:%d, currpos: %u:%d, curPos: %d, m_readFd: %d result: %d", cmd, 
            m_head.pos[index].seqno, m_head.pos[index].offset,
            m_head.read_seqno, m_head.read_offset, curPos, m_readFd, rest);
    }
    
    /******************************************
     * 打开数据文件写
     */
    int CFileQueue::openWriteFile()
    {
        if (m_writeFd != -1) {
            fsync(m_writeFd);
            close(m_writeFd);
            m_writeFd = -1;
        }
        char tmp[256];
        sprintf(tmp, "%s/%08u.dat", m_queuePath, m_head.write_seqno);
        m_writeFd = open(tmp, O_WRONLY|O_CREAT|O_APPEND, 0600);
        if (m_writeFd == -1) {
            TBSYS_LOG(ERROR, "文件打开失败: %s", tmp);
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }
    /**
     * 删除处理完的文件
     */
    int CFileQueue::deleteReadFile()
    {
        if (m_readFd != -1) {
            close(m_readFd);
            m_readFd = -1;
        }
        char tmp[256];
        sprintf(tmp, "%s/%08u.dat", m_queuePath, m_head.read_seqno - 1);
        unlink(tmp);
        return EXIT_SUCCESS;
    }
    /**
     * 打开数据文件读
     */
    int CFileQueue::openReadFile()
    {
        if (m_readFd != -1) {
            close(m_readFd);
            m_readFd = -1;
        }
        char tmp[256];
        sprintf(tmp, "%s/%08u.dat", m_queuePath, m_head.read_seqno);
        m_readFd = open(tmp, O_RDONLY, 0600);
        if (m_readFd == -1) {
            TBSYS_LOG(ERROR, "文件打开失败: %s", tmp);
            return EXIT_FAILURE;
        }
        lseek(m_readFd, m_head.read_offset, SEEK_SET);
        TBSYS_LOG(INFO, "打开下一文件:%s,wseq:%u,rseq:%u,roffset:%d,m_readFd:%d", 
            tmp, m_head.write_seqno, m_head.read_seqno, m_head.read_offset, m_readFd);
        return EXIT_SUCCESS;
    }    
    
    /**
     * 写head
     */
    int CFileQueue::writeHead()
    {
        if (m_infoFd != -1) {
            lseek(m_infoFd, 0, SEEK_SET);
            if (write(m_infoFd, &m_head, sizeof(m_head)) != sizeof(m_head)) {
                TBSYS_LOG(ERROR, "写失败: %s", m_queuePath);
            }
        }
        return EXIT_SUCCESS;
    }
    /**
     * 恢复记录
     */
    void CFileQueue::recoverRecord() {
        char tmp[256];
        int fd, size, count = 0;
        for(int i=0; i<TBFQ_MAX_THREAD_COUNT; i++) {
            unsettle *pos = &(m_head.pos[i]);
            if (pos->seqno == 0) continue;
            if (pos->seqno > m_head.read_seqno) continue;
            if (pos->offset >= m_head.read_offset) continue;
            
            sprintf(tmp, "%s/%08u.dat", m_queuePath, pos->seqno);
            fd = open(tmp, O_RDONLY, 0600);
            if (fd == -1) continue;
            lseek(fd, pos->offset, SEEK_SET);
            if (read(fd, &size, sizeof(int)) == sizeof(int) && 
                size >= (int)sizeof(queue_item) && 
                size < (int)TBFQ_MAX_FILE_SIZE) {
                size -= sizeof(int);
                queue_item *item = (queue_item*) malloc(size);
                assert(item != NULL);
                if (read(fd, item, size) == size && item->flag == TBFQ_FILE_QUEUE_FLAG) {
                    push(&(item->data[0]), item->len);
                    count ++;
                }
                free(item);
            }
            close(fd);
        }
        TBSYS_LOG(INFO, "%s: recoverRecord: %d", m_queuePath, count);
        memset(&m_head.pos[0], 0, TBFQ_MAX_THREAD_COUNT*sizeof(unsettle));
        writeHead();
    }
}

//////////////////END
