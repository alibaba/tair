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

#ifndef __SIMPLE_HTTP_H__
#define __SIMPLE_HTTP_H__

#include <map>
#include <string>
#include <sstream>
#include <iostream>

namespace System {
namespace Net {
class HttpWebRequest;

class HttpWebResponse;

class HttpGobal {
public:
    static void Initialize();

    static void Destroy();
};

class HttpStatus {
public:
    HttpStatus() {}

    long GetCode() { return code_; }

    void SetCode(long code) { code_ = code; }

    const std::string &GetDescription() { return description_; }

    void SetDescription(const std::string &description) { description_ = description; }

    friend std::ostream &operator<<(std::ostream &os, const HttpStatus &status) {
        os << status.code_;
        return os;
    }

private:
    int code_;
    std::string description_;
};

class HttpHeader {
public:
    const static std::string Accept;
    const static std::string Connection;
    const static std::string ContentLength;
    const static std::string ContentType;
    const static std::string Expect;
    const static std::string Date;
    const static std::string Host;
    const static std::string IfModifiedSince;
    const static std::string Referer;
    const static std::string TransferEncoding;
    const static std::string UserAgent;

    const static std::string None;
public:
    typedef std::map<std::string, std::string>::iterator Iterator;
public:
    HttpHeader() {}

    ~HttpHeader() {}

    HttpHeader::Iterator begin() {
        return header_.begin();
    }

    HttpHeader::Iterator end() {
        return header_.end();
    }

    inline void Put(const std::string &name, const std::string &value) {
        header_.insert(std::make_pair<std::string, std::string>(name, value));
    }

    inline const std::string &Get(const std::string &name) {
        std::map<std::string, std::string>::iterator it = header_.find(name);
        return (it == header_.end() ? HttpHeader::None : it->second);
    }

private:
    std::map<std::string, std::string> header_;
private:
    friend class HttpWebResponse;

    friend class HttpWebRequest;
};

class HttpWebResponse {
public:
    inline std::stringstream &GetResponseStream() { return os_; }

    inline const std::string &GetHeader(const std::string &name) { return header_.Get(name); }

    inline const std::string &GetProtocolVersion() { return version_; }

    inline const std::string &GetResponseUri() { return uri_; }

    inline const std::string &GetServer() { return server_; }

    inline void Destroy() { delete this; }

private:
    HttpWebResponse()
            : status_(),
              os_(std::ios_base::in | std::ios_base::out | std::ios_base::binary) {}

private:
    std::string server_;
    std::string uri_;
    std::string version_;
    HttpStatus status_;
    HttpHeader header_;
    std::stringstream os_;
private:
    friend class HttpWebRequest;
};

class HttpWebRequest {
public:
    struct Method {
        const static std::string POST;
        const static std::string GET;
        const static std::string DELETE;
    };
public:
    inline static HttpWebRequest *Create(std::string &uri) {
        return new HttpWebRequest(uri);
    }

    inline void Destroy() {
        delete this;
    }

    inline void SetMethod(const std::string &method) { method_ = method; }

    const std::string &GetMethod() { return method_; }

    inline void SetHeader(const std::string &name, const std::string &value) { header_.Put(name, value); }

    inline const std::string &GetHeader(const std::string &name) { return header_.Get(name); }

    inline std::stringstream &GetRequestStream() { return is_; }

    HttpWebResponse *GetResponse();

private:
    HttpWebRequest(std::string &uri);

    static size_t callback(void *buffer, size_t size, size_t nmemb, void *data);

    HttpWebResponse *GetFileResponse(const char *uri);

    HttpWebResponse *GetNetResponse(const char *uri);

private:
    std::string uri_;
    std::string method_;
    HttpHeader header_;
    std::stringstream is_;
};
}
}

#endif
