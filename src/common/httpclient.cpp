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

#include "httpclient.hpp"

#include <assert.h>
#include <string.h>
#include <curl/curl.h>

#include <fstream>

namespace System {
namespace Net {

const std::string HttpHeader::Accept = "Accept";
const std::string HttpHeader::Connection = "Connection";
const std::string HttpHeader::ContentLength = "Content-Length";
const std::string HttpHeader::ContentType = "Content-Type";
const std::string HttpHeader::Expect = "Expect";
const std::string HttpHeader::Date = "Date";
const std::string HttpHeader::Host = "Host";
const std::string HttpHeader::IfModifiedSince = "If-Modified-Since";
const std::string HttpHeader::Referer = "Referer";
const std::string HttpHeader::TransferEncoding = "Transfer-Encoding";
const std::string HttpHeader::UserAgent = "User-Agent";
const std::string HttpHeader::None = "None";

const std::string HttpWebRequest::Method::POST = "POST";
const std::string HttpWebRequest::Method::GET = "GET";
const std::string HttpWebRequest::Method::DELETE = "DELETE";

void HttpGobal::Initialize() { curl_global_init(CURL_GLOBAL_ALL); }

void HttpGobal::Destroy() { curl_global_cleanup(); }

HttpWebRequest::HttpWebRequest(std::string &uri)
        : is_(std::ios_base::in | std::ios_base::out | std::ios_base::binary) {
    uri_ = uri;
    method_ = HttpWebRequest::Method::GET;
}

size_t HttpWebRequest::callback(void *buffer, size_t size, size_t nmemb, void *data) {
    HttpWebResponse *resp = (HttpWebResponse *) data;
    std::stringstream &ss = resp->GetResponseStream();
    ss.write((char *) buffer, size * nmemb);
    return size * nmemb;
}

HttpWebResponse *HttpWebRequest::GetFileResponse(const char *uri) {
    assert(strlen(uri) > 6);
    HttpWebResponse *resp = new HttpWebResponse();
    const char *filename = uri + 6;

    std::ifstream file;
    file.open(filename);
    if (file.is_open()) {
        resp->os_ << file.rdbuf();
        file.close();
    }

    return resp;
}

HttpWebResponse *HttpWebRequest::GetNetResponse(const char *uri) {
    HttpWebResponse *resp = new HttpWebResponse();

    CURL *curl = curl_easy_init();

    curl_easy_setopt(curl, CURLOPT_URL, uri);

    curl_slist *http_headers = NULL;
    for (std::map<std::string, std::string>::iterator it = header_.begin();
         it != header_.end(); it++) {
        http_headers = curl_slist_append(http_headers,
                                         (it->first + ":" + it->second).c_str());
    }
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, http_headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) resp);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 2);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);

    curl_easy_perform(curl);

    long retcode = 0;
    CURLcode code = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &retcode);
    if (code == CURLE_OK && retcode == 200) {
        resp->status_.SetCode(200);

        char *ctype = NULL;
        curl_easy_getinfo(curl, CURLINFO_CONTENT_TYPE, &ctype);
        std::string type(ctype);
        resp->header_.Put(HttpHeader::ContentType, type);

        double length = 0;
        curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &length);
        std::ostringstream os;
        os << (long) length;
        resp->header_.Put(HttpHeader::ContentLength, os.str());
    }


    curl_slist_free_all(http_headers);
    curl_easy_cleanup(curl);

    return resp;
}

HttpWebResponse *HttpWebRequest::GetResponse() {
    const char *uri = uri_.c_str();
    if (strncmp(uri, "file://", 7) == 0) {
        return GetFileResponse(uri);
    }
    return GetNetResponse(uri);
}
}
}

// int main(int argc, char** argv) {
//   System::Net::HttpInitialize();
//
//   std::string uri = "http://localhost:8080/shanghai/ldbcommon";
//   System::Net::HttpWebRequest* req = System::Net::HttpWebRequest::Create(uri);
//   System::Net::HttpWebResponse* resp = req->GetResponse();
//
//   std::cout << resp->GetHeader(System::Net::HttpHeader::ContentLength) << std::endl;
//   std::cout << resp->GetResponseStream().str() << std::endl;
//
//   std::string str = resp->GetResponseStream().str();
//
//   System::Net::HttpDestroy();
// }
