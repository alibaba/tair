#ifndef INVAL_PROCESSOR_HPP
#define INVAL_PROCESSOR_HPP
#include <tbsys.h>
#include <tbnet.h>

#include "inval_loader.hpp"
#include "log.hpp"
#include "invalid_packet.hpp"
#include "hide_by_proxy_packet.hpp"
#include "prefix_hides_by_proxy_packet.hpp"
#include "prefix_invalids_packet.hpp"

namespace tair {
  class RequestProcessor {
  public:
    RequestProcessor(InvalLoader *invalid_loader) {
      this->invalid_loader = invalid_loader;
    }

    int process(request_invalid *req, request_invalid *&post_req);
    int process(request_hide_by_proxy *req, request_hide_by_proxy *&post_req);
    int process(request_prefix_hides_by_proxy *req, request_prefix_hides_by_proxy *&post_req);
    int process(request_prefix_invalids *req, request_prefix_invalids *&post_req);
  private:
    InvalLoader *invalid_loader;
  };
}

#endif
