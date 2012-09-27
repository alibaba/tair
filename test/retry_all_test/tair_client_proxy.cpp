#include "tair_client_proxy.hpp"
#include "tair_client_api_impl.hpp"
int tair_client_proxy::debug_support(uint64_t invalid_server, std::vector<std::string> &infos)
{
  impl->debug_support(invalid_server, infos);
}
