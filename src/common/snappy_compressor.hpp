#include "tbsys.h"
#include "snappy.h"

#ifndef TAIR_DATA_ENTRY_SNAPPY_COMPRESS
#define TAIR_DATA_ENTRY_SNAPPY_COMPRESS

namespace tair {
  namespace common {
    class snappy_compressor {
    public:
      static int do_compress(char** dest, uint32_t* dest_len, const char* src, uint32_t src_len)
      {
        int ret = 0;
        size_t tmp_len = snappy::MaxCompressedLength(src_len);
        char* tmp_data = new char[tmp_len];
        assert(tmp_data != NULL);
        snappy::RawCompress(src, src_len, tmp_data, &tmp_len);
        // to check if the tmp_len overflow, check if tmp_len bigger than 1M
        if (0 != (tmp_len >> 20)) {
          delete tmp_data;
          ret = -1;
        } else {
          *dest = tmp_data;
          *dest_len = tmp_len;
          log_debug("snappy compress ok, from %d to %d", src_len, tmp_len);
        }
        return ret;
      }

      static int do_decompress(char **dest, uint32_t *dest_len, const char* src, uint32_t src_len)
      {
        int ret = 0;
        size_t tmp_len = 0;

        if (!snappy::GetUncompressedLength(src, src_len, &tmp_len)) {
          log_error("snappy decompress err while GetUncompressedLength");
          ret = -1;
        } else {
          char* tmp_data = new char[tmp_len];
          assert(tmp_data != NULL);
          if (!snappy::RawUncompress(src, src_len, tmp_data)) {
            log_error("snappy decompress err while RawUncompress");
            delete tmp_data;
            ret = -1;
          } else {
            // to check if the tmp_len overflow, check if tmp_len bigger than 1M
            if (0 != (tmp_len >> 20)) {
              delete tmp_data;
              ret = -1;
            } else {
              *dest = tmp_data;
              *dest_len = tmp_len;
              log_debug("snappy decompress ok, from %d to %d", src_len, tmp_len);
            }
          }
        }

        return ret;
      }
    };
  }
}

#endif
