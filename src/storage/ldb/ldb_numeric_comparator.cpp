#include "common/define.hpp"
#include "ldb_define.hpp"
#include "ldb_gc_factory.hpp"
#include "ldb_comparator.hpp"

namespace tair
{
  namespace storage
  {
    namespace ldb
    {
      static const size_t LDB_COMPARE_SKIP_SIZE = LDB_EXPIRED_TIME_SIZE;
      static const size_t LDB_COMPARE_META_ALL_SIZE = LDB_EXPIRED_TIME_SIZE + LDB_KEY_BUCKET_NUM_SIZE + LDB_KEY_AREA_SIZE;
      NumericalComparatorImpl::NumericalComparatorImpl() : number_delim_(0), meta_len_(0)
      {
        ldb_comparator_ = new BitcmpLdbComparatorImpl();
      }

      NumericalComparatorImpl::NumericalComparatorImpl(LdbGcFactory* gc, const char delim, size_t len) 
        : number_delim_(delim), meta_len_(len)
      {
        ldb_comparator_ = new BitcmpLdbComparatorImpl(gc);
      }

      NumericalComparatorImpl::~NumericalComparatorImpl()
      {
        delete ldb_comparator_;
      }

      const char* NumericalComparatorImpl::Name() const
      {
        return "ldb.numericComparator";
      }
      
      //skip meta_size_, return pointer next
      inline const char *NumericalComparatorImpl::MetaSkip(const char *key, size_t len) const 
      {
        if (len < meta_len_) 
          return key;
        return key + meta_len_;
      }

      //skip prefix_size_, return pointer next, if no delim found return key itself
      const char *NumericalComparatorImpl::FindNumber(const char *key, size_t len) const 
      {
        size_t i;
        for (i=0; i<len; i++) 
          if (key[i] == number_delim_)
            break;
        if (i == len)
          return key;
        else
          return key+i+1;
      }

      // using this method instead of strtoll because maybe no '\0' in slice key
      int64_t NumericalComparatorImpl::Strntoul(const char *key, size_t len, const char **endptr) const 
      {
        char c;
        int64_t number = 0;
        for(;len-- > 0; key++)
        {
          c = *key;
          if (c < '0' || c > '9')
            break;
          number *= 10;
          number += c - '0';
        }
        *endptr = key;
        return number; //return last number pointer
      }

      int NumericalComparatorImpl::StringCompare(const char *a, size_t len_a, const char *b, size_t len_b) const 
      {
        size_t len_min = len_a > len_b ? len_b : len_a;  
        int ret = 0;
        if (len_min == 0 || (ret = memcmp(a, b, len_min)) == 0)
        {
          if (len_a != len_b) 
          {
            ret = len_a - len_b;  
          }
        }
        return ret;
      }

      // Numerical user_key format: "%s%ld%s",prefix,inc_number,suffix
      // if no numbers found, treat it as string
      int  NumericalComparatorImpl::Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
      {
        assert(a.size() > LDB_COMPARE_META_ALL_SIZE && b.size() > LDB_COMPARE_META_ALL_SIZE );
        int64_t num_a = 0, num_b = 0;
        int ret = 0;
        const char *prefix_a, *prefix_b, *delimiter_a, *delimiter_b, *suffix_a, *suffix_b;

        prefix_a = MetaSkip(a.data() + LDB_COMPARE_META_ALL_SIZE, a.size() - LDB_COMPARE_META_ALL_SIZE);  
        delimiter_a = FindNumber(prefix_a, a.size() - (prefix_a - a.data()));
        prefix_b = MetaSkip(b.data() + LDB_COMPARE_META_ALL_SIZE, b.size() - LDB_COMPARE_META_ALL_SIZE);  
        delimiter_b = FindNumber(prefix_b, b.size() - (prefix_b - b.data()));
        //compare bucket_num+area+meta+prefix
        const size_t pre_len_a = delimiter_a - a.data() - LDB_COMPARE_SKIP_SIZE;
        const size_t pre_len_b = delimiter_b - b.data() - LDB_COMPARE_SKIP_SIZE;
        ret = StringCompare(a.data() + LDB_COMPARE_SKIP_SIZE, pre_len_a, b.data() + LDB_COMPARE_SKIP_SIZE, pre_len_b);
        if (ret == 0)
        {
          //prefixs equal, compare number
          num_a = Strntoul(delimiter_a, a.size() - (delimiter_a - a.data()), &suffix_a);
          num_b = Strntoul(delimiter_b, b.size() - (delimiter_b - b.data()), &suffix_b);
          if (num_a != num_b)
          {
            ret = (num_a - num_b) > 0 ? 1 : -1;
          } 
          else
          {
            //numbers equal or no numbers found, compare suffix
            const size_t suf_len_a = a.size() - (suffix_a - a.data());
            const size_t suf_len_b = b.size() - (suffix_b - b.data());
            ret = StringCompare(suffix_a, suf_len_a, suffix_b, suf_len_b);
          }
        }
        return ret;
      }

      void NumericalComparatorImpl::FindShortSuccessor(std::string* key) const
      {
      }

      void  NumericalComparatorImpl::FindShortestSeparator(
        std::string* start,
        const leveldb::Slice& limit) const
      {
      }


      bool NumericalComparatorImpl::ShouldDrop(const char* key, int64_t sequence, uint32_t will_gc) const
      {
        return ldb_comparator_->ShouldDrop(key, sequence, will_gc);
      }


      bool NumericalComparatorImpl::ShouldDropMaybe(const char* key, int64_t sequence, uint32_t now) const
      {
        return ldb_comparator_->ShouldDropMaybe(key, sequence, now);
      }

      const leveldb::Comparator*  NumericalComparator(LdbGcFactory* gc, const char start, size_t len)
      {
        return new NumericalComparatorImpl(gc, start, len);
      }
    }
  }
}
