/*************************************************************************************************
 * Utility functions
 *                                                               Copyright (C) 2009-2010 FAL Labs
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#ifndef _KCUTIL_H                        // duplication check
#define _KCUTIL_H

#include <kccommon.h>

namespace kyotocabinet {                 // common namespace


/** An alias of hash map of strings. */
typedef std::unordered_map<std::string, std::string> StringHashMap;


/** An alias of tree map of strings. */
typedef std::map<std::string, std::string> StringTreeMap;


/** The package version. */
extern const char* const VERSION;


/** The library version. */
extern const int32_t LIBVER;


/** The library revision. */
extern const int32_t LIBREV;


/** The database format version. */
extern const int32_t FMTVER;


/** The system name. */
extern const char* SYSNAME;


/** The flag for big endian environments. */
extern const bool BIGEND;


/** The clock tick of interruption. */
extern const int32_t CLOCKTICK;


/** The size of a page. */
extern const int32_t PAGESIZE;


/** The extra feature list. */
extern const char* const FEATURES;


/** The buffer size for numeric data. */
const size_t NUMBUFSIZ = 32;


/** The maximum memory size for debugging. */
const size_t MEMMAXSIZ = INT32_MAX / 2;


/**
 * Convert a decimal string to an integer.
 * @param str the decimal string.
 * @return the integer.  If the string does not contain numeric expression, 0 is returned.
 */
int64_t atoi(const char* str);


/**
 * Convert a decimal string with a metric prefix to an integer.
 * @param str the decimal string, which can be trailed by a binary metric prefix.  "K", "M", "G",
 * "T", "P", and "E" are supported.  They are case-insensitive.
 * @return the integer.  If the string does not contain numeric expression, 0 is returned.  If
 * the integer overflows the domain, INT64_MAX or INT64_MIN is returned according to the
 * sign.
 */
int64_t atoix(const char* str);


/**
 * Convert a hexadecimal string to an integer.
 * @param str the hexadecimal string.
 * @return the integer.  If the string does not contain numeric expression, 0 is returned.
 */
int64_t atoih(const char* str);


/**
 * Convert a decimal string to a real number.
 * @param str the decimal string.
 * @return the real number.  If the string does not contain numeric expression, 0.0 is returned.
 */
double atof(const char* str);


/**
 * Normalize a 16-bit number in the native order into the network byte order.
 * @param num the 16-bit number in the native order.
 * @return the number in the network byte order.
 */
uint16_t hton16(uint16_t num);


/**
 * Normalize a 32-bit number in the native order into the network byte order.
 * @param num the 32-bit number in the native order.
 * @return the number in the network byte order.
 */
uint32_t hton32(uint32_t num);


/**
 * Normalize a 64-bit number in the native order into the network byte order.
 * @param num the 64-bit number in the native order.
 * @return the number in the network byte order.
 */
uint64_t hton64(uint64_t num);


/**
 * Denormalize a 16-bit number in the network byte order into the native order.
 * @param num the 16-bit number in the network byte order.
 * @return the converted number in the native order.
 */
uint16_t ntoh16(uint16_t num);


/**
 * Denormalize a 32-bit number in the network byte order into the native order.
 * @param num the 32-bit number in the network byte order.
 * @return the converted number in the native order.
 */
uint32_t ntoh32(uint32_t num);


/**
 * Denormalize a 64-bit number in the network byte order into the native order.
 * @param num the 64-bit number in the network byte order.
 * @return the converted number in the native order.
 */
uint64_t ntoh64(uint64_t num);


/**
 * Write a number in fixed length format into a buffer.
 * @param buf the desitination buffer.
 * @param num the number.
 * @param width the width.
 */
void writefixnum(void* buf, uint64_t num, size_t width);


/**
 * Read a number in fixed length format from a buffer.
 * @param buf the source buffer.
 * @param width the width.
 * @return the read number.
 */
uint64_t readfixnum(const void* buf, size_t width);


/**
 * Write a number in variable length format into a buffer.
 * @param buf the desitination buffer.
 * @param num the number.
 * @return the length of the written region.
 */
size_t writevarnum(void* buf, uint64_t num);


/**
 * Read a number in variable length format from a buffer.
 * @param buf the source buffer.
 * @param size the size of the source buffer.
 * @param np the pointer to the variable into which the read number is assigned.
 * @return the length of the read region, or 0 on failure.
 */
size_t readvarnum(const void* buf, size_t size, uint64_t* np);


/**
 * Check the size of variable length format of a number.
 * @return the size of variable length format.
 */
size_t sizevarnum(uint64_t num);


/**
 * Get the hash value by MurMur hashing.
 * @param buf the source buffer.
 * @param size the size of the source buffer.
 * @return the hash value.
 */
uint64_t hashmurmur(const void* buf, size_t size);


/**
 * Get the hash value by FNV hashing.
 * @param buf the source buffer.
 * @param size the size of the source buffer.
 * @return the hash value.
 */
uint64_t hashfnv(const void* buf, size_t size);


/**
 * Get the hash value suitable for a file name.
 * @param buf the source buffer.
 * @param size the size of the source buffer.
 * @param obuf the buffer into which the result hash string is written.  It must be more than
 * NUMBUFSIZ.
 * @return the auxiliary hash value.
 */
uint32_t hashpath(const void* buf, size_t size, char* obuf);


/**
 * Get a prime number nearby a number.
 * @param num a natural number.
 * @return the result number.
 */
uint64_t nearbyprime(uint64_t num);


/**
 * Get the quiet Not-a-Number value.
 * @return the quiet Not-a-Number value.
 */
double nan();


/**
 * Get the positive infinity value.
 * @return the positive infinity value.
 */
double inf();


/**
 * Check a number is a Not-a-Number value.
 * @return true for the number is a Not-a-Number value, or false if not.
 */
bool chknan(double num);


/**
 * Check a number is an infinity value.
 * @return true for the number is an infinity value, or false if not.
 */
bool chkinf(double num);


/**
 * Append a formatted string at the end of a string.
 * @param dest the destination string.
 * @param format the printf-like format string.  The conversion character `%' can be used with
 * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
 * `S' treats the pointer to a std::string object.
 * @param ap used according to the format string.
 */
void vstrprintf(std::string* dest, const char* format, va_list ap);


/**
 * Append a formatted string at the end of a string.
 * @param dest the destination string.
 * @param format the printf-like format string.  The conversion character `%' can be used with
 * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
 * `S' treats the pointer to a std::string object.
 * @param ... used according to the format string.
 */
void strprintf(std::string* dest, const char* format, ...);


/**
 * Generate a formatted string.
 * @param format the printf-like format string.  The conversion character `%' can be used with
 * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
 * `S' treats the pointer to a std::string object.
 * @param ... used according to the format string.
 * @return the result string.
 */
std::string strprintf(const char* format, ...);


/**
 * Split a string with a delimiter.
 * @param str the string.
 * @param delim the delimiter.
 * @param elems a vector object into which the result elements are pushed.
 * @return the number of result elements.
 */
size_t strsplit(const std::string& str, char delim, std::vector<std::string>* elems);


/**
 * Split a string with delimiters.
 * @param str the string.
 * @param delims the delimiters.
 * @param elems a vector object into which the result elements are pushed.
 * @return the number of result elements.
 */
size_t strsplit(const std::string& str, const std::string& delims,
                std::vector<std::string>* elems);


/**
 * Encode a serial object by hexadecimal encoding.
 * @param buf the pointer to the region.
 * @param size the size of the region.
 * @return the result string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* hexencode(const void* buf, size_t size);


/**
 * Decode a string encoded by hexadecimal encoding.
 * @param str specifies the encoded string.
 * @param sp the pointer to the variable into which the size of the region of the return value
 * is assigned.
 * @return the pointer to the region of the result.
 * @note Because an additional zero code is appended at the end of the region of the return
 * value, the return value can be treated as a character string.  Because the region of the
 * return value is allocated with the the new[] operator, it should be released with the delete[]
 * operator when it is no longer in use.
 */
char* hexdecode(const char* str, size_t* sp);


/**
 * Encode a serial object by URL encoding.
 * @param buf the pointer to the region.
 * @param size the size of the region.
 * @return the result string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* urlencode(const void* buf, size_t size);


/**
 * Decode a string encoded by URL encoding.
 * @param str specifies the encoded string.
 * @param sp the pointer to the variable into which the size of the region of the return value
 * is assigned.
 * @return the pointer to the region of the result.
 * @note Because an additional zero code is appended at the end of the region of the return
 * value, the return value can be treated as a character string.  Because the region of the
 * return value is allocated with the the new[] operator, it should be released with the delete[]
 * operator when it is no longer in use.
 */
char* urldecode(const char* str, size_t* sp);


/**
 * Encode a serial object by Quoted-printable encoding.
 * @param buf the pointer to the region.
 * @param size the size of the region.
 * @return the result string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* quoteencode(const void* buf, size_t size);


/**
 * Decode a string encoded by Quoted-printable encoding.
 * @param str specifies the encoded string.
 * @param sp the pointer to the variable into which the size of the region of the return value
 * is assigned.
 * @return the pointer to the region of the result.
 * @note Because an additional zero code is appended at the end of the region of the return
 * value, the return value can be treated as a character string.  Because the region of the
 * return value is allocated with the the new[] operator, it should be released with the delete[]
 * operator when it is no longer in use.
 */
char* quotedecode(const char* str, size_t* sp);


/**
 * Encode a serial object by Base64 encoding.
 * @param buf the pointer to the region.
 * @param size the size of the region.
 * @return the result string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* baseencode(const void* buf, size_t size);


/**
 * Decode a string encoded by Base64 encoding.
 * @param str specifies the encoded string.
 * @param sp the pointer to the variable into which the size of the region of the return value
 * is assigned.
 * @return the pointer to the region of the result.
 * @note Because an additional zero code is appended at the end of the region of the return
 * value, the return value can be treated as a character string.  Because the region of the
 * return value is allocated with the the new[] operator, it should be released with the delete[]
 * operator when it is no longer in use.
 */
char* basedecode(const char* str, size_t* sp);


/**
 * Cipher or decipher a serial object with the Arcfour stream cipher.
 * @param ptr the pointer to the region.
 * @param size the size of the region.
 * @param kbuf the pointer to the region of the cipher key.
 * @param ksiz the size of the region of the cipher key.
 * @param obuf the pointer to the region into which the result data is written.  The size of the
 * buffer should be equal to or more than the input region.  The region can be the same as the
 * source region.
 */
void arccipher(const void* ptr, size_t size, const void* kbuf, size_t ksiz, void* obuf);


/**
 * Duplicate a region on memory.
 * @param ptr the source buffer.
 * @param size the size of the source buffer.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* memdup(const char* ptr, size_t size);


/**
 * Duplicate a string on memory.
 * @param str the source string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* strdup(const char* str);


/**
 * Convert the letters of a string into upper case.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strtoupper(char* str);


/**
 * Convert the letters of a string into lower case.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strtolower(char* str);


/**
 * Cut space characters at head or tail of a string.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strtrim(char* str);


/**
 * Squeeze space characters in a string and trim it.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strsqzspc(char* str);


/**
 * Normalize space characters in a string and trim it.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strnrmspc(char* str);


/**
 * Compare two strings with case insensitive evaluation.
 * @param astr a string.
 * @param bstr the other string.
 * @return positive if the former is big, negative if the latter is big, 0 if both are
 * equivalent.
 */
int32_t stricmp(const char* astr, const char* bstr);


/**
 * Check whether a string begins with a key.
 * @param str the string.
 * @param key the forward matching key string.
 * @return true if the target string begins with the key, else, it is false.
 */
bool strfwm(const char* str, const char* key);


/**
 * Check whether a string begins with a key by case insensitive evaluation.
 * @param str the string.
 * @param key the forward matching key string.
 * @return true if the target string begins with the key, else, it is false.
 */
bool strifwm(const char* str, const char* key);


/**
 * Check whether a string ends with a key.
 * @param str the string.
 * @param key the backward matching key string.
 * @return true if the target string ends with the key, else, it is false.
 */
bool strbwm(const char* str, const char* key);


/**
 * Check whether a string ends with a key by case insensitive evaluation.
 * @param str the string.
 * @param key the backward matching key string.
 * @return true if the target string ends with the key, else, it is false.
 */
bool stribwm(const char* str, const char* key);


/**
 * Allocate a region on memory.
 * @param size the size of the region.
 * @return the pointer to the allocated region.
 */
void* xmalloc(size_t size);


/**
 * Allocate a nullified region on memory.
 * @param nmemb the number of elements.
 * @param size the size of each element.
 * @return the pointer to the allocated region.
 */
void* xcalloc(size_t nmemb, size_t size);


/**
 * Re-allocate a region on memory.
 * @param ptr the pointer to the region.
 * @param size the size of the region.
 * @return the pointer to the re-allocated region.
 */
void* xrealloc(void* ptr, size_t size);


/**
 * Free a region on memory.
 * @param ptr the pointer to the region.
 */
void xfree(void* ptr);


/**
 * Allocate a nullified region on mapped memory.
 * @param size the size of the region.
 * @return the pointer to the allocated region.  It should be released with the memfree call.
 */
void* mapalloc(size_t size);


/**
 * Free a region on mapped memory.
 * @param ptr the pointer to the allocated region.
 */
void mapfree(void* ptr);


/**
 * Get the time of day in seconds.
 * @return the time of day in seconds.  The accuracy is in microseconds.
 */
double time();


/**
 * Get the process ID.
 * @return the process ID.
 */
int64_t getpid();


/**
 * Get the value of an environment variable.
 * @return the value of the environment variable, or NULL on failure.
 */
const char* getenv(const char* name);


/**
 * Get system information of the environment.
 * @param strmap a string map to contain the result.
 */
void getsysinfo(std::map<std::string, std::string>* strmap);


/**
 * Set the standard streams into the binary mode.
 */
void setstdiobin();


/**
 * Convert a decimal string to an integer.
 */
inline int64_t atoi(const char* str) {
  _assert_(str);
  while (*str > '\0' && *str <= ' ') {
    str++;
  }
  int32_t sign = 1;
  int64_t num = 0;
  if (*str == '-') {
    str++;
    sign = -1;
  } else if (*str == '+') {
    str++;
  }
  while (*str != '\0') {
    if (*str < '0' || *str > '9') break;
    num = num * 10 + *str - '0';
    str++;
  }
  return num * sign;
}


/**
 * Convert a decimal string with a metric prefix to an integer.
 */
inline int64_t atoix(const char* str) {
  _assert_(str);
  while (*str > '\0' && *str <= ' ') {
    str++;
  }
  int32_t sign = 1;
  if (*str == '-') {
    str++;
    sign = -1;
  } else if (*str == '+') {
    str++;
  }
  long double num = 0;
  while (*str != '\0') {
    if (*str < '0' || *str > '9') break;
    num = num * 10 + *str - '0';
    str++;
  }
  if (*str == '.') {
    str++;
    long double base = 10;
    while (*str != '\0') {
      if (*str < '0' || *str > '9') break;
      num += (*str - '0') / base;
      str++;
      base *= 10;
    }
  }
  num *= sign;
  while (*str > '\0' && *str <= ' ') {
    str++;
  }
  if (*str == 'k' || *str == 'K') {
    num *= 1LL << 10;
  } else if (*str == 'm' || *str == 'M') {
    num *= 1LL << 20;
  } else if (*str == 'g' || *str == 'G') {
    num *= 1LL << 30;
  } else if (*str == 't' || *str == 'T') {
    num *= 1LL << 40;
  } else if (*str == 'p' || *str == 'P') {
    num *= 1LL << 50;
  } else if (*str == 'e' || *str == 'E') {
    num *= 1LL << 60;
  }
  if (num > INT64_MAX) return INT64_MAX;
  if (num < INT64_MIN) return INT64_MIN;
  return (int64_t)num;
}


/**
 * Convert a hexadecimal string to an integer.
 */
inline int64_t atoih(const char* str) {
  _assert_(str);
  while (*str > '\0' && *str <= ' ') {
    str++;
  }
  if (str[0] == '0' && (str[1] == 'x' || str[1] == 'X')) {
    str += 2;
  }
  int64_t num = 0;
  while (true) {
    if (*str >= '0' && *str <= '9') {
      num = num * 0x10 + *str - '0';
    } else if (*str >= 'a' && *str <= 'f') {
      num = num * 0x10 + *str - 'a' + 10;
    } else if (*str >= 'A' && *str <= 'F') {
      num = num * 0x10 + *str - 'A' + 10;
    } else {
      break;
    }
    str++;
  }
  return num;
}


/**
 * Convert a decimal string to a real number.
 */
inline double atof(const char* str) {
  _assert_(str);
  while (*str > '\0' && *str <= ' ') {
    str++;
  }
  int32_t sign = 1;
  if (*str == '-') {
    str++;
    sign = -1;
  } else if (*str == '+') {
    str++;
  }
  if ((str[0] == 'i' || str[0] == 'I') && (str[1] == 'n' || str[1] == 'N') &&
      (str[2] == 'f' || str[2] == 'F')) return HUGE_VAL * sign;
  if ((str[0] == 'n' || str[0] == 'N') && (str[1] == 'a' || str[1] == 'A') &&
      (str[2] == 'n' || str[2] == 'N')) return nan();
  long double num = 0;
  int32_t col = 0;
  while (*str != '\0') {
    if (*str < '0' || *str > '9') break;
    num = num * 10 + *str - '0';
    str++;
    if (num > 0) col++;
  }
  if (*str == '.') {
    str++;
    long double fract = 0.0;
    long double base = 10;
    while (col < 16 && *str != '\0') {
      if (*str < '0' || *str > '9') break;
      fract += (*str - '0') / base;
      str++;
      col++;
      base *= 10;
    }
    num += fract;
  }
  if (*str == 'e' || *str == 'E') {
    str++;
    num *= std::pow((long double)10, (long double)atoi(str));
  }
  return num * sign;
}


/**
 * Normalize a 16-bit number in the native order into the network byte order.
 */
inline uint16_t hton16(uint16_t num) {
  _assert_(true);
  if (BIGEND) return num;
  return ((num & 0x00ffU) << 8) | ((num & 0xff00U) >> 8);
}


/**
 * Normalize a 32-bit number in the native order into the network byte order.
 */
inline uint32_t hton32(uint32_t num) {
  _assert_(true);
  if (BIGEND) return num;
  return ((num & 0x000000ffUL) << 24) | ((num & 0x0000ff00UL) << 8) | \
    ((num & 0x00ff0000UL) >> 8) | ((num & 0xff000000UL) >> 24);
}


/**
 * Normalize a 64-bit number in the native order into the network byte order.
 */
inline uint64_t hton64(uint64_t num) {
  _assert_(true);
  if (BIGEND) return num;
  return ((num & 0x00000000000000ffULL) << 56) | ((num & 0x000000000000ff00ULL) << 40) |
    ((num & 0x0000000000ff0000ULL) << 24) | ((num & 0x00000000ff000000ULL) << 8) |
    ((num & 0x000000ff00000000ULL) >> 8) | ((num & 0x0000ff0000000000ULL) >> 24) |
    ((num & 0x00ff000000000000ULL) >> 40) | ((num & 0xff00000000000000ULL) >> 56);
}


/**
 * Denormalize a 16-bit number in the network byte order into the native order.
 */
inline uint16_t ntoh16(uint16_t num) {
  _assert_(true);
  return hton16(num);
}


/**
 * Denormalize a 32-bit number in the network byte order into the native order.
 */
inline uint32_t ntoh32(uint32_t num) {
  _assert_(true);
  return hton32(num);
}


/**
 * Denormalize a 64-bit number in the network byte order into the native order.
 */
inline uint64_t ntoh64(uint64_t num) {
  _assert_(true);
  return hton64(num);
}


/**
 * Write a number in fixed length format into a buffer.
 */
inline void writefixnum(void* buf, uint64_t num, size_t width) {
  _assert_(buf && width <= sizeof(int64_t));
  num = hton64(num);
  std::memcpy(buf, (const char*)&num + sizeof(num) - width, width);
}


/**
 * Read a number in fixed length format from a buffer.
 */
inline uint64_t readfixnum(const void* buf, size_t width) {
  _assert_(buf && width <= sizeof(int64_t));
  uint64_t num = 0;
  std::memcpy(&num, buf, width);
  return ntoh64(num) >> ((sizeof(num) - width) * 8);
}


/**
 * Write a number in variable length format into a buffer.
 */
inline size_t writevarnum(void* buf, uint64_t num) {
  _assert_(buf);
  unsigned char* wp = (unsigned char*)buf;
  if (num < (1ULL << 7)) {
    *(wp++) = num;
  } else if (num < (1ULL << 14)) {
    *(wp++) = (num >> 7) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 21)) {
    *(wp++) = (num >> 14) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 28)) {
    *(wp++) = (num >> 21) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 35)) {
    *(wp++) = (num >> 28) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 42)) {
    *(wp++) = (num >> 35) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 49)) {
    *(wp++) = (num >> 42) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 56)) {
    *(wp++) = (num >> 49) | 0x80;
    *(wp++) = ((num >> 42) & 0x7f) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 63)) {
    *(wp++) = (num >> 56) | 0x80;
    *(wp++) = ((num >> 49) & 0x7f) | 0x80;
    *(wp++) = ((num >> 42) & 0x7f) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else {
    *(wp++) = (num >> 63) | 0x80;
    *(wp++) = ((num >> 56) & 0x7f) | 0x80;
    *(wp++) = ((num >> 49) & 0x7f) | 0x80;
    *(wp++) = ((num >> 42) & 0x7f) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  }
  return wp - (unsigned char*)buf;
}


/**
 * Read a number in variable length format from a buffer.
 */
inline size_t readvarnum(const void* buf, size_t size, uint64_t* np) {
  _assert_(buf && size <= MEMMAXSIZ && np);
  const unsigned char* rp = (const unsigned char*)buf;
  const unsigned char* ep = rp + size;
  uint64_t num = 0;
  uint32_t c;
  do {
    if (rp >= ep) {
      *np = 0;
      return 0;
    }
    c = *rp;
    num = (num << 7) + (c & 0x7f);
    rp++;
  } while (c >= 0x80);
  *np = num;
  return rp - (const unsigned char*)buf;
}


/**
 * Check the size of variable length format of a number.
 */
inline size_t sizevarnum(uint64_t num) {
  _assert_(true);
  if (num < (1ULL << 7)) return 1;
  if (num < (1ULL << 14)) return 2;
  if (num < (1ULL << 21)) return 3;
  if (num < (1ULL << 28)) return 4;
  if (num < (1ULL << 35)) return 5;
  if (num < (1ULL << 42)) return 6;
  if (num < (1ULL << 49)) return 7;
  if (num < (1ULL << 56)) return 8;
  if (num < (1ULL << 63)) return 9;
  return 10;
}


/**
 * Get the hash value by MurMur hashing.
 */
inline uint64_t hashmurmur(const void* buf, size_t size) {
  _assert_(buf && size <= MEMMAXSIZ);
  const uint64_t mul = 0xc6a4a7935bd1e995ULL;
  const int32_t rtt = 47;
  uint64_t hash = 19780211ULL ^ (size * mul);
  const unsigned char* rp = (const unsigned char*)buf;
  while (size >= sizeof(uint64_t)) {
    uint64_t num = ((uint64_t)rp[0] << 0) | ((uint64_t)rp[1] << 8) |
      ((uint64_t)rp[2] << 16) | ((uint64_t)rp[3] << 24) |
      ((uint64_t)rp[4] << 32) | ((uint64_t)rp[5] << 40) |
      ((uint64_t)rp[6] << 48) | ((uint64_t)rp[7] << 56);
    num *= mul;
    num ^= num >> rtt;
    num *= mul;
    hash *= mul;
    hash ^= num;
    rp += sizeof(uint64_t);
    size -= sizeof(uint64_t);
  }
  switch (size) {
    case 7: hash ^= (uint64_t)rp[6] << 48;
    case 6: hash ^= (uint64_t)rp[5] << 40;
    case 5: hash ^= (uint64_t)rp[4] << 32;
    case 4: hash ^= (uint64_t)rp[3] << 24;
    case 3: hash ^= (uint64_t)rp[2] << 16;
    case 2: hash ^= (uint64_t)rp[1] << 8;
    case 1: hash ^= (uint64_t)rp[0];
      hash *= mul;
  };
  hash ^= hash >> rtt;
  hash *= mul;
  hash ^= hash >> rtt;
  return hash;
}


/**
 * Get the hash value by FNV hashing.
 */
inline uint64_t hashfnv(const void* buf, size_t size) {
  _assert_(buf && size <= MEMMAXSIZ);
  uint64_t hash = 14695981039346656037ULL;
  const unsigned char* rp = (unsigned char*)buf;
  const unsigned char* ep = rp + size;
  while (rp < ep) {
    hash = (hash ^ *(rp++)) * 109951162811ULL;
  }
  return hash;
}


/**
 * Get the hash value suitable for a file name.
 */
inline uint32_t hashpath(const void* buf, size_t size, char* obuf) {
  _assert_(buf && size <= MEMMAXSIZ && obuf);
  const unsigned char* rp = (const unsigned char*)buf;
  uint32_t rv;
  char* wp = obuf;
  if (size <= 10) {
    if (size > 0) {
      const unsigned char* ep = rp + size;
      while (rp < ep) {
        int32_t num = *rp >> 4;
        if (num < 10) {
          *(wp++) = '0' + num;
        } else {
          *(wp++) = 'a' + num - 10;
        }
        num = *rp & 0x0f;
        if (num < 10) {
          *(wp++) = '0' + num;
        } else {
          *(wp++) = 'a' + num - 10;
        }
        rp++;
      }
    } else {
      *(wp++) = '0';
    }
    uint64_t hash = hashmurmur(buf, size);
    rv = (((hash & 0xffff000000000000ULL) >> 48) | ((hash & 0x0000ffff00000000ULL) >> 16)) ^
      (((hash & 0x000000000000ffffULL) << 16) | ((hash & 0x00000000ffff0000ULL) >> 16));
  } else {
    *(wp++) = 'f' + 1 + (size & 0x0f);
    for (int32_t i = 0; i <= 6; i += 3) {
      uint32_t num = (rp[i] ^ rp[i+1] ^ rp[i+2] ^
                      rp[size-i-1] ^ rp[size-i-2] ^ rp[size-i-3]) % 36;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
    }
    uint64_t hash = hashmurmur(buf, size);
    rv = (((hash & 0xffff000000000000ULL) >> 48) | ((hash & 0x0000ffff00000000ULL) >> 16)) ^
      (((hash & 0x000000000000ffffULL) << 16) | ((hash & 0x00000000ffff0000ULL) >> 16));
    uint64_t inc = hashfnv(buf, size);
    inc = (((inc & 0xffff000000000000ULL) >> 48) | ((inc & 0x0000ffff00000000ULL) >> 16)) ^
      (((inc & 0x000000000000ffffULL) << 16) | ((inc & 0x00000000ffff0000ULL) >> 16));
    for (size_t i = 0; i < sizeof(hash); i++) {
      uint32_t least = hash >> ((sizeof(hash) - 1) * 8);
      uint64_t num = least >> 4;
      if (inc & 0x01) num += 0x10;
      inc = inc >> 1;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
      num = least & 0x0f;
      if (inc & 0x01) num += 0x10;
      inc = inc >> 1;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
      hash = hash << 8;
    }
  }
  *wp = '\0';
  return rv;
}


/**
 * Get a prime number nearby a number.
 */
inline uint64_t nearbyprime(uint64_t num) {
  _assert_(true);
  static uint64_t table[] = {
    2ULL, 3ULL, 5ULL, 7ULL, 11ULL, 13ULL, 17ULL, 19ULL, 23ULL, 29ULL, 31ULL, 37ULL, 41ULL,
    43ULL, 47ULL, 53ULL, 59ULL, 61ULL, 67ULL, 71ULL, 79ULL, 97ULL, 107ULL, 131ULL, 157ULL,
    181ULL, 223ULL, 257ULL, 307ULL, 367ULL, 431ULL, 521ULL, 613ULL, 727ULL, 863ULL, 1031ULL,
    1217ULL, 1451ULL, 1723ULL, 2053ULL, 2437ULL, 2897ULL, 3449ULL, 4099ULL, 4871ULL, 5801ULL,
    6899ULL, 8209ULL, 9743ULL, 11587ULL, 13781ULL, 16411ULL, 19483ULL, 23173ULL, 27581ULL,
    32771ULL, 38971ULL, 46349ULL, 55109ULL, 65537ULL, 77951ULL, 92681ULL, 110221ULL, 131101ULL,
    155887ULL, 185363ULL, 220447ULL, 262147ULL, 311743ULL, 370759ULL, 440893ULL, 524309ULL,
    623521ULL, 741457ULL, 881743ULL, 1048583ULL, 1246997ULL, 1482919ULL, 1763491ULL,
    2097169ULL, 2493949ULL, 2965847ULL, 3526987ULL, 4194319ULL, 4987901ULL, 5931641ULL,
    7053971ULL, 8388617ULL, 9975803ULL, 11863289ULL, 14107921ULL, 16777259ULL, 19951597ULL,
    23726569ULL, 28215809ULL, 33554467ULL, 39903197ULL, 47453149ULL, 56431657ULL,
    67108879ULL, 79806341ULL, 94906297ULL, 112863217ULL, 134217757ULL, 159612679ULL,
    189812533ULL, 225726419ULL, 268435459ULL, 319225391ULL, 379625083ULL, 451452839ULL,
    536870923ULL, 638450719ULL, 759250133ULL, 902905657ULL, 1073741827ULL, 1276901429ULL,
    1518500279ULL, 1805811341ULL, 2147483659ULL, 2553802871ULL, 3037000507ULL, 3611622607ULL,
    4294967311ULL, 5107605691ULL, 6074001001ULL, 7223245229ULL, 8589934609ULL, 10215211387ULL,
    12148002047ULL, 14446490449ULL, 17179869209ULL, 20430422699ULL, 24296004011ULL,
    28892980877ULL, 34359738421ULL, 40860845437ULL, 48592008053ULL, 57785961671ULL,
    68719476767ULL, 81721690807ULL, 97184016049ULL, 115571923303ULL, 137438953481ULL,
    163443381347ULL, 194368032011ULL, 231143846587ULL, 274877906951ULL, 326886762733ULL,
    388736063999ULL, 462287693167ULL, 549755813911ULL, 653773525393ULL, 777472128049ULL,
    924575386373ULL, 1099511627791ULL, 1307547050819ULL, 1554944255989ULL, 1849150772699ULL,
    2199023255579ULL, 2615094101561ULL, 3109888512037ULL, 3698301545321ULL,
    4398046511119ULL, 5230188203153ULL, 6219777023959ULL, 7396603090651ULL,
    8796093022237ULL, 10460376406273ULL, 12439554047911ULL, 14793206181251ULL,
    17592186044423ULL, 20920752812471ULL, 24879108095833ULL, 29586412362491ULL,
    35184372088891ULL, 41841505624973ULL, 49758216191633ULL, 59172824724919ULL,
    70368744177679ULL, 83683011249917ULL, 99516432383281ULL, 118345649449813ULL,
    140737488355333ULL, 167366022499847ULL, 199032864766447ULL, 236691298899683ULL,
    281474976710677ULL, 334732044999557ULL, 398065729532981ULL, 473382597799229ULL,
    562949953421381ULL, 669464089999087ULL, 796131459065743ULL, 946765195598473ULL,
    1125899906842679ULL, 1338928179998197ULL, 1592262918131449ULL, 1893530391196921ULL,
    2251799813685269ULL, 2677856359996339ULL, 3184525836262943ULL, 3787060782393821ULL,
    4503599627370517ULL, 5355712719992603ULL, 6369051672525833ULL, 7574121564787633ULL
  };
  static const size_t tnum = sizeof(table) / sizeof(table[0]);
  uint64_t* ub = std::lower_bound(table, table + tnum, num);
  return ub == (uint64_t*)table + tnum ? num : *ub;
}


/**
 * Get the quiet Not-a-Number value.
 */
inline double nan() {
  _assert_(true);
  return std::numeric_limits<double>::quiet_NaN();
}


/**
 * Get the positive infinity value.
 */
inline double inf() {
  _assert_(true);
  return std::numeric_limits<double>::infinity();
}


/**
 * Check a number is a Not-a-Number value.
 */
inline bool chknan(double num) {
  _assert_(true);
  return num != num;
}


/**
 * Check a number is an infinity value.
 */
inline bool chkinf(double num) {
  _assert_(true);
  return num == inf() || num == -inf();
}


/**
 * Append a formatted string at the end of a string.
 */
inline void vstrprintf(std::string* dest, const char* format, va_list ap) {
  _assert_(dest && format);
  while (*format != '\0') {
    if (*format == '%') {
      char cbuf[NUMBUFSIZ];
      cbuf[0] = '%';
      size_t cbsiz = 1;
      int32_t lnum = 0;
      format++;
      while (std::strchr("0123456789 .+-hlLz", *format) && *format != '\0' &&
             cbsiz < NUMBUFSIZ - 1) {
        if (*format == 'l' || *format == 'L') lnum++;
        cbuf[cbsiz++] = *(format++);
      }
      cbuf[cbsiz++] = *format;
      cbuf[cbsiz] = '\0';
      switch (*format) {
        case 's': {
          const char* tmp = va_arg(ap, const char*);
          if (tmp) {
            dest->append(tmp);
          } else {
            dest->append("(null)");
          }
          break;
        }
        case 'S': {
          const std::string* tmp = va_arg(ap, const std::string*);
          if (tmp) {
            dest->append(*tmp);
          } else {
            dest->append("(null)");
          }
          break;
        }
        case 'd': {
          char tbuf[NUMBUFSIZ*4];
          size_t tsiz;
          if (lnum >= 2) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, long long));
          } else if (lnum >= 1) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, long));
          } else {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, int));
          }
          dest->append(tbuf, tsiz);
          break;
        }
        case 'o': case 'u': case 'x': case 'X': case 'c': {
          char tbuf[NUMBUFSIZ*4];
          size_t tsiz;
          if (lnum >= 2) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, unsigned long long));
          } else if (lnum >= 1) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, unsigned long));
          } else {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, unsigned int));
          }
          dest->append(tbuf, tsiz);
          break;
        }
        case 'e': case 'E': case 'f': case 'g': case 'G': {
          char tbuf[NUMBUFSIZ*4];
          size_t tsiz;
          if (lnum >= 1) {
            tsiz = std::snprintf(tbuf, sizeof(tbuf), cbuf, va_arg(ap, long double));
          } else {
            tsiz = std::snprintf(tbuf, sizeof(tbuf), cbuf, va_arg(ap, double));
          }
          if (tsiz > sizeof(tbuf)) {
            tbuf[sizeof(tbuf)-1] = '*';
            tsiz = sizeof(tbuf);
          }
          dest->append(tbuf, tsiz);
          break;
        }
        case 'p': {
          char tbuf[NUMBUFSIZ*4];
          size_t tsiz = std::sprintf(tbuf, "%p", va_arg(ap, void*));
          dest->append(tbuf, tsiz);
          break;
        }
        case '%': {
          dest->append("%", 1);
          break;
        }
      }
    } else {
      dest->append(format, 1);
    }
    format++;
  }
}


/**
 * Append a formatted string at the end of a string.
 */
inline void strprintf(std::string* dest, const char* format, ...) {
  _assert_(dest && format);
  va_list ap;
  va_start(ap, format);
  vstrprintf(dest, format, ap);
  va_end(ap);
}


/**
 * Generate a formatted string on memory.
 */
inline std::string strprintf(const char* format, ...) {
  _assert_(format);
  std::string str;
  va_list ap;
  va_start(ap, format);
  vstrprintf(&str, format, ap);
  va_end(ap);
  return str;
}


/**
 * Split a string with a delimiter
 */
inline size_t strsplit(const std::string& str, char delim, std::vector<std::string>* elems) {
  _assert_(elems);
  elems->clear();
  std::string::const_iterator it = str.begin();
  std::string::const_iterator pv = it;
  while (it != str.end()) {
    if (*it == delim) {
      std::string col(pv, it);
      elems->push_back(col);
      pv = it + 1;
    }
    it++;
  }
  std::string col(pv, it);
  elems->push_back(col);
  return elems->size();
}


/**
 * Split a string with delimiters.
 */
inline size_t strsplit(const std::string& str, const std::string& delims,
                       std::vector<std::string>* elems) {
  _assert_(elems);
  elems->clear();
  std::string::const_iterator it = str.begin();
  std::string::const_iterator pv = it;
  while (it != str.end()) {
    while (delims.find(*it, 0) != std::string::npos) {
      std::string col(pv, it);
      elems->push_back(col);
      pv = it + 1;
      break;
    }
    it++;
  }
  std::string col(pv, it);
  elems->push_back(col);
  return elems->size();
}


/**
 * Encode a serial object by hexadecimal encoding.
 */
inline char* hexencode(const void* buf, size_t size) {
  _assert_(buf && size <= MEMMAXSIZ);
  const unsigned char* rp = (const unsigned char*)buf;
  char* zbuf = new char[size*2+1];
  char* wp = zbuf;
  for (const unsigned char* ep = rp + size; rp < ep; rp++) {
    int32_t num = *rp >> 4;
    if (num < 10) {
      *(wp++) = '0' + num;
    } else {
      *(wp++) = 'a' + num - 10;
    }
    num = *rp & 0x0f;
    if (num < 10) {
      *(wp++) = '0' + num;
    } else {
      *(wp++) = 'a' + num - 10;
    }
  }
  *wp = '\0';
  return zbuf;
}


/**
 * Decode a string encoded by hexadecimal encoding.
 */
inline char* hexdecode(const char* str, size_t* sp) {
  _assert_(str && sp);
  size_t zsiz = std::strlen(str);
  char* zbuf = new char[zsiz+1];
  char* wp = zbuf;
  for (size_t i = 0; i < zsiz; i += 2) {
    while (str[i] >= '\0' && str[i] <= ' ') {
      i++;
    }
    int32_t num = 0;
    int32_t c = str[i];
    if (c == '\0') break;
    if (c >= '0' && c <= '9') {
      num = c - '0';
    } else if (c >= 'a' && c <= 'f') {
      num = c - 'a' + 10;
    } else if (c >= 'A' && c <= 'F') {
      num = c - 'A' + 10;
    } else if (c == '\0') {
      break;
    }
    c = str[i+1];
    if (c >= '0' && c <= '9') {
      num = num * 0x10 + c - '0';
    } else if (c >= 'a' && c <= 'f') {
      num = num * 0x10 + c - 'a' + 10;
    } else if (c >= 'A' && c <= 'F') {
      num = num * 0x10 + c - 'A' + 10;
    } else if (c == '\0') {
      break;
    }
    *(wp++) = num;
  }
  *wp = '\0';
  *sp = wp - zbuf;
  return zbuf;
}


/**
 * Encode a serial object by URL encoding.
 */
inline char* urlencode(const void* buf, size_t size) {
  _assert_(buf && size <= MEMMAXSIZ);
  const unsigned char* rp = (const unsigned char*)buf;
  char* zbuf = new char[size*3+1];
  char* wp = zbuf;
  for (const unsigned char* ep = rp + size; rp < ep; rp++) {
    int32_t c = *rp;
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || (c != '\0' && std::strchr("_-.!~*'()", c))) {
      *(wp++) = c;
    } else {
      *(wp++) = '%';
      int32_t num = c >> 4;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
      num = c & 0x0f;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
    }
  }
  *wp = '\0';
  return zbuf;
}


/**
 * Decode a string encoded by URL encoding.
 */
inline char* urldecode(const char* str, size_t* sp) {
  _assert_(str && sp);
  size_t zsiz = std::strlen(str);
  char* zbuf = new char[zsiz+1];
  char* wp = zbuf;
  const char* ep = str + zsiz;
  while (str < ep) {
    int32_t c = *str;
    if (c == '%') {
      int32_t num = 0;
      if (++str >= ep) break;
      c = *str;
      if (c >= '0' && c <= '9') {
        num = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        num = c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        num = c - 'A' + 10;
      }
      if (++str >= ep) break;
      c = *str;
      if (c >= '0' && c <= '9') {
        num = num * 0x10 + c - '0';
      } else if (c >= 'a' && c <= 'f') {
        num = num * 0x10 + c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        num = num * 0x10 + c - 'A' + 10;
      }
      *(wp++) = num;
      str++;
    } else if (c == '+') {
      *(wp++) = ' ';
      str++;
    } else if (c <= ' ' || c == 0x7f) {
      str++;
    } else {
      *(wp++) = c;
      str++;
    }
  }
  *wp = '\0';
  *sp = wp - zbuf;
  return zbuf;
}


/**
 * Encode a serial object by Quoted-printable encoding.
 */
inline char* quoteencode(const void* buf, size_t size) {
  _assert_(buf && size <= MEMMAXSIZ);
  const unsigned char* rp = (const unsigned char*)buf;
  char* zbuf = new char[size*3+1];
  char* wp = zbuf;
  for (const unsigned char* ep = rp + size; rp < ep; rp++) {
    int32_t c = *rp;
    if (c == '=' || c < ' ' || c > 0x7e) {
      *(wp++) = '=';
      int32_t num = c >> 4;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'A' + num - 10;
      }
      num = c & 0x0f;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'A' + num - 10;
      }
    } else {
      *(wp++) = c;
    }
  }
  *wp = '\0';
  return zbuf;
}


/**
 * Decode a string encoded by Quoted-printable encoding.
 */
inline char* quotedecode(const char* str, size_t* sp) {
  _assert_(str && sp);
  size_t zsiz = std::strlen(str);
  char* zbuf = new char[zsiz+1];
  char* wp = zbuf;
  const char* ep = str + zsiz;
  while (str < ep) {
    int32_t c = *str;
    if (c == '=') {
      int32_t num = 0;
      if (++str >= ep) break;
      c = *str;
      if (c == '\r') {
        if (++str >= ep) break;
        if (*str == '\n') str++;
      } else if (c == '\n') {
        str++;
      } else {
        if (c >= '0' && c <= '9') {
          num = c - '0';
        } else if (c >= 'a' && c <= 'f') {
          num = c - 'a' + 10;
        } else if (c >= 'A' && c <= 'F') {
          num = c - 'A' + 10;
        }
        if (++str >= ep) break;
        c = *str;
        if (c >= '0' && c <= '9') {
          num = num * 0x10 + c - '0';
        } else if (c >= 'a' && c <= 'f') {
          num = num * 0x10 + c - 'a' + 10;
        } else if (c >= 'A' && c <= 'F') {
          num = num * 0x10 + c - 'A' + 10;
        }
        *(wp++) = num;
        str++;
      }
    } else if (c < ' ' || c == 0x7f) {
      str++;
    } else {
      *(wp++) = c;
      str++;
    }
  }
  *wp = '\0';
  *sp = wp - zbuf;
  return zbuf;
}


/**
 * Encode a serial object by Base64 encoding.
 */
inline char* baseencode(const void* buf, size_t size) {
  _assert_(buf && size <= MEMMAXSIZ);
  const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const unsigned char* rp = (const unsigned char*)buf;
  char* zbuf = new char[size*4/3+5];
  char* wp = zbuf;
  for (size_t i = 0; i < size; i += 3) {
    switch (size - i) {
      case 1: {
        *(wp++) = tbl[rp[0] >> 2];
        *(wp++) = tbl[(rp[0] & 3) << 4];
        *(wp++) = '=';
        *(wp++) = '=';
        break;
      }
      case 2: {
        *(wp++) = tbl[rp[0] >> 2];
        *(wp++) = tbl[((rp[0] & 3) << 4) + (rp[1] >> 4)];
        *(wp++) = tbl[(rp[1] & 0xf) << 2];
        *(wp++) = '=';
        break;
      }
      default: {
        *(wp++) = tbl[rp[0] >> 2];
        *(wp++) = tbl[((rp[0] & 3) << 4) + (rp[1] >> 4)];
        *(wp++) = tbl[((rp[1] & 0xf) << 2) + (rp[2] >> 6)];
        *(wp++) = tbl[rp[2] & 0x3f];
        break;
      }
    }
    rp += 3;
  }
  *wp = '\0';
  return zbuf;
}


/**
 * Decode a string encoded by Base64 encoding.
 */
inline char* basedecode(const char* str, size_t* sp) {
  _assert_(str && sp);
  size_t bpos = 0;
  size_t eqcnt = 0;
  size_t len = std::strlen(str);
  unsigned char* zbuf = new unsigned char[len+4];
  unsigned char* wp = zbuf;
  size_t zsiz = 0;
  while (bpos < len && eqcnt == 0) {
    size_t bits = 0;
    size_t i;
    for (i = 0; bpos < len && i < 4; bpos++) {
      if (str[bpos] >= 'A' && str[bpos] <= 'Z') {
        bits = (bits << 6) | (str[bpos] - 'A');
        i++;
      } else if (str[bpos] >= 'a' && str[bpos] <= 'z') {
        bits = (bits << 6) | (str[bpos] - 'a' + 26);
        i++;
      } else if (str[bpos] >= '0' && str[bpos] <= '9') {
        bits = (bits << 6) | (str[bpos] - '0' + 52);
        i++;
      } else if (str[bpos] == '+') {
        bits = (bits << 6) | 62;
        i++;
      } else if (str[bpos] == '/') {
        bits = (bits << 6) | 63;
        i++;
      } else if (str[bpos] == '=') {
        bits <<= 6;
        i++;
        eqcnt++;
      }
    }
    if (i == 0 && bpos >= len) continue;
    switch (eqcnt) {
      case 0: {
        *wp++ = (bits >> 16) & 0xff;
        *wp++ = (bits >> 8) & 0xff;
        *wp++ = bits & 0xff;
        zsiz += 3;
        break;
      }
      case 1: {
        *wp++ = (bits >> 16) & 0xff;
        *wp++ = (bits >> 8) & 0xff;
        zsiz += 2;
        break;
      }
      case 2: {
        *wp++ = (bits >> 16) & 0xff;
        zsiz += 1;
        break;
      }
    }
  }
  zbuf[zsiz] = '\0';
  *sp = zsiz;
  return (char*)zbuf;
}


/**
 * Cipher or decipher a serial object with the Arcfour stream cipher.
 */
inline void arccipher(const void* ptr, size_t size, const void* kbuf, size_t ksiz, void* obuf) {
  _assert_(ptr && size <= MEMMAXSIZ && kbuf && ksiz <= MEMMAXSIZ && obuf);
  if (ksiz < 1) {
    kbuf = "";
    ksiz = 1;
  }
  uint32_t sbox[0x100], kbox[0x100];
  for (int32_t i = 0; i < 0x100; i++) {
    sbox[i] = i;
    kbox[i] = ((uint8_t*)kbuf)[i%ksiz];
  }
  uint32_t sidx = 0;
  for (int32_t i = 0; i < 0x100; i++) {
    sidx = (sidx + sbox[i] + kbox[i]) & 0xff;
    uint32_t swap = sbox[i];
    sbox[i] = sbox[sidx];
    sbox[sidx] = swap;
  }
  uint32_t x = 0;
  uint32_t y = 0;
  for (size_t i = 0; i < size; i++) {
    x = (x + 1) & 0xff;
    y = (y + sbox[x]) & 0xff;
    uint32_t swap = sbox[x];
    sbox[x] = sbox[y];
    sbox[y] = swap;
    ((uint8_t*)obuf)[i] = ((uint8_t*)ptr)[i] ^ sbox[(sbox[x]+sbox[y])&0xff];
  }
}


/**
 * Duplicate a region on memory.
 */
inline char* memdup(const char* ptr, size_t size) {
  _assert_(ptr && size <= MEMMAXSIZ);
  char* obuf = new char[size+1];
  std::memcpy(obuf, ptr, size);
  return obuf;
}


/**
 * Duplicate a string on memory.
 */
inline char* strdup(const char* str) {
  _assert_(str);
  size_t size = std::strlen(str);
  char* obuf = memdup(str, size);
  obuf[size] = '\0';
  return obuf;
}


/**
 * Convert the letters of a string into upper case.
 */
inline char* strtoupper(char* str) {
  _assert_(str);
  char* wp = str;
  while (*wp != '\0') {
    if (*wp >= 'a' && *wp <= 'z') *wp -= 'a' - 'A';
    wp++;
  }
  return str;
}


/**
 * Convert the letters of a string into lower case.
 */
inline char* strtolower(char* str) {
  _assert_(str);
  char* wp = str;
  while (*wp != '\0') {
    if (*wp >= 'A' && *wp <= 'Z') *wp += 'a' - 'A';
    wp++;
  }
  return str;
}


/**
 * Cut space characters at head or tail of a string.
 */
inline char* strtrim(char* str) {
  _assert_(str);
  const char* rp = str;
  char* wp = str;
  bool head = true;
  while (*rp != '\0') {
    if (*rp > '\0' && *rp <= ' ') {
      if (!head) *(wp++) = *rp;
    } else {
      *(wp++) = *rp;
      head = false;
    }
    rp++;
  }
  *wp = '\0';
  while (wp > str && wp[-1] > '\0' && wp[-1] <= ' ') {
    *(--wp) = '\0';
  }
  return str;
}


/**
 * Squeeze space characters in a string and trim it.
 */
inline char* strsqzspc(char* str) {
  _assert_(str);
  const char* rp = str;
  char* wp = str;
  bool spc = true;
  while (*rp != '\0') {
    if (*rp > '\0' && *rp <= ' ') {
      if (!spc) *(wp++) = *rp;
      spc = true;
    } else {
      *(wp++) = *rp;
      spc = false;
    }
    rp++;
  }
  *wp = '\0';
  for (wp--; wp >= str; wp--) {
    if (*wp > '\0' && *wp <= ' ') {
      *wp = '\0';
    } else {
      break;
    }
  }
  return str;
}


/**
 * Normalize space characters in a string and trim it.
 */
inline char* strnrmspc(char* str) {
  _assert_(str);
  const char* rp = str;
  char* wp = str;
  bool spc = true;
  while (*rp != '\0') {
    if ((*rp > '\0' && *rp <= ' ') || *rp == 0x7f) {
      if (!spc) *(wp++) = ' ';
      spc = true;
    } else {
      *(wp++) = *rp;
      spc = false;
    }
    rp++;
  }
  *wp = '\0';
  for (wp--; wp >= str; wp--) {
    if (*wp == ' ') {
      *wp = '\0';
    } else {
      break;
    }
  }
  return str;
}



/**
 * Compare two strings with case insensitive evaluation.
 */
inline int32_t stricmp(const char* astr, const char* bstr) {
  _assert_(astr && bstr);
  while (*astr != '\0') {
    if (*bstr == '\0') return 1;
    int32_t ac = (*astr >= 'A' && *astr <= 'Z') ? *astr + ('a' - 'A') : *(unsigned char*)astr;
    int32_t bc = (*bstr >= 'A' && *bstr <= 'Z') ? *bstr + ('a' - 'A') : *(unsigned char*)bstr;
    if (ac != bc) return ac - bc;
    astr++;
    bstr++;
  }
  return (*bstr == '\0') ? 0 : -1;
}


/**
 * Check whether a string begins with a key.
 */
inline bool strfwm(const char* str, const char* key) {
  _assert_(str && key);
  while (*key != '\0') {
    if (*str != *key || *str == '\0') return false;
    key++;
    str++;
  }
  return true;
}


/**
 * Check whether a string begins with a key by case insensitive evaluation.
 */
inline bool strifwm(const char* str, const char* key) {
  _assert_(str && key);
  while (*key != '\0') {
    if (*str == '\0') return false;
    int32_t sc = *str;
    if (sc >= 'A' && sc <= 'Z') sc += 'a' - 'A';
    int32_t kc = *key;
    if (kc >= 'A' && kc <= 'Z') kc += 'a' - 'A';
    if (sc != kc) return false;
    key++;
    str++;
  }
  return true;
}


/**
 * Check whether a string ends with a key.
 */
inline bool strbwm(const char* str, const char* key) {
  _assert_(str && key);
  size_t slen = std::strlen(str);
  size_t klen = std::strlen(key);
  for (size_t i = 1; i <= klen; i++) {
    if (i > slen || str[slen-i] != key[klen-i]) return false;
  }
  return true;
}


/**
 * Check whether a string ends with a key by case insensitive evaluation.
 */
inline bool stribwm(const char* str, const char* key) {
  _assert_(str && key);
  size_t slen = std::strlen(str);
  size_t klen = std::strlen(key);
  for (size_t i = 1; i <= klen; i++) {
    if (i > slen) return false;
    int32_t sc = str[slen-i];
    if (sc >= 'A' && sc <= 'Z') sc += 'a' - 'A';
    int32_t kc = key[klen-i];
    if (kc >= 'A' && kc <= 'Z') kc += 'a' - 'A';
    if (sc != kc) return false;
  }
  return true;
}


/**
 * Allocate a region on memory.
 */
inline void* xmalloc(size_t size) {
  _assert_(size <= MEMMAXSIZ);
  void* ptr = std::malloc(size);
  if (!ptr) throw std::bad_alloc();
  return ptr;
}


/**
 * Allocate a nullified region on memory.
 */
inline void* xcalloc(size_t nmemb, size_t size) {
  _assert_(nmemb <= MEMMAXSIZ && size <= MEMMAXSIZ);
  void* ptr = std::calloc(nmemb, size);
  if (!ptr) throw std::bad_alloc();
  return ptr;
}


/**
 * Re-allocate a region on memory.
 */
inline void* xrealloc(void* ptr, size_t size) {
  _assert_(size <= MEMMAXSIZ);
  ptr = std::realloc(ptr, size);
  if (!ptr) throw std::bad_alloc();
  return ptr;
}


/**
 * Free a region on memory.
 */
inline void xfree(void* ptr) {
  _assert_(true);
  std::free(ptr);
}


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
