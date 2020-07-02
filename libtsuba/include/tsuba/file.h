#ifndef GALOIS_LIBTSUBA_TSUBA_FILE_H_
#define GALOIS_LIBTSUBA_TSUBA_FILE_H_

#include <cstdint>
#include <string_view>
#include <string>

#include "galois/Result.h"

namespace tsuba {

constexpr uint64_t kBlockSize       = 4UL << 10; /* 4K */
constexpr uint64_t kBlockOffsetMask = kBlockSize - 1;
constexpr uint64_t kBlockMask       = ~kBlockOffsetMask;

template <typename T>
constexpr T RoundDownToBlock(T val) {
  return val & kBlockMask;
}
template <typename T>
constexpr T RoundUpToBlock(T val) {
  return RoundDownToBlock(val + kBlockOffsetMask);
}

// Check to see if the name is formed in a way that tsuba expects
static inline bool IsUri(std::string_view uri) {
  return uri.find("s3://") == 0;
}

struct StatBuf {
  uint64_t size;
};

// Download a file and open it
int FileOpen(const std::string& uri);

// Map a particular chunk of this file (partial download). @begin and @size
// should be aligned to kBlockSize return value will be aligned to kBlockSize as
// well
uint8_t* FileMmap(const std::string& filename, uint64_t begin, uint64_t size);
int FileMunmap(uint8_t* ptr);

// Take whatever is in @data and put it a file called @uri
int FileStore(const std::string& uri, const uint8_t* data, uint64_t size);

// Take whatever is in @data and put it a file called @uri
int FileStoreSync(const std::string& uri, const uint8_t* data, uint64_t size);

// Take whatever is in @data and start putting it a the file called @uri
int FileStoreAsync(const std::string& uri, const uint8_t* data, uint64_t size);
// Make sure put has occurred, and wait if it hasn't
int FileStoreAsyncFinish(const std::string& uri);

// Take whatever is in @data and start putting it a the file called @uri
int FileStoreMultiAsync1(const std::string& uri, const uint8_t* data,
                         uint64_t size);
int FileStoreMultiAsync2(const std::string& uri);
int FileStoreMultiAsync3(const std::string& uri);
// Make sure put has occurred, and wait if it hasn't
int FileStoreMultiAsyncFinish(const std::string& uri);

// read a (probably small) part of the file into a caller defined buffer
int FilePeek(const std::string& filename, uint8_t* result_buffer,
             uint64_t begin, uint64_t size);

template <typename StrType, typename T>
static inline int FilePeek(const StrType& filename, T* obj) {
  return FilePeek(filename, reinterpret_cast<uint8_t*>(obj), /* NOLINT */
                  0, sizeof(*obj));
}

int FileStat(const std::string& filename, StatBuf* s_buf);

} // namespace tsuba

#endif
