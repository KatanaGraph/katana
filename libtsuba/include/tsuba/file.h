#ifndef GALOIS_LIBTSUBA_TSUBA_FILE_H_
#define GALOIS_LIBTSUBA_TSUBA_FILE_H_

#include <cstdint>
#include <string_view>
#include <string>

#include "galois/config.h"
#include "galois/Result.h"
#include "tsuba/FileAsyncWork.h"

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

struct StatBuf {
  uint64_t size;
};

// Download a file and open it
//[[deprecated]] (turning this on breaks the build)
galois::Result<int> FileOpen(const std::string& uri);

GALOIS_EXPORT galois::Result<void> FileCreate(const std::string& filename,
                                              bool overwrite = false);

// Map a particular chunk of this file (partial download). @begin and @size
// should be aligned to kBlockSize return value will be aligned to kBlockSize as
// well
GALOIS_EXPORT galois::Result<uint8_t*> FileMmap(const std::string& filename,
                                                uint64_t begin, uint64_t size);
GALOIS_EXPORT galois::Result<void> FileMunmap(uint8_t* ptr);

// Take whatever is in @data and put it a file called @uri
GALOIS_EXPORT galois::Result<void>
FileStore(const std::string& uri, const uint8_t* data, uint64_t size);

// Take whatever is in @data and start putting it a the file called @uri
GALOIS_EXPORT galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
FileStoreAsync(const std::string& uri, const uint8_t* data, uint64_t size);

// read a part of the file into a caller defined buffer
GALOIS_EXPORT galois::Result<void> FilePeek(const std::string& filename,
                                            uint8_t* result_buffer,
                                            uint64_t begin, uint64_t size);

// start reading a part of the file into a caller defined buffer
GALOIS_EXPORT galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
FilePeekAsync(const std::string& filename, uint8_t* result_buffer,
              uint64_t begin, uint64_t size);

template <typename StrType, typename T>
static inline galois::Result<void> FilePeek(const StrType& filename, T* obj) {
  return FilePeek(filename, reinterpret_cast<uint8_t*>(obj), /* NOLINT */
                  0, sizeof(*obj));
}

GALOIS_EXPORT galois::Result<void> FileStat(const std::string& filename,
                                            StatBuf* s_buf);

} // namespace tsuba

#endif
