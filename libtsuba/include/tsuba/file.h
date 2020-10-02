#ifndef GALOIS_LIBTSUBA_TSUBA_FILE_H_
#define GALOIS_LIBTSUBA_TSUBA_FILE_H_

#include <cstdint>
#include <future>
#include <string>
#include <string_view>
#include <unordered_set>

#include "galois/Result.h"
#include "galois/config.h"

namespace tsuba {

constexpr uint64_t kBlockSize = UINT64_C(4) << 10; /* 4K */
constexpr uint64_t kBlockOffsetMask = kBlockSize - 1;
constexpr uint64_t kBlockMask = ~kBlockOffsetMask;

template <typename T>
constexpr T
RoundDownToBlock(T val) {
  return val & kBlockMask;
}
template <typename T>
constexpr T
RoundUpToBlock(T val) {
  return RoundDownToBlock(val + kBlockOffsetMask);
}

struct StatBuf {
  uint64_t size{UINT64_C(0)};
};

// Download a file and open it
//[[deprecated]] (turning this on breaks the build)
galois::Result<int> FileOpen(const std::string& uri);

GALOIS_EXPORT galois::Result<void> FileCreate(
    const std::string& filename, bool overwrite = false);

// Map a particular chunk of this file (partial download). @begin and @size
// should be aligned to kBlockSize return value will be aligned to kBlockSize as
// well
GALOIS_EXPORT galois::Result<uint8_t*> FileMmap(
    const std::string& filename, uint64_t begin, uint64_t size);
GALOIS_EXPORT galois::Result<void> FileMunmap(uint8_t* ptr);

// Take whatever is in @data and put it a file called @uri
GALOIS_EXPORT galois::Result<void> FileStore(
    const std::string& uri, const uint8_t* data, uint64_t size);

// Take whatever is in @data and start putting it a the file called @uri
GALOIS_EXPORT galois::Result<std::future<galois::Result<void>>> FileStoreAsync(
    const std::string& uri, const uint8_t* data, uint64_t size);

/// List the set of files in a directory
/// \param directory is URI whose contents are listed. It can be
/// Async return type allows this function to be called repeatedly (and
/// synchronously)
/// \param list is populated with the files found
/// \param size is populated with the size of the corresponding file
///
/// \return future; files will be in `list` after this object
/// reports its return value
GALOIS_EXPORT galois::Result<std::future<galois::Result<void>>> FileListAsync(
    const std::string& directory, std::vector<std::string>* list,
    std::vector<uint64_t>* size = nullptr);

/// Delete a set of files in a directory
/// \param directory is a base URI
/// \param files is a set of file names relative to the directory that should be
/// deleted
GALOIS_EXPORT galois::Result<void> FileDelete(
    const std::string& directory, const std::unordered_set<std::string>& files);

// read a part of the file into a caller defined buffer
GALOIS_EXPORT galois::Result<void> FilePeek(
    const std::string& filename, uint8_t* result_buffer, uint64_t begin,
    uint64_t size);

// start reading a part of the file into a caller defined buffer
GALOIS_EXPORT galois::Result<std::future<galois::Result<void>>> FilePeekAsync(
    const std::string& filename, uint8_t* result_buffer, uint64_t begin,
    uint64_t size);

template <typename StrType, typename T>
static inline galois::Result<void>
FilePeek(const StrType& filename, T* obj) {
  return FilePeek(
      filename, reinterpret_cast<uint8_t*>(obj), /* NOLINT */
      0, sizeof(*obj));
}

GALOIS_EXPORT galois::Result<void> FileStat(
    const std::string& filename, StatBuf* s_buf);

}  // namespace tsuba

#endif
