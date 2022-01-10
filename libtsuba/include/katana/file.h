#ifndef KATANA_LIBTSUBA_KATANA_FILE_H_
#define KATANA_LIBTSUBA_KATANA_FILE_H_

#include <cstdint>
#include <future>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>

#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

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

// Returns an error file uri does not exist
KATANA_EXPORT katana::Result<void> FileStat(
    const std::string& uri, StatBuf* s_buf);

/// Take whatever is in a buffer and put it in the file
///
/// \param uri the destination file to fill with data
/// \param data a pointer to the buffer containing data
/// \param size the length of the buffer
KATANA_EXPORT katana::Result<void> FileStore(
    const std::string& uri, const void* data, uint64_t size);

template <
    class ContigContainer,
    std::enable_if_t<
        std::is_standard_layout<typename ContigContainer::value_type>::value,
        bool> = true>
KATANA_EXPORT katana::Result<void>
FileStore(const std::string& uri, const ContigContainer& container) {
  return FileStore(
      uri, container.data(),
      container.size() * sizeof(typename ContigContainer::value_type));
}

/// Copy a slice of a file from source_uri into dest_uri
/// using a remote operation (avoiding a roundt rip through memory) if possible.
/// The slice starts at \param begin and extends \param size bytes.
/// The caller is responsible for ensuring that the slice is valid. This
/// operation is only well defined if source_uri and dest_uri
/// map to the (i.e., one of: s3, gs, azure blob store, or local file system)
///
/// \param source_uri source URI
/// \param dest_uri destination URI
KATANA_EXPORT katana::Result<void> FileRemoteCopy(
    const std::string& source_uri, const std::string& dest_uri, uint64_t begin,
    uint64_t size);

/// Take whatever is in a buffer and put it in the file
///
/// \param uri the destination file to fill with data
/// \param data a pointer to the buffer containing data
/// \param size the length of the buffer
KATANA_EXPORT std::future<katana::CopyableResult<void>> FileStoreAsync(
    const std::string& uri, const void* data, uint64_t size);

// read a part of the file into a caller defined buffer
KATANA_EXPORT katana::Result<void> FileGet(
    const std::string& uri, void* result_buffer, uint64_t begin, uint64_t size);

template <
    typename StrType, typename T,
    std::enable_if_t<std::is_standard_layout<T>::value, bool> = true>
static inline katana::Result<void>
FileGet(const StrType& uri, T* obj) {
  return FileGet(uri, obj, 0, sizeof(T));
}

// start reading a part of the file into a caller defined buffer
KATANA_EXPORT std::future<katana::CopyableResult<void>> FileGetAsync(
    const std::string& uri, void* result_buffer, uint64_t begin, uint64_t size);

/// List the set of files in a directory
/// \param directory is URI whose contents are listed. It can be
/// Async return type allows this function to be called repeatedly (and
/// synchronously)
/// \param list is populated with the files found
/// \param size is populated with the size of the corresponding file
///
/// \return future; files will be in `list` after this object
/// reports its return value
KATANA_EXPORT std::future<katana::CopyableResult<void>> FileListAsync(
    const std::string& directory, std::vector<std::string>* list,
    std::vector<uint64_t>* size = nullptr);

/// Delete a set of files in a directory
/// \param directory is a base URI
/// \param files is a set of file names relative to the directory that should be
/// deleted
KATANA_EXPORT katana::Result<void> FileDelete(
    const std::string& directory, const std::unordered_set<std::string>& files);

}  // namespace katana

#endif
