#ifndef GALOIS_LIBTSUBA_TSUBA_TSUBA_H_
#define GALOIS_LIBTSUBA_TSUBA_TSUBA_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include <arrow/api.h>

#include "galois/Result.h"
#include "tsuba/RDG.h"

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

/* Check to see if the name is formed in a way that tsuba expects */
static inline bool IsUri(std::string_view uri) {
  return uri.find("s3://") == 0;
}

struct StatBuf {
  uint64_t size;
};

// acceptable values for Open's flags
constexpr int kReadOnly  = 0;
constexpr int kReadWrite = 1;
// Open an RDGHandle object from storage
galois::Result<std::shared_ptr<RDGHandle>> Open(const std::string& rdg_name,
                                                int flags);

galois::Result<void> Create(const std::string& name);

// acceptable values for Rename's flags
constexpr int kOverwrite = 1;
// Rename
galois::Result<void> Rename(std::shared_ptr<RDGHandle> handle,
                            const std::string& name, int flags);

// Load the RDG described by the metadata in @handle into memory
galois::Result<RDG> Load(std::shared_ptr<RDGHandle> handle);
// Load the RDG described by the metadata in @handle into memory, but only
//    populate the listed properties
galois::Result<RDG> Load(std::shared_ptr<RDGHandle> handle,
                         const std::vector<std::string>& node_properties,
                         const std::vector<std::string>& edge_properties);

galois::Result<void> Store(const RDG& rdg);
galois::Result<void> Store(const RDG& rdg, const void* new_top_buf,
                           uint64_t new_top_size);

galois::Result<void>
AddNodeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);
galois::Result<void>
AddEdgeProperties(RDG* rdg, const std::shared_ptr<arrow::Table>& table);

galois::Result<void> DropNodeProperty(RDG* rdg, int i);
galois::Result<void> DropEdgeProperty(RDG* rdg, int i);

/* Setup and tear down */
int Init();
void Fini();

/* Download a file and open it */
int Open(const std::string& uri);

/* map a particular chunk of this file (partial download)
 * @begin and @size should be well aligned to TSUBA_BLOCK_SIZE
 * return value will be aligned to tsuba_block_size as well
 */
uint8_t* Mmap(const std::string& filename, uint64_t begin, uint64_t size);
int Munmap(uint8_t* ptr);

/* Take whatever is in @data and put it a file called @uri */
int Store(const std::string& uri, const uint8_t* data, uint64_t size);

/* Take whatever is in @data and put it a file called @uri */
int StoreSync(const std::string& uri, const uint8_t* data, uint64_t size);

/* Take whatever is in @data and start putting it a the file called @uri */
int StoreAsync(const std::string& uri, const uint8_t* data, uint64_t size);
/* Make sure put has occurred, and wait if it hasn't */
int StoreAsyncFinish(const std::string& uri);

/* Take whatever is in @data and start putting it a the file called @uri */
int StoreMultiAsync1(const std::string& uri, const uint8_t* data,
                     uint64_t size);
int StoreMultiAsync2(const std::string& uri);
int StoreMultiAsync3(const std::string& uri);
/* Make sure put has occurred, and wait if it hasn't */
int StoreMultiAsyncFinish(const std::string& uri);

/* read a (probably small) part of the file into a caller defined buffer */
int Peek(const std::string& filename, uint8_t* result_buffer, uint64_t begin,
         uint64_t size);

template <typename StrType, typename T>
static inline int Peek(const StrType& filename, T* obj) {
  return Peek(filename, reinterpret_cast<uint8_t*>(obj), /* NOLINT */
              0, sizeof(*obj));
}

int Stat(const std::string& filename, StatBuf* s_buf);

} // namespace tsuba

#endif
