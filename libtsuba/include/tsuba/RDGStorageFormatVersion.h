#ifndef KATANA_LIBTSUBA_TSUBA_RDGSTORAGEFORMATVERSION_H_
#define KATANA_LIBTSUBA_TSUBA_RDGSTORAGEFORMATVERSION_H_

#include <string_view>

namespace tsuba {

/// list of known storage format version
static const uint32_t kPartitionStorageFormatVersion1 = 1;
static const uint32_t kPartitionStorageFormatVersion2 = 2;
static const uint32_t kPartitionStorageFormatVersion3 = 3;

/// kLatestPartitionStorageFormatVersion to be bumped any time
/// the on disk format of RDGPartHeader changes
static const uint32_t kLatestPartitionStorageFormatVersion =
    kPartitionStorageFormatVersion3;

};  // namespace tsuba

#endif
