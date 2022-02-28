#ifndef KATANA_LIBTSUBA_KATANA_RDGSTORAGEFORMATVERSION_H_
#define KATANA_LIBTSUBA_KATANA_RDGSTORAGEFORMATVERSION_H_

#include <string_view>

#include "katana/Experimental.h"

namespace katana {

/// list of known storage format version
static const uint32_t kPartitionStorageFormatVersion1 = 1;
static const uint32_t kPartitionStorageFormatVersion2 = 2;
static const uint32_t kPartitionStorageFormatVersion3 = 3;
static const uint32_t kPartitionStorageFormatVersion4 = 4;
static const uint32_t kPartitionStorageFormatVersion5 = 5;
static const uint32_t kPartitionStorageFormatVersion6 = 6;

/// kLatestPartitionStorageFormatVersion to be bumped any time
/// the on disk format of RDGPartHeader changes
static const uint32_t kLatestPartitionStorageFormatVersion =
    kPartitionStorageFormatVersion6;

};  // namespace katana

#endif
