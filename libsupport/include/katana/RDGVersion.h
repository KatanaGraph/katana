#ifndef KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_
#define KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_

#include <cstring>
#include <iostream>
#include <iterator>
#include <vector>

#include <fmt/format.h>

#include "katana/config.h"

namespace katana {

struct KATANA_EXPORT RDGVersion {
  // A vectorized version in the form of num:id
  // The last one has an empty branch "".
  std::vector<uint64_t> numbers_{0};
  std::vector<std::string> branches_{""};

  // TODO(wkyu): to clean up these operators
#if 1
  RDGVersion(const RDGVersion& in) = default;
  RDGVersion& operator=(const RDGVersion& in) = default;
#else
  RDGVersion(const RDGVersion& in)
      : numbers_(in.numbers_), branches_(in.branches_) {}

  RDGVersion& operator=(const RDGVersion& in) {
    RDGVersion tmp = in;
    numbers_ = std::move(tmp.numbers_);
    branches_ = std::move(tmp.branches_);
    return *this;
  }
#endif

  RDGVersion(
      const std::vector<uint64_t>& vers, const std::vector<std::string>& ids);
  explicit RDGVersion(uint64_t num = 0);
  std::string GetBranchPath() const;
  std::string ToVectorString() const;
  uint64_t LeafVersionNumber();
  void SetNextVersion();
  void SetBranchPoint(const std::string& name);
  std::vector<uint64_t>& GetVersionNumbers();
  std::vector<std::string>& GetBranchIDs();
};

KATANA_EXPORT bool operator==(const RDGVersion& lhs, const RDGVersion& rhs);
KATANA_EXPORT bool operator>(const RDGVersion& lhs, const RDGVersion& rhs);
KATANA_EXPORT bool operator<(const RDGVersion& lhs, const RDGVersion& rhs);

// Check the example from URI.h on comparators and formatter.

}  // namespace katana
#endif
