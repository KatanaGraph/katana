#ifndef KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_
#define KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_

#include <cstring>
#include <iostream>
#include <iterator>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include "katana/Random.h"
#include "katana/config.h"

namespace katana {

const uint64_t kRDGVersionMaxID = (1 << 30);
const uint64_t kRDGVersionIDLength = (20);

struct KATANA_EXPORT RDGVersion {
  // A vectorized version in the form of num:id
  // The last one has an empty branch "".
  std::vector<uint64_t> numbers_{0};
  std::vector<std::string> branches_{"."};

  RDGVersion(const RDGVersion& in) = default;
  RDGVersion& operator=(const RDGVersion& in) = default;

  RDGVersion(
      const std::vector<uint64_t>& nums,
      const std::vector<std::string>& branches);
  RDGVersion(const std::string& str);
  explicit RDGVersion(uint64_t num = 0);

  // Accessors
  std::string ToString() const;
  uint64_t LeafNumber() const;
  bool ShareBranch(const RDGVersion& in) const;
  bool NullNumber() const;
  bool NullBranch() const;
  bool IsNull() const;

  // Mutators
  void IncrementLeaf(uint64_t num = 1);
  void SetLeafNumber(uint64_t num = 0);
  void AddBranch(const std::string& name);
};

KATANA_EXPORT bool operator==(const RDGVersion& lhs, const RDGVersion& rhs);
KATANA_EXPORT bool operator!=(const RDGVersion& lhs, const RDGVersion& rhs);
KATANA_EXPORT bool operator>(const RDGVersion& lhs, const RDGVersion& rhs);
KATANA_EXPORT bool operator<(const RDGVersion& lhs, const RDGVersion& rhs);

void to_json(nlohmann::json& j, const RDGVersion& version);
void from_json(const nlohmann::json& j, RDGVersion& version);
}  // namespace katana

#endif
