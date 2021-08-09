#include "katana/RDGVersion.h"

#include "katana/Logging.h"

namespace katana {

RDGVersion::RDGVersion(
    const std::vector<uint64_t>& vers, const std::vector<std::string>& ids)
    : numbers_(vers), branches_(ids) {}

RDGVersion::RDGVersion(uint64_t num) { numbers_.back() = num; }

std::string
RDGVersion::GetBranchPath() const {
  std::string vec = "";
  //KATANA_LOG_VERBOSE("number size {}, branch size {}", numbers_.size(), branches_.size());
  // return a subdir formed by branches without a trailing SepChar
  for (uint32_t i = 0; (i + 1) < branches_.size(); i++) {
    vec += fmt::format("{}", branches_[i]);
    // the last two branches (ID:"") will not need a SepChar
    if ((i + 2) < branches_.size())
      vec += "/";
  }
  return vec;
}

std::string
RDGVersion::ToVectorString() const {
  std::string vec = "";
  //KATANA_LOG_VERBOSE("number size {}, branch size {}", numbers_.size(), branches_.size());
  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    vec += fmt::format("{}_{},", numbers_[i], branches_[i]);
  }
  // Last one has only the ver number.
  return fmt::format("{}{}", vec, numbers_.back());
}

uint64_t
RDGVersion::LeafVersionNumber() {
  return numbers_.back();
}

void
RDGVersion::SetNextVersion() {
  numbers_.back()++;
}

void
RDGVersion::SetBranchPoint(const std::string& name) {
  branches_.back() = name;
  // 1 to begin a branch
  numbers_.emplace_back(1);
  branches_.emplace_back(".");
}

std::vector<uint64_t>&
RDGVersion::GetVersionNumbers() {
  return numbers_;
}

std::vector<std::string>&
RDGVersion::GetBranchIDs() {
  return branches_;
}

bool
operator==(const RDGVersion& lhs, const RDGVersion& rhs) {
  return (lhs.numbers_ == rhs.numbers_ && lhs.branches_ == rhs.branches_);
}

bool
operator>(const RDGVersion& lhs, const RDGVersion& rhs) {
  return lhs.numbers_ > rhs.numbers_;
}

bool
operator<(const RDGVersion& lhs, const RDGVersion& rhs) {
  return lhs.numbers_ < rhs.numbers_;
}

// Check the example from URI.h on comparitors and formatter.

}  // namespace katana
