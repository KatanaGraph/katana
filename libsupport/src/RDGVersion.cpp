#include "katana/RDGVersion.h"

namespace katana {

RDGVersion::RDGVersion(
    const std::vector<uint64_t>& vers, const std::vector<std::string>& ids)
    : numbers_(vers), branches_(ids) {
  width_ = vers.size() - 1;
}

RDGVersion::RDGVersion(uint64_t num) {
  numbers_.emplace_back(num);
  branches_.emplace_back("");
  width_ = 0;
}

std::string
RDGVersion::GetBranchPath() const {
  std::string vec = "";
  if (numbers_.back() <= 0)
    return vec;

  // return a subdir formed by branches without a trailing SepChar
  for (uint64_t i = 0; i < width_; i++) {
    vec += fmt::format("{}", branches_[i]);
    if ((i + 1) < width_)
      vec += "/";
  }
  return vec;
}

std::string
RDGVersion::ToVectorString() const {
  std::string vec = "";
  for (uint64_t i = 0; i < width_; i++) {
    vec += fmt::format("{}_{},", numbers_[i], branches_[i]);
  }
  // Last one has only the ver number.
  return fmt::format("{}{}", vec, numbers_[width_]);
}

uint64_t
RDGVersion::LeafVersionNumber() {
  return numbers_.back();
}

void
RDGVersion::SetNextVersion() {
  numbers_[width_]++;
}

void
RDGVersion::SetBranchPoint(const std::string& name) {
  branches_[width_] = name;
  // 1 to begin a branch
  numbers_.emplace_back(1);
  branches_.emplace_back("");
  width_++;
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
