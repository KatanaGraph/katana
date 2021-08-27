#include "katana/RDGVersion.h"

#include "katana/Logging.h"

namespace katana {

RDGVersion::RDGVersion(
    const std::vector<uint64_t>& vers, const std::vector<std::string>& ids)
    : numbers_(vers), branches_(ids) {}

RDGVersion::RDGVersion(uint64_t num) { numbers_.back() = num; }

RDGVersion::RDGVersion(const std::string& src) {
  char dest[kRDGVersionIDLength + 1];
  char* token;

  strncpy(dest, src.c_str(), kRDGVersionIDLength);
  dest[kRDGVersionIDLength] = '\0';
  token = strtok(dest, "_");

  if (token != NULL) {
    numbers_.clear();
    branches_.clear();
    do {
      uint64_t val = strtoul(token, nullptr, 10);
      numbers_.emplace_back(val);
      token = strtok(NULL, "_");
      if (token != NULL) {
        branches_.emplace_back(token);
        token = strtok(NULL, "_");
      } else {
        branches_.emplace_back(".");
        break;
      }
    } while (token != NULL);
  }
}

std::string
RDGVersion::ToPathName() const {
  std::string vec = fmt::format("{0:0{1}d}",
      0, kRDGVersionPaddingLength);
  if (numbers_.size() == 0) {
    return vec;
  }
  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    vec += fmt::format("{0:0{1}d}",
        numbers_[i], kRDGVersionPaddingLength);
    vec += fmt::format("_{}_", branches_[i]);
  }
  // include only the number from the last pair, ignore "."
  vec += fmt::format("{0:0{1}d}",
      numbers_.back(), kRDGVersionPaddingLength);
  return vec;
}

std::string
RDGVersion::ToString() const {
  return fmt::format("{}_{}", ToPathName(), branches_.back());
}

bool
RDGVersion::NullNumber() const {
  // No branch and no positive ID
  return (numbers_.empty() || numbers_.back() == 0);
}

bool
RDGVersion::NullBranch() const {
  // No branch and no positive ID
  return (branches_.empty() || branches_.back() == ".");
}

bool
RDGVersion::IsNull() const {
  // No branch and no positive ID
  return (branches_.size() <= 1 && (numbers_.empty() || numbers_.back() == 0));
}

uint64_t
RDGVersion::LeafNumber() const {
  return numbers_.back();
}

bool
RDGVersion::ShareBranch(const RDGVersion& in) const {
  KATANA_LOG_DEBUG_ASSERT(
      (branches_.size() == numbers_.size()) &&
      (in.branches_.size() == in.numbers_.size()));
  if (branches_.size() != in.branches_.size()) {
    return false;
  }

  for (uint32_t i = 0; i < branches_.size(); i++) {
    // take "" as equivalent to "."
    if ((branches_[i] == "." || branches_[i] == "") &&
        (in.branches_[i] == "." || in.branches_[i] == "")) {
      continue;
    } else if (branches_[i] != in.branches_[i]) {
      return false;
    }
  }

  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    if (numbers_[i] != in.numbers_[i]) {
      return false;
    }
  }

  return true;
}

void
RDGVersion::IncrementLeaf(uint64_t num) {
  numbers_.back() += num;
}

void
RDGVersion::SetLeafNumber(uint64_t num) {
  numbers_.back() = num;
}

void
RDGVersion::AddBranch(const std::string& name) {
  branches_.back() = name;
  numbers_.emplace_back(0);
  branches_.emplace_back(".");
}

bool
operator==(const RDGVersion& lhs, const RDGVersion& rhs) {
  RDGVersion tmp = lhs;
  if (!tmp.ShareBranch(rhs))
    return false;

  if (lhs.numbers_ == rhs.numbers_)
    return true;
  else
    return false;
}

bool
operator!=(const RDGVersion& lhs, const RDGVersion& rhs) {
  return (!(lhs == rhs));
}

bool
operator>(const RDGVersion& lhs, const RDGVersion& rhs) {
  uint32_t min = std::min(lhs.numbers_.size(), rhs.numbers_.size());

  for (uint32_t i = 0; i < min; i++) {
    if (lhs.numbers_[i] < rhs.numbers_[i]) {
      return false;
    }
    if (lhs.numbers_[i] > rhs.numbers_[i]) {
      return true;
    }
  }

  if (lhs.numbers_.size() <= rhs.numbers_.size())
    return false;

  return true;
}

bool
operator<(const RDGVersion& lhs, const RDGVersion& rhs) {
  uint32_t min = std::min(lhs.numbers_.size(), rhs.numbers_.size());

  for (uint32_t i = 0; i < min; i++) {
    if (lhs.numbers_[i] > rhs.numbers_[i]) {
      return false;
    }
    if (lhs.numbers_[i] < rhs.numbers_[i]) {
      return true;
    }
  }

  if (lhs.numbers_.size() >= rhs.numbers_.size()) {
    return false;
  } else {
    return true;
  }
}

}  // namespace katana
