#include "katana/RDGVersion.h"

#include "katana/Logging.h"

namespace katana {

RDGVersion::RDGVersion(
    const std::vector<uint64_t>& vers, const std::vector<std::string>& ids)
    : numbers_(vers), branches_(ids) {}

RDGVersion::RDGVersion(uint64_t num) { numbers_.back() = num; }

RDGVersion::RDGVersion(const std::string& src) {

  KATANA_LOG_DEBUG_ASSERT(src.size() == 20);

  std::string str = src;
  std::vector<char> vec(str.begin(), str.end());
  char* source = vec.data();
  char* token;

  token = strtok(source, "_");

  if (token != NULL) {
    numbers_.clear();
    branches_.clear();
    do {
      uint64_t val = strtoul(token, nullptr, 10);
      if (val >= 5) {
        KATANA_LOG_DEBUG("in str {} found val {} with {}; ", 
            str, val, ToString());
      }
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
RDGVersion::ToString() const {
  std::string vec = "";
  if (numbers_.size() == 0) {
    return vec;
  }
  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    vec += fmt::format("{}_{}_", numbers_[i], branches_[i]);
  }
  // include only the number from the past pair, ignore "."
  return fmt::format("{}{}", vec, numbers_.back());
}

bool
RDGVersion::IsNull() {
  return (numbers_.size() == 0 || numbers_.back() == 0);
}

uint64_t
RDGVersion::LeafNumber() {
  return numbers_.back();
}

void
RDGVersion::IncrementNumber() {
  numbers_.back()++;
}

void
RDGVersion::AddBranch(const std::string& name) {
  branches_.back() = name;
  numbers_.emplace_back(1);
  branches_.emplace_back(".");
}

std::vector<uint64_t>&
RDGVersion::GetNumbers() {
  return numbers_;
}

std::vector<std::string>&
RDGVersion::GetBranches() {
  return branches_;
}

bool
RDGVersion::ShareBranch(const RDGVersion& in) {
  if (branches_.size() != in.branches_.size())
    return false;

  // TODO(wkyu): to get a correct string comparison 
#if 0
  for (uint32_t i = 0; i < branches_.size(); i++) {
    if (0!=(branches_[i].compare(in.branches_[i]))) {
      return false;
    }
  }
#else
  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    if (numbers_[i]!=in.numbers_[i]) {
      return false;
    }
  }
#endif
  return true;
}

bool
operator==(const RDGVersion& lhs, const RDGVersion& rhs) {
  // TODO(wkyu): add correct string comparison later
#if 0
  if (lhs.branches_.size() != rhs.branches_.size())
    return false;
  for (uint32_t i = 0; i < lhs.branches_.size(); i++) {
    if (0!=(lhs.branches_[i].compare(rhs.branches_[i]))) {
      return false;
    }
  }
#else
  if (lhs.numbers_ == rhs.numbers_) 
    return true;
  else
    return false;
#endif
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

// Check the example from URI.h on comparitors and formatter.

}  // namespace katana
