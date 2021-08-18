#include "katana/RDGVersion.h"

#include "katana/Logging.h"

namespace katana {

RDGVersion::RDGVersion(
    const std::vector<uint64_t>& vers, const std::vector<std::string>& ids)
    : numbers_(vers), branches_(ids) {}

RDGVersion::RDGVersion(uint64_t num) { numbers_.back() = num; }

RDGVersion::RDGVersion(const std::string& src) {
  KATANA_LOG_DEBUG_ASSERT(src.size() == 20);
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
      if (val >= 5) {
        KATANA_LOG_DEBUG(
            "from version ID {} found token {} with val {} in {}; ", src, token,
            val, ToString());
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
    vec += fmt::format("{:03d}_{}_", numbers_[i], branches_[i]);
  }
  // include only the number from the past pair, ignore "."
  return fmt::format("{}{:03d}", vec, numbers_.back());
}

bool
RDGVersion::IsNull() const {
  // No branch and no positive ID
  return (branches_.size() <= 1 && numbers_.back() == 0);
}

uint64_t
RDGVersion::LeafNumber() const {
  return numbers_.back();
}

bool
RDGVersion::ShareBranch(const RDGVersion& in) const {
  if (branches_.size() != in.branches_.size())
    return false;

  // only need to compare branch_
  for (uint32_t i = 0; i < branches_.size(); i++) {
    if (0 != strcmp(branches_[i].c_str(), in.branches_[i].c_str())) {
      return false;
    }
  }

#if 1
  if (std::equal(numbers_.begin(), numbers_.end() - 1, in.numbers_.begin()))
    return true;
  else
    return false;

  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    if (numbers_[i] != in.numbers_[i]) {
      return false;
    }
  }
#endif
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
