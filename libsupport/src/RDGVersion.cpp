#include "katana/RDGVersion.h"

#include "katana/Logging.h"

namespace katana {

RDGVersion::RDGVersion(
    const std::vector<uint64_t>& vers, const std::vector<std::string>& ids)
    : numbers_(vers), branches_(ids) {}

RDGVersion::RDGVersion(uint64_t num) { numbers_.back() = num; }

RDGVersion::RDGVersion(const std::string str) { 
  std::vector<char> vec(str.begin(), str.end());
  char* source = vec.data();
  char* token;

  token = strtok(source, "_");
  if (token != NULL) {
    numbers_.clear();
    branches_.clear();
    do {
      numbers_.emplace_back(strtoul(token, nullptr, 10));
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
  for (uint32_t i = 0; (i + 1) < numbers_.size(); i++) {
    vec += fmt::format("{}_{},", numbers_[i], branches_[i]);
  }
  // include only the number from the past pair, ignore "."
  return fmt::format("{}{}", vec, numbers_.back());
}

bool
RDGVersion::IsNull() {
  return (numbers_.back()==0);
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
RDGVersion::ShareBranch(const RDGVersion & in) {
  return branches_ == in.branches_;
}

bool
operator==(const RDGVersion& lhs, const RDGVersion& rhs) {
  return (lhs.numbers_ == rhs.numbers_ && lhs.branches_ == rhs.branches_);
}

bool
operator!=(const RDGVersion& lhs, const RDGVersion& rhs) {
  return (! (lhs == rhs));
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
