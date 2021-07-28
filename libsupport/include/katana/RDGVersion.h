#ifndef KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_
#define KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_

#include <cstring>
#include <iterator>

struct KATANA_EXPORT RDGVersion {
  // A version consists of mulitple BranchPoints of width, each in the form of num:id
  // The last one has an empty branch "".
  std::vector<uint64_t> numbers_;
  std::vector<std::string> branches_;
  uint64_t width_{0};

  /*RDGVersion() = default;*/
  /*~RDGVersion() = default;*/
  RDGVersion(const RDGVersion & in) :
    numbers_(in.numbers_),
    branches_(in.branches_),
    width_(in.width_) {}

  RDGVersion(const std::vector<uint64_t> & vers, 
      const std::vector<std::string> & ids) :
   numbers_ (vers), 
   branches_(ids) { width_ = vers.size()-1; }

  RDGVersion(uint64_t num=0) {
    numbers_.emplace_back(num);
    branches_.emplace_back("");
    width_ = 0;
  }

  std::string
  ToVectorString() const {
    // Before the last version, for a pair ver_id,ver_id
    // the last version has no id.
    std::string vec = "";
    for (uint64_t i=0; i < width_; i ++) {
      vec += fmt::format("{}_{},", numbers_[i], branches_[i]);
    }
    return fmt::format("{}{}", vec, numbers_[width_]);
  }

  uint64_t 
  LeafVersionNumber() {
    return numbers_[width_];
  }

  void
  SetNextVersion() {
    numbers_[width_] ++;
  }

  void
  SetBranchPoint(const std::string& name) {
    branches_[width_] = name;
    // record the version for the branch
    numbers_.emplace_back(1);
    branches_.emplace_back("");
    width_ ++;
  }

  std::vector<uint64_t> &
  GetVersionNumbers() {
    return numbers_;
  }

  std::vector<std::string> &
  GetBranchIDs() {
    return branches_;
  }
};
#endif
