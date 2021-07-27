#ifndef KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_
#define KATANA_LIBSUPPORT_KATANA_RDGVERSION_H_

#include <cstring>
#include <iterator>

struct KATANA_EXPORT RDGVersion {
  // A version consists of mulitple BranchPoints, each in the form of num:id
  // The last one has an empty branch "".
  std::vector<uint64_t> ver_numbers_;
  std::vector<std::string> branch_ids_;
  uint64_t branches_{0};

  /*RDGVersion() = default;*/
  /*~RDGVersion() = default;*/
  RDGVersion(const RDGVersion & in) :
    ver_numbers_(in.ver_numbers_),
    branch_ids_(in.branch_ids_),
    branches_(in.branches_) {}

  RDGVersion(const std::vector<uint64_t> & vers, 
      const std::vector<std::string> & ids) :
   ver_numbers_ (vers), 
   branch_ids_(ids) { branches_ = vers.size()-1; }

  RDGVersion(uint64_t num=0) {
    ver_numbers_.emplace_back(num);
    branch_ids_.emplace_back("");
    branches_ = 0;
  }

  std::string
  ToVectorString() const {
    // Before the last version, for a pair ver_id,ver_id
    // the last version has no id.
    std::string vec = "";
    for (uint64_t i=0; i < branches_; i ++) {
      vec += fmt::format("{}_{},", ver_numbers_[i], branch_ids_[i]);
    }
    return fmt::format("{}{}", vec, ver_numbers_[branches_]);
  }

  uint64_t 
  LeafVersionNumber() {
    return ver_numbers_[branches_];
  }

  void
  SetNextVersion() {
    ver_numbers_[branches_] ++;
  }

  void
  SetBranchPoint(const std::string& branch_name) {
    branch_ids_[branches_] = branch_name;
    // record the version for the branch
    ver_numbers_.emplace_back(1);
    branch_ids_.emplace_back("");
    branches_ ++;
  }

  std::vector<uint64_t> &
  GetBranchNumbers() {
    return ver_numbers_;
  }

  std::vector<std::string> &
  GetBranchIDs() {
    return branch_ids_;
  }
};
#endif
