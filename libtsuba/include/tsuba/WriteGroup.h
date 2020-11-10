#ifndef GALOIS_LIBTSUBA_TSUBA_WRITEGROUP_H_
#define GALOIS_LIBTSUBA_TSUBA_WRITEGROUP_H_

#include <future>
#include <list>
#include <memory>

#include "galois/Result.h"
#include "tsuba/FileFrame.h"
#include "tsuba/file.h"

namespace tsuba {

/// Track multiple, outstanding async writes and provide a mechanism to ensure
/// that they have all completed
class WriteGroup {
  struct AsyncOp {
    std::future<galois::Result<void>> result;
    std::string location;
  };

  std::string tag_;
  std::list<AsyncOp> pending_ops_;

  WriteGroup(std::string tag) : tag_(std::move(tag)){};

  /// Add future to the list of futures this descriptor will wait for, note
  /// the file name for debugging
  void AddOp(std::future<galois::Result<void>> future, std::string file);

public:
  /// Build a descriptor with a tag. If running with multiple hosts, Make should
  /// be Called BSP style and all hosts will have the same tag
  static galois::Result<std::unique_ptr<WriteGroup>> Make();

  /// Return a random tag that uniquely identifies this op
  const std::string& tag() const { return tag_; }

  /// Wait until all operations this descriptor knows about have completed
  galois::Result<void> Finish();

  /// Start async store op, we hold onto the data until op finishes
  void StartStore(std::shared_ptr<FileFrame> ff);

  /// Start async store op, caller responsible for keeping buffer live
  void StartStore(const std::string& file, const uint8_t* buf, uint64_t size) {
    AddOp(FileStoreAsync(file, buf, size), file);
  }
};

}  // namespace tsuba

#endif
