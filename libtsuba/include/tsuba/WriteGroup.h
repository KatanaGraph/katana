#ifndef KATANA_LIBTSUBA_TSUBA_WRITEGROUP_H_
#define KATANA_LIBTSUBA_TSUBA_WRITEGROUP_H_

#include <future>
#include <list>
#include <memory>

#include "katana/Result.h"
#include "tsuba/FileFrame.h"
#include "tsuba/file.h"

namespace tsuba {

/// Track multiple, outstanding async writes and provide a mechanism to ensure
/// that they have all completed
class WriteGroup {
  struct AsyncOp {
    std::future<katana::Result<void>> result;
    std::string location;
    uint64_t accounted_size;
  };

  std::string tag_;
  std::list<AsyncOp> pending_ops_;
  uint64_t outstanding_size_{0};
  uint64_t errors_{0};
  uint64_t total_{0};
  katana::Result<void> last_error_{katana::ResultSuccess()};

  WriteGroup(std::string tag) : tag_(std::move(tag)){};

  /// Add future to the list of futures this descriptor will wait for, note
  /// the file name for debugging. If the operation is associated with a file
  /// frame that we are responsible for, note the size
  void AddOp(
      std::future<katana::Result<void>> future, std::string file,
      uint64_t accounted_size = 0);

  /// Wait for the next op if there is one, account errors. Returns true if
  /// there was a next op
  bool Drain();

public:
  static constexpr uint64_t kMaxOutstandingSize = 10ULL << 30;  // 10 GB

  /// Build a descriptor with a tag. If running with multiple hosts, Make should
  /// be Called BSP style and all hosts will have the same tag
  static katana::Result<std::unique_ptr<WriteGroup>> Make();

  /// Return a random tag that uniquely identifies this op
  const std::string& tag() const { return tag_; }

  /// Wait until all operations this descriptor knows about have completed
  katana::Result<void> Finish();

  /// Start async store op, we hold onto the data until op finishes
  void StartStore(std::shared_ptr<FileFrame> ff);

  /// Start async store op, caller responsible for keeping buffer live
  void StartStore(const std::string& file, const uint8_t* buf, uint64_t size) {
    AddOp(FileStoreAsync(file, buf, size), file);
  }
};

}  // namespace tsuba

#endif
