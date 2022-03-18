#ifndef KATANA_LIBTSUBA_KATANA_EPHEMERALSTORAGEPREFIX_H_
#define KATANA_LIBTSUBA_KATANA_EPHEMERALSTORAGEPREFIX_H_

#include <memory>

#include "katana/URI.h"
#include "katana/file.h"

namespace katana {

/// EphemeralStoragePrefix uses RAII to manage ephemeral storage locations. It
/// creates a URI under the katana temporary storage prefix and when the object
/// is destroyed it deletes all files under that prefix.
///
/// It does not allow construction with arbitrary prefixes under the assumption
/// that only prefixes under the katana temporary storage prefix would need to
/// be ephemeral.
///
/// NB: There are situations in which this destructor won't be called. The
/// fail-safe is a (not yet written) signal handler that will clear the entire
/// katana temporary storage prefix in as many cases as possible.
/// NB: it would be useful if this recursively deleted all files in all
/// subdirectories (a 'rm -rf'-like operation).
class EphemeralStoragePrefix {
public:
  ~EphemeralStoragePrefix() {
    std::vector<std::string> files;
    auto list_future = FileListAsync(prefix_.path(), &files);
    if (!list_future.valid()) {
      KATANA_LOG_WARN(
          "unable to list files, not cleaning up ephemeral storage");
      return;
    }

    auto list_future_res = list_future.get();
    if (!list_future_res) {
      KATANA_LOG_WARN(
          "unable to list files, not cleaning up ephemeral storage: {}",
          list_future_res.error());
    }

    std::unordered_set deletable_files(files.begin(), files.end());
    auto delete_res = FileDelete(prefix_.path(), deletable_files);
    if (!delete_res) {
      KATANA_LOG_WARN(
          "unable to delete files, not cleaning up ephemeral storage: {}",
          delete_res.error());
    }
  }

  static Result<std::unique_ptr<EphemeralStoragePrefix>> Make() {
    auto tmp_prefix = KATANA_CHECKED(URI::MakeTempDir());
    return std::unique_ptr<EphemeralStoragePrefix>(
        new EphemeralStoragePrefix(tmp_prefix.RandSubdir("ephemeral")));
  }

  // there should be a single object associated with any one ephemeral storage
  // prefix
  EphemeralStoragePrefix(EphemeralStoragePrefix& no_copy) = delete;
  EphemeralStoragePrefix& operator=(EphemeralStoragePrefix& no_copy) = delete;

  EphemeralStoragePrefix(EphemeralStoragePrefix&& other) noexcept = default;
  EphemeralStoragePrefix& operator=(EphemeralStoragePrefix&& other) noexcept =
      default;

  const URI& uri() { return prefix_; }

private:
  EphemeralStoragePrefix(URI prefix) : prefix_(std::move(prefix)) {}

  URI prefix_;
};

}  // namespace katana

#endif
