#ifndef KATANA_LIBTSUBA_GLOBALSTATE_H_
#define KATANA_LIBTSUBA_GLOBALSTATE_H_

#include <memory>
#include <vector>

#include "LocalStorage.h"
#include "katana/CommBackend.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/FileStorage.h"

namespace tsuba {

class GlobalState {
  static std::unique_ptr<GlobalState> ref_;

  std::vector<FileStorage*> file_stores_;
  katana::CommBackend* comm_;

  tsuba::LocalStorage local_storage_;

  GlobalState(katana::CommBackend* comm) : comm_(comm) {
    file_stores_.emplace_back(&local_storage_);
  }

  FileStorage* GetDefaultFS() const;

public:
  GlobalState(const GlobalState& no_copy) = delete;
  GlobalState(const GlobalState&& no_move) = delete;
  GlobalState& operator=(const GlobalState& no_copy) = delete;
  GlobalState& operator=(const GlobalState&& no_move) = delete;

  ~GlobalState() = default;

  katana::CommBackend* Comm() const;

  /// Get the correct FileStorage based on the URI
  ///
  /// store object is selected based on scheme:
  /// s3://...    -> S3Store
  /// abfs://...  -> AzureStore
  /// gs://...    -> GSStore
  /// file://...  -> LocalStore
  /// {no scheme} -> LocalStore
  FileStorage* FS(std::string_view uri) const;

  static katana::Result<void> Init(katana::CommBackend* comm);
  static katana::Result<void> Fini();
  static const GlobalState& Get();
};

KATANA_EXPORT katana::CommBackend* Comm();
FileStorage* FS(std::string_view uri);

/// Execute cb on one host, if it succeeds return success if not print
/// the error and return MpiError
KATANA_EXPORT katana::Result<void> OneHostOnly(
    const std::function<katana::Result<void>()>& cb);

}  // namespace tsuba

#endif
