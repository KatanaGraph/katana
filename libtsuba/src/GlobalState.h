#ifndef KATANA_LIBTSUBA_GLOBALSTATE_H_
#define KATANA_LIBTSUBA_GLOBALSTATE_H_

#include <memory>
#include <vector>

#include "LocalStorage.h"
#include "katana/CommBackend.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/FileStorage.h"
#include "tsuba/NameServerClient.h"

namespace tsuba {

class GlobalState {
  static std::unique_ptr<GlobalState> ref_;
  static std::function<
      katana::Result<std::unique_ptr<tsuba::NameServerClient>>()>
      make_name_server_client_cb_;

  std::vector<FileStorage*> file_stores_;
  katana::CommBackend* comm_;
  tsuba::NameServerClient* name_server_client_;

  tsuba::LocalStorage local_storage_;

  GlobalState(katana::CommBackend* comm, tsuba::NameServerClient* ns)
      : comm_(comm), name_server_client_(ns) {
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
  NameServerClient* NS() const;

  /// Get the correct FileStorage based on the URI
  ///
  /// store object is selected based on scheme:
  /// s3://...    -> S3Store
  /// abfs://...  -> AzureStore
  /// gs://...    -> GSStore
  /// file://...  -> LocalStore
  /// {no scheme} -> LocalStore
  FileStorage* FS(std::string_view uri) const;

  static katana::Result<void> Init(
      katana::CommBackend* comm, tsuba::NameServerClient* ns);
  static katana::Result<void> Fini();
  static const GlobalState& Get();

  static void set_make_name_server_client_cb(
      std::function<katana::Result<std::unique_ptr<tsuba::NameServerClient>>()>
          cb) {
    make_name_server_client_cb_ = cb;
  }
  static katana::Result<std::unique_ptr<tsuba::NameServerClient>>
  MakeNameServerClient() {
    return make_name_server_client_cb_();
  }
};

katana::CommBackend* Comm();
FileStorage* FS(std::string_view uri);
NameServerClient* NS();

/// Execute cb on one host, if it succeeds return success if not print
/// the error and return MpiError
KATANA_EXPORT katana::Result<void> OneHostOnly(
    const std::function<katana::Result<void>()>& cb);

}  // namespace tsuba

#endif
