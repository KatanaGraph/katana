#ifndef GALOIS_LIBTSUBA_GLOBALSTATE_H_
#define GALOIS_LIBTSUBA_GLOBALSTATE_H_

#include <memory>
#include <vector>

#include "FileStorage.h"
#include "galois/CommBackend.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/NameServerClient.h"

namespace tsuba {

class GlobalFileStorageAllocator {
  std::function<std::unique_ptr<FileStorage>()> gen_fun_;

public:
  GlobalFileStorageAllocator(
      std::function<std::unique_ptr<FileStorage>()> gen_fun)
      : gen_fun_(std::move(gen_fun)) {}

  std::unique_ptr<FileStorage> allocate() { return gen_fun_(); }
};

class GlobalState {
  static std::unique_ptr<GlobalState> ref_;

  std::vector<std::unique_ptr<FileStorage>> file_stores_;
  galois::CommBackend* comm_;
  tsuba::NameServerClient* name_server_client_;

  GlobalState(galois::CommBackend* comm, tsuba::NameServerClient* ns)
      : comm_(comm), name_server_client_(ns){};
  FileStorage* GetDefaultFS() const;

public:
  GlobalState(const GlobalState& no_copy) = delete;
  GlobalState(const GlobalState&& no_move) = delete;
  GlobalState& operator=(const GlobalState& no_copy) = delete;
  GlobalState& operator=(const GlobalState&& no_move) = delete;

  ~GlobalState() = default;

  galois::CommBackend* Comm() const;
  NameServerClient* NS() const;

  /// Get the correct FileStorage based on the URI
  ///
  /// store object is selected based on scheme:
  /// s3://...    -> S3Store
  /// abfs://...  -> AzureStore
  /// file://...  -> LocalStore
  /// {no scheme} -> LocalStore
  FileStorage* FS(std::string_view uri) const;

  static galois::Result<void> Init(
      galois::CommBackend* comm, tsuba::NameServerClient* ns);
  static galois::Result<void> Fini();
  static const GlobalState& Get();
};

galois::CommBackend* Comm();
FileStorage* FS(std::string_view uri);
NameServerClient* NS();

/// Execute cb on one host, if it succeeds return success if not print
/// the error and return MpiError
galois::Result<void> OneHostOnly(
    const std::function<galois::Result<void>()>& cb);

}  // namespace tsuba

#endif
