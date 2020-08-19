#ifndef GALOIS_LIBTSUBA_GLOBALSTATE_H_
#define GALOIS_LIBTSUBA_GLOBALSTATE_H_

#include <memory>
#include <vector>

#include "galois/CommBackend.h"
#include "galois/Logging.h"
#include "FileStorage.h"

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

  GlobalState(galois::CommBackend* comm) : comm_(comm){};
  FileStorage* GetDefaultFS() const;

public:
  GlobalState(const GlobalState& no_copy)  = delete;
  GlobalState(const GlobalState&& no_move) = delete;
  GlobalState& operator=(const GlobalState& no_copy) = delete;
  GlobalState& operator=(const GlobalState&& no_move) = delete;

  ~GlobalState() = default;

  galois::CommBackend* Comm() const;

  /// Get the correct FileStorage based on the URI
  ///
  /// store object is selected based on scheme:
  /// s3://...    -> S3Store
  /// abfs://...  -> AzureStore
  /// file://...  -> LocalStore
  /// {no scheme} -> LocalStore
  FileStorage* FS(std::string_view uri) const;

  static galois::Result<void> Init(galois::CommBackend* comm);
  static galois::Result<void> Fini();
  static const GlobalState& Get();
};

galois::CommBackend* Comm();
FileStorage* FS(std::string_view uri);

} // namespace tsuba

#endif
