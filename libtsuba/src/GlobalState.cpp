#include "GlobalState.h"

#include <algorithm>
#include <cassert>

#include "galois/Result.h"
#include "LocalStorage.h"
#include "S3Storage.h"

namespace tsuba {

std::unique_ptr<tsuba::GlobalState> GlobalState::ref = nullptr;

galois::CommBackend* GlobalState::Comm() const {
  assert(comm_ != nullptr);
  return comm_;
}

FileStorage* GlobalState::GetDefaultFS() const {
  assert(file_stores_.size() > 0);
  return file_stores_[0].get();
}

tsuba::FileStorage* GlobalState::FS(std::string_view uri) const {
  for (auto& fs : file_stores_) {
    if (uri.find(fs->uri_scheme()) == 0) {
      return fs.get();
    }
  }
  return GetDefaultFS();
}

galois::Result<void> GlobalState::Init(galois::CommBackend* comm) {
  assert(ref == nullptr);
  // new to access non-public constructor
  std::unique_ptr<GlobalState> global_state(new GlobalState(comm));

  std::unique_ptr<LocalStorage> local_storage(new LocalStorage());
  if (auto res = local_storage->Init(); !res) {
    return res.error();
  }
  std::unique_ptr<S3Storage> s3_storage(new S3Storage());
  if (auto res = s3_storage->Init(); !res) {
    return res.error();
  }

  // The first is returned for URI without schema
  global_state->file_stores_.emplace_back(std::move(local_storage));
  global_state->file_stores_.emplace_back(std::move(s3_storage));

  ref = std::move(global_state);
  return galois::ResultSuccess();
}

galois::Result<void> GlobalState::Fini() {
  for (auto& fs : ref->file_stores_) {
    if (auto res = fs->Fini(); !res) {
      return res.error();
    }
  }
  ref.reset(nullptr);
  return galois::ResultSuccess();
}

const tsuba::GlobalState& GlobalState::Get() {
  assert(ref != nullptr);
  return *ref;
}

} // namespace tsuba

galois::CommBackend* tsuba::Comm() { return GlobalState::Get().Comm(); }

tsuba::FileStorage* tsuba::FS(std::string_view uri) {
  return GlobalState::Get().FS(uri);
}
