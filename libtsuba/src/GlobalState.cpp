#include "GlobalState.h"

#include <algorithm>
#include <cassert>

#include "FileStorage_internal.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/NameServerClient.h"

std::unique_ptr<tsuba::GlobalState> tsuba::GlobalState::ref_ = nullptr;

galois::CommBackend*
tsuba::GlobalState::Comm() const {
  assert(comm_ != nullptr);
  return comm_;
}

tsuba::FileStorage*
tsuba::GlobalState::GetDefaultFS() const {
  assert(file_stores_.size() > 0);
  return file_stores_[0];
}

tsuba::FileStorage*
tsuba::GlobalState::FS(std::string_view uri) const {
  for (FileStorage* fs : file_stores_) {
    if (uri.find(fs->uri_scheme()) == 0) {
      return fs;
    }
  }
  return GetDefaultFS();
}

tsuba::NameServerClient*
tsuba::GlobalState::NS() const {
  return name_server_client_;
}

galois::Result<void>
tsuba::GlobalState::Init(
    galois::CommBackend* comm, tsuba::NameServerClient* ns) {
  assert(ref_ == nullptr);

  // quick ping to say hello and fail fast if something was misconfigured
  if (auto res = ns->CheckHealth(); !res) {
    return res.error();
  }

  // new to access non-public constructor
  std::unique_ptr<GlobalState> global_state(new GlobalState(comm, ns));

  std::vector<FileStorage*>& registered = GetRegisteredFileStorages();
  for (FileStorage* fs : registered) {
    global_state->file_stores_.emplace_back(fs);
  }
  registered.clear();

  std::sort(
      global_state->file_stores_.begin(), global_state->file_stores_.end(),
      [](const FileStorage* lhs, const FileStorage* rhs) {
        return lhs->Priority() > rhs->Priority();
      });

  for (FileStorage* fs : global_state->file_stores_) {
    if (auto res = fs->Init(); !res) {
      return res.error();
    }
  }

  ref_ = std::move(global_state);
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::GlobalState::Fini() {
  for (FileStorage* fs : ref_->file_stores_) {
    if (auto res = fs->Fini(); !res) {
      return res.error();
    }
  }
  ref_.reset(nullptr);
  return galois::ResultSuccess();
}

const tsuba::GlobalState&
tsuba::GlobalState::Get() {
  assert(ref_ != nullptr);
  return *ref_;
}

galois::CommBackend*
tsuba::Comm() {
  return GlobalState::Get().Comm();
}

tsuba::FileStorage*
tsuba::FS(std::string_view uri) {
  return GlobalState::Get().FS(uri);
}

tsuba::NameServerClient*
tsuba::NS() {
  return GlobalState::Get().NS();
}

galois::Result<void>
tsuba::OneHostOnly(const std::function<galois::Result<void>()>& cb) {
  bool failed = false;
  if (Comm()->ID == 0) {
    auto res = cb();
    if (!res) {
      GALOIS_LOG_ERROR("OneHostOnly operation failed: {}", res.error());
      failed = true;
    }
  }
  if (Comm()->Broadcast(0, failed)) {
    return ErrorCode::MpiError;
  }
  return galois::ResultSuccess();
}
