#include "GlobalState.h"

#include <algorithm>
#include <cassert>

#include "FileStorage_internal.h"
#include "MemoryNameServerClient.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"

namespace {

katana::Result<std::unique_ptr<tsuba::NameServerClient>>
GetMemoryClient() {
  return std::make_unique<tsuba::MemoryNameServerClient>();
}

}  // namespace

std::unique_ptr<tsuba::GlobalState> tsuba::GlobalState::ref_ = nullptr;

std::function<katana::Result<std::unique_ptr<tsuba::NameServerClient>>()>
    tsuba::GlobalState::make_name_server_client_cb_ = GetMemoryClient;

katana::CommBackend*
tsuba::GlobalState::Comm() const {
  KATANA_LOG_DEBUG_ASSERT(comm_ != nullptr);
  return comm_;
}

tsuba::FileStorage*
tsuba::GlobalState::GetDefaultFS() const {
  KATANA_LOG_DEBUG_ASSERT(file_stores_.size() > 0);
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

katana::Result<void>
tsuba::GlobalState::Init(
    katana::CommBackend* comm, tsuba::NameServerClient* ns) {
  KATANA_LOG_DEBUG_ASSERT(ref_ == nullptr);

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
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::GlobalState::Fini() {
  for (FileStorage* fs : ref_->file_stores_) {
    if (auto res = fs->Fini(); !res) {
      return res.error();
    }
  }
  ref_.reset(nullptr);
  return katana::ResultSuccess();
}

const tsuba::GlobalState&
tsuba::GlobalState::Get() {
  KATANA_LOG_DEBUG_ASSERT(ref_ != nullptr);
  return *ref_;
}

katana::CommBackend*
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

katana::Result<void>
tsuba::OneHostOnly(const std::function<katana::Result<void>()>& cb) {
  // Prevent a race when the callback affects a condition guarding the
  // execution of OneHostOnly
  Comm()->Barrier();

  katana::Result<void> res = katana::ResultSuccess();

  bool failed = false;
  if (Comm()->ID == 0) {
    res = cb();
    if (!res) {
      failed = true;
    }
  }

  if (Comm()->Broadcast(0, failed)) {
    if (!res) {
      return res.error().WithContext(
          ErrorCode::MpiError, "failure in single host execution");
    }
    return ErrorCode::MpiError;
  }

  return katana::ResultSuccess();
}
