#include "tsuba/tsuba.h"

#include "galois/CommBackend.h"
#include "s3.h"
#include "tsuba_internal.h"

std::unique_ptr<tsuba::GlobalState> tsuba::GlobalState::ref = nullptr;

static galois::NullCommBackend default_backend;

galois::Result<void> tsuba::Init(galois::CommBackend* comm) {
  GlobalState::Init(comm);
  return S3Init();
}

galois::Result<void> tsuba::Init() { return Init(&default_backend); }

galois::Result<void> tsuba::Fini() {
  auto s3_fini_ret = S3Fini();
  GlobalState::Fini();
  return s3_fini_ret;
}
