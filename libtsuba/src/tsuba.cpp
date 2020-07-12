#include "tsuba/tsuba.h"

#include "galois/CommBackend.h"
#include "s3.h"
#include "GlobalState.h"

static galois::NullCommBackend default_backend;

galois::Result<void> tsuba::Init(galois::CommBackend* comm) {
  return GlobalState::Init(comm);
}

galois::Result<void> tsuba::Init() { return Init(&default_backend); }

galois::Result<void> tsuba::Fini() { return GlobalState::Fini(); }
