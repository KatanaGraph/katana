#include "tsuba/tsuba.h"

#include "GlobalState.h"
#include "galois/CommBackend.h"
#include "tsuba/MemoryNameServerClient.h"
#include "tsuba/NameServerClient.h"

static galois::NullCommBackend default_comm_backend;
static tsuba::MemoryNameServerClient default_ns_client;

galois::Result<void>
tsuba::Init(galois::CommBackend* comm, tsuba::NameServerClient* ns) {
  return GlobalState::Init(comm, ns);
}

galois::Result<void>
tsuba::Init() {
  return Init(&default_comm_backend, &default_ns_client);
}

galois::Result<void>
tsuba::Fini() {
  return GlobalState::Fini();
}
