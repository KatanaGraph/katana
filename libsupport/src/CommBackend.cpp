#include "katana/CommBackend.h"

// Anchor vtables

katana::CommBackend::~CommBackend() = default;

void
katana::NullCommBackend::NotifyFailure() {}
