#include "katana/HostAllocator.h"

namespace katana {

HostAllocator::~HostAllocator() {}

SwappableHostAllocator::~SwappableHostAllocator() {}

const SwappableHostAllocator swappable_host_allocator;

}  // namespace katana
