#include "katana/HostAllocator.h"

namespace katana {

HostHeap::~HostHeap() {}

SwappableHostHeap::~SwappableHostHeap() {}

HostHeap*
GetSwappableHostHeap() {
  static SwappableHostHeap swappable_host_heap;
  return &swappable_host_heap;
}

}  // namespace katana
