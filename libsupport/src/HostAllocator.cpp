#include "katana/HostAllocator.h"

namespace katana {

HostHeap::~HostHeap() {}

SwappableHostHeap::~SwappableHostHeap() {}

SwappableHostHeap swappable_host_heap;

SwappableHostHeap*
GetSwappableHostHeap() {
  return &swappable_host_heap;
}

}  // namespace katana
