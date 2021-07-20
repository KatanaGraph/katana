#pragma once

namespace katana {

//TODO (serge): change to a polymorphic allocator to switch between pinned and swappable memory
enum class MemoryPinType { Swappable = 0, Pinned = 1 };

#ifdef KATANA_ENABLE_GPU
static constexpr const MemoryPinType kUseMemoryPinType = MemoryPinType::Pinned;
#else
static constexpr const MemoryPinType kUseMemoryPinType =
    MemoryPinType::Swappable;
#endif  // KATANA_ENABLE_GPU

}  // namespace katana
