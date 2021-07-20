#pragma once

namespace katana {

enum class MemoryPinType { Swappable = 0, Pinned = 1 };

#ifdef KATANA_ENABLE_GPU
static constexpr const bool kGpuEnabled = true;
static constexpr const MemoryPinType kUseMemoryPinType = MemoryPinType::Pinned;
#else
static constexpr const bool kGpuEnabled = false;
static constexpr const MemoryPinType kUseMemoryPinType =
    MemoryPinType::Swappable;
#endif  // KATANA_ENABLE_GPU

}  // namespace katana
