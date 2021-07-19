#pragma once

#ifdef KATANA_ENABLE_GPU
#define KATANA_GPU_ENABLED true
#define KATANA_MEMORY_PIN_TYPE (katana::MemoryPinType::Pinned)
#else
#define KATANA_GPU_ENABLED false
#define KATANA_MEMORY_PIN_TYPE (katana::MemoryPinType::Usual)
#endif  // KATANA_ENABLE_GPU
