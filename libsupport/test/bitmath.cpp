#include "katana/BitMath.h"

#include "katana/Logging.h"

int
main() {
  KATANA_LOG_ASSERT(katana::IsPowerOf2(1));
  KATANA_LOG_ASSERT(katana::IsPowerOf2(2));
  KATANA_LOG_ASSERT(katana::IsPowerOf2(4));
  KATANA_LOG_ASSERT(katana::IsPowerOf2(1024));
  KATANA_LOG_ASSERT(katana::IsPowerOf2(1048576));
  KATANA_LOG_ASSERT(!katana::IsPowerOf2(0));
  KATANA_LOG_ASSERT(!katana::IsPowerOf2(7));
  KATANA_LOG_ASSERT(!katana::IsPowerOf2(1048577));

  KATANA_LOG_ASSERT(katana::AlignUp<uint64_t>(7) == 8);
  KATANA_LOG_ASSERT(katana::AlignDown<uint64_t>(7) == 0);
  KATANA_LOG_ASSERT(katana::AlignUp<uint32_t>(7) == 8);
  KATANA_LOG_ASSERT(katana::AlignDown<uint32_t>(7) == 4);
  KATANA_LOG_ASSERT(katana::AlignDown<uint64_t>(1048577) == 1048576);

  struct StrangeSize {
    uint8_t buf[37];
  };

  KATANA_LOG_ASSERT(katana::AlignDown<StrangeSize>(1) == 0);
  KATANA_LOG_ASSERT(katana::AlignUp<StrangeSize>(1) == 37);
  KATANA_LOG_ASSERT(katana::AlignUp<StrangeSize>(1024) == 1036);
  KATANA_LOG_ASSERT(katana::AlignDown<StrangeSize>(1024) == 999);
}
