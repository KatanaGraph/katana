#ifndef KATANA_LIBSUPPORT_KATANA_BACKTRACE_H_
#define KATANA_LIBSUPPORT_KATANA_BACKTRACE_H_

#include <stdint.h>

#include "katana/config.h"

namespace katana {
// Passing an ID allows us to limit backtraces to a single process
KATANA_EXPORT void InitBacktrace(uint32_t ID = 0);
KATANA_EXPORT void PrintBacktrace();
}  // end namespace katana

#endif
