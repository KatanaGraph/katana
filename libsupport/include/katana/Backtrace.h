#ifndef KATANA_LIBSUPPORT_KATANA_BACKTRACE_H_
#define KATANA_LIBSUPPORT_KATANA_BACKTRACE_H_

#include "katana/config.h"

namespace katana {
// Programmatic interface to print a backtrace
KATANA_EXPORT void PrintBacktrace();
KATANA_EXPORT void InitBacktrace();
}  // end namespace katana

#endif
