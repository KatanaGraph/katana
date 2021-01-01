#ifndef GALOIS_LIBSUPPORT_GALOIS_BACKTRACE_H_
#define GALOIS_LIBSUPPORT_GALOIS_BACKTRACE_H_

#include <stdint.h>

#include "galois/config.h"

namespace galois {
// Passing an ID allows us to limit backtraces to a single process
GALOIS_EXPORT void InitBacktrace(uint32_t ID = 0);
GALOIS_EXPORT void PrintBacktrace();
}  // end namespace galois

#endif
