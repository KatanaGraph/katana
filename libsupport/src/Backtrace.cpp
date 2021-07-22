// Backtrace information.  backward-cpp prints an informative backtrace on abrupt
// termination, and provides a program interface.

#include "katana/Backtrace.h"

#include <backward.hpp>

#include <csignal>
#include <cstdio>

#include "katana/Logging.h"

namespace {
// Install signal handlers
backward::SignalHandling sh;

void
PrintError(int) {
  std::fputs("caught SIGPIPE\n", stderr);
}

}  // namespace

KATANA_EXPORT void
katana::PrintBacktrace() {
  // Only have one thread print backtrace
  static std::once_flag printed_start;
  std::call_once(printed_start, [=]() {
    using namespace backward;
    StackTrace st;
    st.load_here(32);
    Printer p;
    p.print(st);
  });
}

KATANA_EXPORT void
katana::InitBacktrace() {
  std::signal(SIGPIPE, PrintError);
}
