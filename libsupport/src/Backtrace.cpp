#include "katana/Backtrace.h"

#include <backward.hpp>

#include <mutex>

KATANA_EXPORT void
katana::PrintBacktrace() {
  // Only have one thread print backtrace
  static std::once_flag printed_start;
  std::call_once(printed_start, [=]() {
    backward::StackTrace st;
    st.load_here(32);
    backward::Printer p;
    p.print(st);
  });
}
