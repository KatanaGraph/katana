#include "katana/Signals.h"

#include <csignal>

int
main() {
  katana::InitSignalHandlers();

  for (int i = 0; i < 5; ++i) {
    std::raise(SIGPIPE);
  }

  return 0;
}
