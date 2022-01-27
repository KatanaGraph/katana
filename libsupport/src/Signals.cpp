#include "katana/Signals.h"

#include <backward.hpp>

#include <array>
#include <atomic>
#include <csignal>

#include "katana/Backtrace.h"
#include "katana/Env.h"
#include "katana/Logging.h"

namespace {

void
LogAndRaise(int signo, siginfo_t* info, void* ctx) {
  backward::SignalHandling::handleSignal(signo, info, ctx);
  // try to forward the signal.
  std::raise(info->si_signo);

  std::fputs("FATAL: unreachable after raise\n", stderr);
  _exit(EXIT_FAILURE);
}

std::atomic<bool> ignore_once;

void
Ignore(int signo, siginfo_t* info, void* ctx) {
  bool value{};
  if (ignore_once.compare_exchange_strong(value, true)) {
    backward::SignalHandling::handleSignal(signo, info, ctx);
  }
  std::fputs("WARNING: ignoring SIGPIPE\n", stderr);
}

/// SignalHandling installs signal handlers for conventionally terminal signals
/// like SIGSEGV, SIGABRT, etc. that prints a backtrace before terminating the
/// process.
///
/// It also installs a signal handler for SIGPIPE that prints a backtrace once
/// but otherwise ignores SIGPIPE. This is useful for SIGPIPE in particular
/// because the default handler kills the process, but it is usually okay to
/// ignore it.
class SignalHandling {
public:
  SignalHandling() {
    bool loaded = true;

    stack_t ss;
    ss.ss_sp = stack_.begin();
    ss.ss_size = kStackSize;
    ss.ss_flags = 0;
    if (sigaltstack(&ss, nullptr) < 0) {
      loaded = false;
    }

    if (!Install(
            backward::SignalHandling::make_default_signals(), LogAndRaise,
            static_cast<int>(
                SA_SIGINFO | SA_ONSTACK | SA_NODEFER | SA_RESETHAND))) {
      loaded = false;
    }

    bool verbose_sigpipe_handler = false;
    katana::GetEnv("KATANA_VERBOSE_SIGPIPE_HANDLER", &verbose_sigpipe_handler);

    if (verbose_sigpipe_handler) {
      if (!Install(
              {SIGPIPE}, Ignore,
              static_cast<int>(SA_SIGINFO | SA_ONSTACK | SA_NODEFER))) {
        loaded = false;
      }
    } else {
      if (!Mask({SIGPIPE}, static_cast<int>(SA_ONSTACK | SA_NODEFER))) {
        loaded = false;
      }
    }

    loaded_ = loaded;
  }

  bool loaded() const { return loaded_; }

private:
  typedef void (*Handler)(int, siginfo_t*, void*);

  bool Install(const std::vector<int>& signals, Handler handler, int flags) {
    bool loaded = true;

    for (const int& sig : signals) {
      struct sigaction action;
      memset(&action, 0, sizeof action);
      action.sa_flags = flags;
      sigfillset(&action.sa_mask);
      sigdelset(&action.sa_mask, sig);
      action.sa_sigaction = handler;

      int r = sigaction(sig, &action, nullptr);
      if (r != 0) {
        loaded = false;
      }
    }

    return loaded;
  }

  bool Mask(const std::vector<int>& signals, int flags) {
    bool loaded = true;

    for (const int& sig : signals) {
      struct sigaction action;
      memset(&action, 0, sizeof action);
      action.sa_flags = flags;
      sigfillset(&action.sa_mask);
      sigdelset(&action.sa_mask, sig);
      action.sa_handler = SIG_IGN;

      int r = sigaction(sig, &action, nullptr);
      if (r != 0) {
        loaded = false;
      }
    }

    return loaded;
  }

  // SIGSTKSZ is 8k. backward::SignalHandling uses 8 MB.
  constexpr static size_t kStackSize = 1024 * 1024 * 8;

  std::array<char, kStackSize> stack_;
  bool loaded_{};
};

SignalHandling signal_handling;

}  // namespace

KATANA_EXPORT void
katana::InitSignalHandlers() {
  if (!signal_handling.loaded()) {
    KATANA_LOG_WARN("signal handlers not loaded");
  }
}
