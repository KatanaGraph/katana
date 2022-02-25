#include "katana/ProgressTracer.h"

#include <sys/resource.h>
#include <unistd.h>

#include <fstream>
#include <regex>

#include "katana/Logging.h"
#include "katana/config.h"

#if __linux__
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef KATANA_USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

std::unique_ptr<katana::ProgressTracer> katana::ProgressTracer::tracer_ =
    nullptr;

katana::ProgressTracer&
katana::GetTracer() {
  return katana::ProgressTracer::Get();
}

#if __linux__

uint64_t
katana::ProgressTracer::ParseProcSelfRssBytes() {
  static std::regex kRssRegex("^Rss\\w+:\\s+([0-9]+) kB");
  std::ifstream proc_self("/proc/self/status");

  // there are 3 relevant vals: RssAnon, RssFile, RssShmem
  uint32_t rss_vals = 3;
  uint64_t total_mem = 0;
  std::string line;
  while (std::getline(proc_self, line) && rss_vals > 0) {
    std::smatch sub_match;
    if (!std::regex_match(line, sub_match, kRssRegex)) {
      continue;
    }
    std::string val = sub_match[1];
    total_mem += std::strtol(val.c_str(), nullptr, 0);
    rss_vals -= 1;
  }
  if (rss_vals != 0) {
    KATANA_LOG_ERROR("parsing /proc/self/status for memory failed");
  }
  return total_mem * 1024;
}

katana::HostStats
katana::ProgressTracer::GetHostStats() {
  HostStats stats;

  struct sysinfo info;
  sysinfo(&info);

  constexpr int kLen = 256;

  std::array<char, kLen> hostname;
  gethostname(hostname.begin(), kLen);

  stats.ram_gb = info.totalram / (1024 * 1024 * 1024);
  stats.nprocs = get_nprocs();
  stats.hostname = hostname.begin();

  stats.pid = getpid();

  return stats;
}

#else

uint64_t
katana::ProgressTracer::ParseProcSelfRssBytes() {
  KATANA_WARN_ONCE(
      "calculating resident set size is not implemented for this platform");
  return 0;
}

HostStats
katana::ProgressTracer::GetHostStats() {
  KATANA_WARN_ONCE("getting host stats is not implemented for this platform");
  HostStats stats;
  return stats;
}

#endif

long
katana::ProgressTracer::GetMaxMem() {
  struct rusage rusage;
  getrusage(RUSAGE_SELF, &rusage);
  return rusage.ru_maxrss;
}

uint64_t
katana::ProgressTracer::GetMaxMemBytes() {
  struct rusage rusage;
  getrusage(RUSAGE_SELF, &rusage);
  return rusage.ru_maxrss * static_cast<uint64_t>(1024);
}

std::string
katana::ProgressTracer::GetValue(const katana::Value& value) {
  if (std::holds_alternative<std::string>(value)) {
    return "\"" + std::get<std::string>(value) + "\"";
  } else if (std::holds_alternative<int64_t>(value)) {
    return std::to_string(std::get<int64_t>(value));
  } else if (std::holds_alternative<double>(value)) {
    return std::to_string(std::get<double>(value));
  } else if (std::holds_alternative<bool>(value)) {
    return std::get<bool>(value) ? "true" : "false";
  } else if (std::holds_alternative<uint64_t>(value)) {
    return std::to_string(std::get<uint64_t>(value));
  }
  return std::string{};
}

void
katana::ProgressTracer::Set(std::unique_ptr<katana::ProgressTracer> tracer) {
  ProgressTracer::tracer_ = std::move(tracer);
}

katana::ProgressScope
katana::ProgressTracer::StartActiveSpan(const std::string& span_name) {
  return SetActiveSpan(StartSpan(span_name, active_span_));
}
katana::ProgressScope
katana::ProgressTracer::StartActiveSpan(
    const std::string& span_name, const katana::ProgressContext& child_of) {
  return SetActiveSpan(StartSpan(span_name, child_of));
}

void
katana::ProgressTracer::FinishActiveSpan() {
  if (active_span_ != nullptr) {
    auto old_active_span = active_span_;
    active_span_ = old_active_span->GetParentSpan();
    if (!old_active_span->IsFinished()) {
      old_active_span->Finish();
    }
    if (active_span_ != nullptr &&
        (active_span_->ScopeClosed() || active_span_->IsFinished())) {
      // if the active span is already finished
      // then this only sets the active span to its parent
      active_span_->Finish();
    }
  }
}

katana::ProgressSpan&
katana::ProgressTracer::GetActiveSpan() {
  if (active_span_ == nullptr) {
    if (default_active_span_ == nullptr) {
      default_active_span_ = StartSpan("unnamed span", nullptr);
    }
    return *default_active_span_;
  }
  return *active_span_;
}

katana::ProgressScope
katana::ProgressTracer::SetActiveSpan(
    std::shared_ptr<katana::ProgressSpan> span) {
  active_span_ = span;
  return ProgressScope(std::move(span));
}

void
katana::ProgressTracer::Finish() {
  while (HasActiveSpan()) {
    FinishActiveSpan();
  }
  if (default_active_span_ != nullptr) {
    default_active_span_->Finish();
  }

  active_span_ = nullptr;
  default_active_span_ = nullptr;

  Close();
}

katana::ProgressScope::~ProgressScope() {
  if (span_ != nullptr) {
    Close();
  }
}

void
katana::ProgressScope::Close() {
  span_->MarkScopeClosed();
}

std::string
katana::ProgressContext::GetTraceID() const noexcept {
  return std::string{};
}

std::string
katana::ProgressContext::GetSpanID() const noexcept {
  return std::string{};
}

void
katana::ProgressSpan::LogError(
    const std::string& message, const katana::ErrorInfo& error) {
  Log(message, {{"event", "error"},
                {"error.kind", error.error_code().message()},
                {"error.context", fmt::format("{}", error)}});
}

void
katana::ProgressSpan::LogProfile() {
#ifdef KATANA_USE_JEMALLOC
  // Write a profile according to the environment variable
  // MALLOC_CONF
  int ret = mallctl("prof.dump", NULL, NULL, NULL, 0);
  if (ret) {
    KATANA_LOG_ERROR("jemalloc dump: {}", std::strerror(ret));
    return;
  }
  static int stage;
  Log("heap dumped", {{"profile_stage", stage}});
  stage++;
#endif
}

void
katana::ProgressSpan::MarkScopeClosed() {
  if (!scope_closed_) {
    scope_closed_ = true;
    ProgressTracer& tracer = ProgressTracer::Get();
    if (tracer.HasActiveSpan() && this == &tracer.GetActiveSpan()) {
      Finish();
    }
  }
}

bool
katana::ProgressSpan::ScopeClosed() {
  return scope_closed_;
}

void
katana::ProgressSpan::Finish() {
  if (!finished_) {
    finished_ = true;
    Close();
  }
  ProgressTracer& tracer = ProgressTracer::Get();
  if (tracer.HasActiveSpan() && this == &tracer.GetActiveSpan()) {
    tracer.FinishActiveSpan();
  }
}
