#include "katana/JSONTracer.h"

#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <regex>

#include <arrow/memory_pool.h>

#include "katana/Time.h"

#if __linux__
#include <sys/sysinfo.h>
#endif

namespace {

const std::regex kRssRegex("^Rss\\w+:\\s+([0-9]+) kB");

struct HostStats {
  long nprocs{};
  long ram_gb{};
  std::string hostname;
};

#if __linux__

uint64_t
ParseProcSelfRssBytes() {
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

HostStats
GetHostStats() {
  HostStats stats;

  struct sysinfo info;
  sysinfo(&info);

  char hostname[256];
  gethostname(hostname, 256);

  stats.ram_gb = info.totalram / 1024 / 1024 / 1024;
  stats.nprocs = get_nprocs();
  stats.hostname = hostname;

  return stats;
}

#else

uint64_t
ParseProcSelfRssBytes() {
  KATANA_WARN_ONCE(
      "calculating resident set size is not implemented for this platform");
  return 0;
}

HostStats
GetHostStats() {
  KATANA_WARN_ONCE("getting host stats is not implemented for this platform");
  HostStats stats;
  return stats;
}

#endif

// TODO (Patrick)
// This function is included for debugging purposes while
// the tracing infrastructure is added and tested
uint32_t id = 0;
std::string
GenerateID(uint32_t host_id) {
  id++;
  return std::to_string(host_id * 1000 + id);
}

std::string
GetValue(const katana::Value& value) {
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

std::string
GetSpanJSON(
    const std::string& span_id, const std::string& span_name = std::string(),
    const std::string& parent_span_id = std::string()) {
  fmt::memory_buffer buf;
  if (span_name.empty() && parent_span_id.empty()) {
    fmt::format_to(
        std::back_inserter(buf), "\"span_data\":{{\"span_id\":\"{}\"}}",
        span_id);
  } else {
    fmt::format_to(
        std::back_inserter(buf),
        "\"span_data\":{{\"span_id\":\"{}\",\"span_name\":\"{}\",\"parent_id\":"
        "\"{}\"}}",
        span_id, span_name, parent_span_id);
  }
  return fmt::to_string(buf);
}

std::string
GetSpanJSON(const std::string& span_id, bool finish) {
  fmt::memory_buffer buf;
  if (finish) {
    fmt::format_to(
        std::back_inserter(buf),
        "\"span_data\":{{\"span_id\":\"{}\",\"finished\":true}}", span_id);
  } else {
    fmt::format_to(
        std::back_inserter(buf), "\"span_data\":{{\"span_id\":\"{}\"}}",
        span_id);
  }
  return fmt::to_string(buf);
}

std::string
GetHostStatsJSON() {
  fmt::memory_buffer buf;
  HostStats host_stats = GetHostStats();
  auto& tracer = katana::ProgressTracer::GetProgressTracer();

  fmt::format_to(
      std::back_inserter(buf),
      "\"host_data\":{{\"hosts\":{},\"hostname\":\"{}\",\"hardware_threads\":{}"
      ",\"ram_gb\":"
      "{}}}",
      tracer.GetNumHosts(), host_stats.hostname, host_stats.nprocs,
      host_stats.ram_gb);

  return fmt::to_string(buf);
}

std::string
GetTagsJSON(const katana::Tags& tags) {
  if (tags.empty()) {
    return std::string{};
  }
  fmt::memory_buffer buf;

  fmt::format_to(std::back_inserter(buf), "\"tags\":[");
  for (uint32_t i = 0; i < tags.size(); i++) {
    const std::pair<std::string, katana::Value>& tag = tags[i];

    if (i != 0) {
      fmt::format_to(std::back_inserter(buf), ",");
    }
    fmt::format_to(
        std::back_inserter(buf), "{{\"name\":\"{}\",\"value\":{}}}", tag.first,
        GetValue(tag.second));
  }
  fmt::format_to(std::back_inserter(buf), "]");
  return fmt::to_string(buf);
}

std::string
GetLogJSON(const std::string& message) {
  fmt::memory_buffer buf;
  struct rusage rusage;
  getrusage(RUSAGE_SELF, &rusage);

  auto usec_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                     katana::Now().time_since_epoch())
                     .count();
  fmt::format_to(
      std::back_inserter(buf),
      "\"log\":{{\"msg\":\"{}\",\"timestamp_us\":{},\"max_mem_gb\":{:.3f}"
      ","
      "\"mem_gb\":{:.3f},\"arrow_mem_gb\":{:.3f}}}",
      message, usec_ts, rusage.ru_maxrss / 1024.0 / 1024.0,
      ParseProcSelfRssBytes() / 1024.0 / 1024.0 / 1024.0,
      arrow::default_memory_pool()->bytes_allocated() / 1024.0 / 1024.0 /
          1024.0);

  return fmt::to_string(buf);
}

std::string
BuildJSON(
    const std::string& trace_id, const std::string& span_data,
    const std::string& log_data, const std::string& tag_data,
    const std::string& host_data) {
  fmt::memory_buffer buf;
  uint32_t host_id = katana::ProgressTracer::GetProgressTracer().GetHostID();
  static auto begin = katana::Now();
  auto msec_since_begin = katana::UsSince(begin) / 1000;

  fmt::format_to(
      std::back_inserter(buf),
      "{{\"trace_id\":\"{}\",\"host\":{},\"offset_ms\":{},{}", trace_id,
      host_id, msec_since_begin, span_data);
  if (!log_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", log_data);
  }
  if (!tag_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", tag_data);
  }
  if (!host_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", host_data);
  }
  fmt::format_to(std::back_inserter(buf), "}}\n");
  return fmt::to_string(buf);
}

}  // namespace

std::unique_ptr<katana::JSONTracer>
katana::JSONTracer::Make(uint32_t host_id, uint32_t num_hosts) {
  return std::unique_ptr<JSONTracer>(new JSONTracer(host_id, num_hosts));
}

std::shared_ptr<katana::ProgressSpan>
katana::JSONTracer::StartSpan(
    const std::string& span_name, const katana::ProgressContext& child_of) {
  return JSONSpan::Make(span_name, child_of);
}

std::string
katana::JSONTracer::Inject(const katana::ProgressContext& ctx) {
  return ctx.GetTraceID() + "," + ctx.GetSpanID();
}

std::unique_ptr<katana::ProgressContext>
katana::JSONTracer::Extract(const std::string& carrier) {
  size_t split = carrier.find(",");
  if (split == std::string::npos) {
    return nullptr;
  } else {
    std::string trace_id = carrier.substr(0, split);
    std::string span_id = carrier.substr(split + 1);
    return std::unique_ptr<JSONContext>(new JSONContext(trace_id, span_id));
  }
}

std::shared_ptr<katana::ProgressSpan>
katana::JSONTracer::StartSpan(
    const std::string& span_name,
    std::shared_ptr<katana::ProgressSpan> child_of) {
  return JSONSpan::Make(span_name, std::move(child_of));
}

std::unique_ptr<katana::ProgressContext>
katana::JSONContext::Clone() const noexcept {
  return std::unique_ptr<JSONContext>(new JSONContext(trace_id_, span_id_));
}

void
katana::JSONSpan::SetTags(const katana::Tags& tags) {
  std::string span_data = GetSpanJSON(GetContext().GetSpanID());
  std::string log_data;
  std::string tag_data = GetTagsJSON(tags);
  std::string host_data;

  std::string output_json = BuildJSON(
      GetContext().GetTraceID(), span_data, log_data, tag_data, host_data);

  std::cerr << output_json;
}

void
katana::JSONSpan::Log(const std::string& message, const katana::Tags& tags) {
  std::string span_data = GetSpanJSON(GetContext().GetSpanID());
  std::string log_data = GetLogJSON(message);
  std::string tag_data = GetTagsJSON(tags);
  std::string host_data;

  std::string output_json = BuildJSON(
      GetContext().GetTraceID(), span_data, log_data, tag_data, host_data);

  std::cerr << output_json;
}

katana::JSONSpan::JSONSpan(
    const std::string& span_name, std::shared_ptr<katana::ProgressSpan> parent)
    : ProgressSpan(parent), context_(JSONContext{"", ""}) {
  auto& tracer = ProgressTracer::GetProgressTracer();

  std::string parent_span_id{"null"};
  std::string trace_id;
  std::string host_data;
  if (parent != nullptr) {
    auto parent_span = std::static_pointer_cast<JSONSpan>(parent);
    parent_span_id = parent_span->GetContext().GetSpanID();
    trace_id = parent_span->GetContext().GetTraceID();
  } else {
    trace_id = GenerateID(tracer.GetHostID());
    host_data = GetHostStatsJSON();
  }
  std::string span_id = GenerateID(tracer.GetHostID());
  context_ = JSONContext(trace_id, span_id);

  std::string message{"start"};

  std::string span_data = GetSpanJSON(span_id, span_name, parent_span_id);
  std::string log_data = GetLogJSON(message);
  std::string tag_data;

  std::string output_json =
      BuildJSON(trace_id, span_data, log_data, tag_data, host_data);

  std::cerr << output_json;
}

katana::JSONSpan::JSONSpan(
    const std::string& span_name, const katana::ProgressContext& parent)
    : ProgressSpan(nullptr), context_(JSONContext{"", ""}) {
  auto& tracer = ProgressTracer::GetProgressTracer();

  std::string parent_span_id = parent.GetSpanID();
  std::string trace_id = parent.GetTraceID();
  std::string span_id = GenerateID(tracer.GetHostID());
  context_ = JSONContext(trace_id, span_id);

  std::string message{"start"};

  std::string host_data = GetHostStatsJSON();
  std::string span_data = GetSpanJSON(span_id, span_name, parent_span_id);
  std::string log_data = GetLogJSON(message);
  std::string tag_data;

  std::string output_json =
      BuildJSON(trace_id, span_data, log_data, tag_data, host_data);

  std::cerr << output_json;
}

std::shared_ptr<katana::ProgressSpan>
katana::JSONSpan::Make(
    const std::string& span_name,
    std::shared_ptr<katana::ProgressSpan> parent) {
  return std::shared_ptr<JSONSpan>(new JSONSpan(span_name, std::move(parent)));
}
std::shared_ptr<katana::ProgressSpan>
katana::JSONSpan::Make(
    const std::string& span_name, const katana::ProgressContext& parent) {
  return std::shared_ptr<JSONSpan>(new JSONSpan(span_name, parent));
}

void
katana::JSONSpan::Close() {
  std::string message{"finished"};

  std::string span_data = GetSpanJSON(GetContext().GetSpanID(), true);
  std::string log_data = GetLogJSON(message);
  std::string tag_data;
  std::string host_data;

  std::string output_json = BuildJSON(
      GetContext().GetTraceID(), span_data, log_data, tag_data, host_data);

  std::cerr << output_json;
}
