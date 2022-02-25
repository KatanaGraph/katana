#include "katana/JSONTracer.h"

#include <sys/time.h>

#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>

#include <arrow/memory_pool.h>

#include "katana/Random.h"
#include "katana/Time.h"

namespace {

std::mutex output_mutex;

std::string
GenerateID() {
  std::uniform_int_distribution<uint64_t> dist(
      0, std::numeric_limits<uint64_t>::max());
  return fmt::format("0x{:x}", dist(katana::GetGenerator()));
}

std::string
GetSpanJSON(
    const std::string& span_id, const std::string& span_name = "",
    const std::string& parent_span_id = "") {
  fmt::memory_buffer buf;
  if (span_name.empty() && parent_span_id.empty()) {
    fmt::format_to(
        std::back_inserter(buf), R"("span_data":{{"span_id":"{}"}})", span_id);
  } else {
    fmt::format_to(
        std::back_inserter(buf),
        R"("span_data":{{"span_name":"{}","span_id":"{}","parent_id":"{}"}})",
        span_name, span_id, parent_span_id);
  }
  return fmt::to_string(buf);
}

std::string
GetSpanJSON(const std::string& span_id, bool finish) {
  fmt::memory_buffer buf;
  if (finish) {
    fmt::format_to(
        std::back_inserter(buf),
        R"("span_data":{{"span_id":"{}","finished":true}})", span_id);
  } else {
    fmt::format_to(
        std::back_inserter(buf), R"("span_data":{{"span_id":"{}"}})", span_id);
  }
  return fmt::to_string(buf);
}

std::string
GetHostStatsJSON() {
  fmt::memory_buffer buf;
  katana::HostStats host_stats = katana::ProgressTracer::GetHostStats();
  auto& tracer = katana::ProgressTracer::Get();

  fmt::format_to(
      std::back_inserter(buf),
      R"("host_data":{{"hosts":{},"hostname":"{}","hardware_threads":{},"pid":{},"ram_gb":{}}})",
      tracer.GetNumHosts(), host_stats.hostname, host_stats.nprocs,
      host_stats.pid, host_stats.ram_gb);

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
        std::back_inserter(buf), R"({{"name":"{}","value":{}}})", tag.first,
        katana::ProgressTracer::GetValue(tag.second));
  }
  fmt::format_to(std::back_inserter(buf), "]");
  return fmt::to_string(buf);
}

std::string
GetLogJSON(const std::string& message) {
  fmt::memory_buffer buf;

  auto usec_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                     katana::Now().time_since_epoch())
                     .count();
  fmt::format_to(
      std::back_inserter(buf),
      R"("log":{{"msg":"{}","timestamp_us":{},"max_mem_gb":{:.3f},"mem_gb":{:.3f},"arrow_mem_gb":{:.3f}}})",
      message, usec_ts,
      katana::ProgressTracer::GetMaxMemBytes() / 1024.0 / 1024.0 / 1024.0,
      katana::ProgressTracer::ParseProcSelfRssBytes() / 1024.0 / 1024.0 /
          1024.0,
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
  uint32_t host_id = katana::ProgressTracer::Get().GetHostID();
  static auto begin = katana::Now();
  auto msec_since_begin = katana::UsSince(begin) / 1000;

  fmt::format_to(
      std::back_inserter(buf), R"({{"host":{},"offset_ms":{})", host_id,
      msec_since_begin);
  if (!log_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", log_data);
  }
  if (!tag_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", tag_data);
  }
  if (!host_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", host_data);
  }
  fmt::format_to(
      std::back_inserter(buf),
      R"(,{},"trace_id":"{}"}})"
      "\n",
      span_data, trace_id);
  return fmt::to_string(buf);
}

void
OutputJSON(const katana::OutputCB& out_callback, const std::string& output) {
  std::lock_guard<std::mutex> lock(output_mutex);
  out_callback(output);
}

}  // namespace

std::unique_ptr<katana::JSONTracer>
katana::JSONTracer::Make(uint32_t host_id, uint32_t num_hosts) {
  return std::unique_ptr<JSONTracer>(new JSONTracer(
      host_id, num_hosts,
      [](const std::string& output) { std::cout << output; }));
}

std::unique_ptr<katana::JSONTracer>
katana::JSONTracer::Make(
    uint32_t host_id, uint32_t num_hosts, katana::OutputCB out_callback) {
  return std::unique_ptr<JSONTracer>(
      new JSONTracer(host_id, num_hosts, std::move(out_callback)));
}

std::shared_ptr<katana::ProgressSpan>
katana::JSONTracer::StartSpan(
    const std::string& span_name, const katana::ProgressContext& child_of) {
  return JSONSpan::Make(span_name, child_of, out_callback_);
}

std::string
katana::JSONTracer::Inject(const katana::ProgressContext& ctx) {
  return ctx.GetTraceID() + "," + ctx.GetSpanID();
}

std::unique_ptr<katana::ProgressContext>
katana::JSONTracer::Extract(const std::string& carrier) {
  size_t split = carrier.find(',');
  if (split == std::string::npos) {
    return nullptr;
  }
  std::string trace_id = carrier.substr(0, split);
  std::string span_id = carrier.substr(split + 1);
  return std::unique_ptr<JSONContext>(new JSONContext(trace_id, span_id));
}

std::shared_ptr<katana::ProgressSpan>
katana::JSONTracer::StartSpan(
    const std::string& span_name,
    std::shared_ptr<katana::ProgressSpan> child_of) {
  return JSONSpan::Make(span_name, std::move(child_of), out_callback_);
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
  OutputJSON(out_callback_, output_json);
}

void
katana::JSONSpan::Log(const std::string& message, const katana::Tags& tags) {
  std::string span_data = GetSpanJSON(GetContext().GetSpanID());
  std::string log_data = GetLogJSON(message);
  std::string tag_data = GetTagsJSON(tags);
  std::string host_data;

  std::string output_json = BuildJSON(
      GetContext().GetTraceID(), span_data, log_data, tag_data, host_data);
  OutputJSON(out_callback_, output_json);
}

katana::JSONSpan::JSONSpan(
    const std::string& span_name, std::shared_ptr<katana::ProgressSpan> parent,
    OutputCB out_callback)
    : ProgressSpan(std::move(parent)),
      context_(JSONContext{"", ""}),
      out_callback_(std::move(out_callback)) {
  std::string parent_span_id{"null"};
  std::string trace_id;
  std::string host_data;
  if (GetParentSpan() != nullptr) {
    auto parent_span = std::static_pointer_cast<JSONSpan>(GetParentSpan());
    parent_span_id = parent_span->GetContext().GetSpanID();
    trace_id = parent_span->GetContext().GetTraceID();
  } else {
    trace_id = GenerateID();
    host_data = GetHostStatsJSON();
  }
  std::string span_id = GenerateID();
  context_ = JSONContext(trace_id, span_id);

  const std::string& message = span_name;

  std::string span_data = GetSpanJSON(span_id, span_name, parent_span_id);
  std::string log_data = GetLogJSON(message);
  std::string tag_data;

  std::string output_json =
      BuildJSON(trace_id, span_data, log_data, tag_data, host_data);
  OutputJSON(out_callback_, output_json);
}

katana::JSONSpan::JSONSpan(
    const std::string& span_name, const katana::ProgressContext& parent,
    OutputCB out_callback)
    : ProgressSpan(nullptr),
      context_(JSONContext{"", ""}),
      out_callback_(std::move(out_callback)) {
  std::string parent_span_id = parent.GetSpanID();
  std::string trace_id = parent.GetTraceID();
  std::string span_id = GenerateID();
  context_ = JSONContext(trace_id, span_id);

  const std::string& message = span_name;

  std::string host_data = GetHostStatsJSON();
  std::string span_data = GetSpanJSON(span_id, span_name, parent_span_id);
  std::string log_data = GetLogJSON(message);
  std::string tag_data;

  std::string output_json =
      BuildJSON(trace_id, span_data, log_data, tag_data, host_data);
  OutputJSON(out_callback_, output_json);
}

std::shared_ptr<katana::ProgressSpan>
katana::JSONSpan::Make(
    const std::string& span_name, std::shared_ptr<katana::ProgressSpan> parent,
    OutputCB out_callback) {
  return std::shared_ptr<JSONSpan>(
      new JSONSpan(span_name, std::move(parent), std::move(out_callback)));
}
std::shared_ptr<katana::ProgressSpan>
katana::JSONSpan::Make(
    const std::string& span_name, const katana::ProgressContext& parent,
    OutputCB out_callback) {
  return std::shared_ptr<JSONSpan>(
      new JSONSpan(span_name, parent, std::move(out_callback)));
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
  OutputJSON(out_callback_, output_json);
}
