#include "katana/TextTracer.h"

#include <sys/time.h>

#include <cstring>
#include <iostream>
#include <mutex>
#include <string>

#include <arrow/memory_pool.h>

#include "katana/Random.h"
#include "katana/Time.h"

namespace {

std::mutex output_mutex;

std::string
GetHostStatsText() {
  katana::HostStats host_stats = katana::ProgressTracer::GetHostStats();
  auto& tracer = katana::ProgressTracer::Get();
  return fmt::format(
      "hosts={} hostname={} hardware_threads={} pid={} ram_gb={}",
      tracer.GetNumHosts(), host_stats.hostname, host_stats.nprocs,
      host_stats.pid, host_stats.ram_gb);
}

std::string
GetTagsText(const katana::Tags& tags) {
  if (tags.empty()) {
    return std::string{};
  }
  fmt::memory_buffer buf;

  for (uint32_t i = 0; i < tags.size(); i++) {
    const std::pair<std::string, katana::Value>& tag = tags[i];

    if (i != 0) {
      fmt::format_to(std::back_inserter(buf), " ");
    }
    fmt::format_to(
        std::back_inserter(buf), "{}={}", tag.first,
        katana::ProgressTracer::GetValue(tag.second));
  }
  return fmt::to_string(buf);
}

std::string
BuildText(
    const std::string& message, const std::string& tag_data,
    const std::string& host_data) {
  fmt::memory_buffer buf;
  uint32_t host_id = katana::ProgressTracer::Get().GetHostID();
  static auto begin = katana::Now();
  auto msec_since_begin = katana::UsSince(begin) / 1000;

  fmt::format_to(
      std::back_inserter(buf),
      "TRACE: host={} ms={} max_mem_gb={:.3f} mem_gb={:.3f} "
      "arrow_mem_gb={:.3f}",
      host_id, msec_since_begin,
      katana::ProgressTracer::GetMaxMemBytes() / 1024.0 / 1024.0 / 1024.0,
      katana::ProgressTracer::ParseProcSelfRssBytes() / 1024.0 / 1024.0 /
          1024.0,
      arrow::default_memory_pool()->bytes_allocated() / 1024.0 / 1024.0 /
          1024.0);
  if (!tag_data.empty()) {
    fmt::format_to(std::back_inserter(buf), " {}", tag_data);
  }
  if (!host_data.empty()) {
    fmt::format_to(std::back_inserter(buf), " {}", host_data);
  }
  fmt::format_to(std::back_inserter(buf), " | {}\n", message);
  return fmt::to_string(buf);
}

void
OutputText(const std::string& output) {
  std::lock_guard<std::mutex> lock(output_mutex);
  std::cerr << output;
}

}  // namespace

std::unique_ptr<katana::TextTracer>
katana::TextTracer::Make(uint32_t host_id, uint32_t num_hosts) {
  return std::unique_ptr<TextTracer>(new TextTracer(host_id, num_hosts));
}

std::shared_ptr<katana::ProgressSpan>
katana::TextTracer::StartSpan(
    const std::string& span_name, const katana::ProgressContext& child_of) {
  return TextSpan::Make(span_name, child_of);
}

std::string
katana::TextTracer::Inject(const katana::ProgressContext& ctx) {
  return ctx.GetTraceID() + "," + ctx.GetSpanID();
}

std::unique_ptr<katana::ProgressContext>
katana::TextTracer::Extract(const std::string& carrier) {
  size_t split = carrier.find(',');
  if (split == std::string::npos) {
    return nullptr;
  }
  std::string trace_id = carrier.substr(0, split);
  std::string span_id = carrier.substr(split + 1);
  return std::unique_ptr<TextContext>(new TextContext(trace_id, span_id));
}

std::shared_ptr<katana::ProgressSpan>
katana::TextTracer::StartSpan(
    const std::string& span_name,
    std::shared_ptr<katana::ProgressSpan> child_of) {
  return TextSpan::Make(span_name, std::move(child_of));
}

std::unique_ptr<katana::ProgressContext>
katana::TextContext::Clone() const noexcept {
  return std::unique_ptr<TextContext>(new TextContext(trace_id_, span_id_));
}

void
katana::TextSpan::SetTags(const katana::Tags& tags) {
  std::string message = "tags for " + span_name_;
  std::string tags_data = GetTagsText(tags);
  std::string host_data;

  std::string output_Text = BuildText(message, tags_data, host_data);
  OutputText(output_Text);
}

void
katana::TextSpan::Log(const std::string& message, const katana::Tags& tags) {
  std::string tags_data = GetTagsText(tags);
  std::string host_data;

  std::string output_Text = BuildText(message, tags_data, host_data);
  OutputText(output_Text);
}

katana::TextSpan::TextSpan(
    const std::string& span_name, std::shared_ptr<katana::ProgressSpan> parent)
    : ProgressSpan(std::move(parent)),
      context_(TextContext{"0", "0"}),
      span_name_(span_name) {
  std::string message = "starting " + span_name;
  std::string tags_data;
  std::string host_data;
  if (GetParentSpan() == nullptr) {
    host_data = GetHostStatsText();
  }

  std::string output_Text = BuildText(message, tags_data, host_data);
  OutputText(output_Text);
}

katana::TextSpan::TextSpan(
    const std::string& span_name,
    [[maybe_unused]] const katana::ProgressContext& parent)
    : ProgressSpan(nullptr),
      context_(TextContext{"0", "0"}),
      span_name_(span_name) {
  std::string message = "starting " + span_name;
  std::string tags_data;
  std::string host_data = GetHostStatsText();

  std::string output_Text = BuildText(message, tags_data, host_data);
  OutputText(output_Text);
}

std::shared_ptr<katana::ProgressSpan>
katana::TextSpan::Make(
    const std::string& span_name,
    std::shared_ptr<katana::ProgressSpan> parent) {
  return std::shared_ptr<TextSpan>(new TextSpan(span_name, std::move(parent)));
}

std::shared_ptr<katana::ProgressSpan>
katana::TextSpan::Make(
    const std::string& span_name, const katana::ProgressContext& parent) {
  return std::shared_ptr<TextSpan>(new TextSpan(span_name, parent));
}

void
katana::TextSpan::Close() {
  std::string message = "finished " + span_name_;
  std::string tags_data;
  std::string host_data;

  std::string output_Text = BuildText(message, tags_data, host_data);
  OutputText(output_Text);
}
