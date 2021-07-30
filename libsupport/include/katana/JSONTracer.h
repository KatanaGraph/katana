#ifndef KATANA_LIBSUPPORT_KATANA_JSONTRACER_H_
#define KATANA_LIBSUPPORT_KATANA_JSONTRACER_H_

#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "katana/ProgressTracer.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT JSONTracer : public ProgressTracer {
  JSONTracer(uint32_t host_id = 0, uint32_t num_hosts = 1)
      : ProgressTracer(host_id, num_hosts) {}

public:
  static std::unique_ptr<JSONTracer> Make(
      uint32_t host_id = 0, uint32_t num_hosts = 1);

  std::unique_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, bool ignore_active_span) override;
  std::unique_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, ProgressSpan& child_of) override;
  std::unique_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, const ProgressContext& child_of) override;

  std::string Inject(const ProgressContext& ctx) override;
  std::unique_ptr<ProgressContext> Extract(const std::string& carrier) override;

  void Close() override {}
};

class KATANA_EXPORT JSONContext : public ProgressContext {
  friend class JSONTracer;
  friend class JSONSpan;

  JSONContext(const std::string& trace_id, const std::string& span_id)
      : trace_id_(trace_id), span_id_(span_id) {}

public:
  std::unique_ptr<ProgressContext> Clone() const noexcept override;
  std::string GetTraceID() const noexcept override { return trace_id_; }
  std::string GetSpanID() const noexcept override { return span_id_; }

private:
  std::string trace_id_;
  std::string span_id_;
};

class KATANA_EXPORT JSONSpan : public ProgressSpan {
  friend JSONTracer;

  JSONSpan(const std::string& span_name, ProgressSpan* parent);
  JSONSpan(const std::string& span_name, const ProgressContext& parent);
  static std::unique_ptr<ProgressSpan> Make(
      const std::string& span_name, ProgressSpan* parent);
  static std::unique_ptr<ProgressSpan> Make(
      const std::string& span_name, const ProgressContext& parent);

public:
  ~JSONSpan() override { Finish(); }

  void Close() override;

  void SetTags(const Tags& tags) override;

  void Log(const std::string& message, const Tags& tags = {}) override;

  const ProgressContext& GetContext() const noexcept override {
    return context_;
  }

private:
  JSONContext context_;
};

}  // namespace katana

#endif
