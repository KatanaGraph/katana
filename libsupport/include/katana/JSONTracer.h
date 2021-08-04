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
public:
  static std::unique_ptr<JSONTracer> Make(
      uint32_t host_id = 0, uint32_t num_hosts = 1);

  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, const ProgressContext& child_of) override;

  std::string Inject(const ProgressContext& ctx) override;
  std::unique_ptr<ProgressContext> Extract(const std::string& carrier) override;

  void Close() override {}

private:
  JSONTracer(uint32_t host_id = 0, uint32_t num_hosts = 1)
      : ProgressTracer(host_id, num_hosts) {}

  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name,
      std::shared_ptr<ProgressSpan> child_of) override;
};

class KATANA_EXPORT JSONContext : public ProgressContext {
public:
  std::unique_ptr<ProgressContext> Clone() const noexcept override;
  std::string GetTraceID() const noexcept override { return trace_id_; }
  std::string GetSpanID() const noexcept override { return span_id_; }

private:
  friend class JSONTracer;
  friend class JSONSpan;

  JSONContext(const std::string& trace_id, const std::string& span_id)
      : trace_id_(trace_id), span_id_(span_id) {}

  std::string trace_id_;
  std::string span_id_;
};

class KATANA_EXPORT JSONSpan : public ProgressSpan {
public:
  ~JSONSpan() override { Finish(); }

  void SetTags(const Tags& tags) override;

  void Log(const std::string& message, const Tags& tags = {}) override;

  const ProgressContext& GetContext() const noexcept override {
    return context_;
  }

private:
  friend JSONTracer;

  JSONSpan(const std::string& span_name, std::shared_ptr<ProgressSpan> parent);
  JSONSpan(const std::string& span_name, const ProgressContext& parent);
  static std::shared_ptr<ProgressSpan> Make(
      const std::string& span_name, std::shared_ptr<ProgressSpan> parent);
  static std::shared_ptr<ProgressSpan> Make(
      const std::string& span_name, const ProgressContext& parent);

  void Close() override;

  JSONContext context_;
};

}  // namespace katana

#endif
