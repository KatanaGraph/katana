#ifndef KATANA_LIBSUPPORT_KATANA_TEXTTRACER_H_
#define KATANA_LIBSUPPORT_KATANA_TEXTTRACER_H_

#include <memory>
#include <string>
#include <vector>

#include "katana/ProgressTracer.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT TextTracer : public ProgressTracer {
public:
  static std::unique_ptr<TextTracer> Make(
      uint32_t host_id = 0, uint32_t num_hosts = 1);

  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, const ProgressContext& child_of) override;

  std::string Inject(const ProgressContext& ctx) override;
  std::unique_ptr<ProgressContext> Extract(const std::string& carrier) override;

private:
  TextTracer(uint32_t host_id, uint32_t num_hosts)
      : ProgressTracer(host_id, num_hosts) {}

  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name,
      std::shared_ptr<ProgressSpan> child_of) override;

  void Close() override {}
};

class KATANA_EXPORT TextContext : public ProgressContext {
public:
  std::unique_ptr<ProgressContext> Clone() const noexcept override;
  std::string GetTraceID() const noexcept override { return trace_id_; }
  std::string GetSpanID() const noexcept override { return span_id_; }

private:
  friend class TextTracer;
  friend class TextSpan;

  TextContext(std::string trace_id, std::string span_id)
      : trace_id_(std::move(trace_id)), span_id_(std::move(span_id)) {}

  std::string trace_id_;
  std::string span_id_;
};

class KATANA_EXPORT TextSpan : public ProgressSpan {
public:
  ~TextSpan() override { Finish(); }

  void SetTags(const Tags& tags) override;

  void Log(const std::string& message, const Tags& tags) override;

  const ProgressContext& GetContext() const noexcept override {
    return context_;
  }

private:
  friend TextTracer;

  TextSpan(const std::string& span_name, std::shared_ptr<ProgressSpan> parent);
  TextSpan(const std::string& span_name, const ProgressContext& parent);
  static std::shared_ptr<ProgressSpan> Make(
      const std::string& span_name, std::shared_ptr<ProgressSpan> parent);
  static std::shared_ptr<ProgressSpan> Make(
      const std::string& span_name, const ProgressContext& parent);

  void Close() override;

  TextContext context_;
  std::string span_name_;
};

}  // namespace katana

#endif
