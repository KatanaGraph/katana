#ifndef KATANA_LIBSUPPORT_KATANA_NOOPTRACER_H_
#define KATANA_LIBSUPPORT_KATANA_NOOPTRACER_H_

#include "katana/ProgressTracer.h"

namespace katana {

class KATANA_EXPORT NoopTracer : public ProgressTracer {
public:
  static std::unique_ptr<NoopTracer> Make(
      uint32_t host_id = 0, uint32_t num_hosts = 1);

  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, const ProgressContext& child_of) override;

  std::string Inject([[maybe_unused]] const ProgressContext& ctx) override {
    return "";
  }
  std::unique_ptr<ProgressContext> Extract(const std::string& carrier) override;

private:
  NoopTracer(uint32_t host_id, uint32_t num_hosts)
      : ProgressTracer(host_id, num_hosts) {}

  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name,
      std::shared_ptr<ProgressSpan> child_of) override;

  void Close() override {}
};

class KATANA_EXPORT NoopContext : public ProgressContext {
public:
  std::unique_ptr<ProgressContext> Clone() const noexcept override;
  std::string GetTraceID() const noexcept override { return ""; }
  std::string GetSpanID() const noexcept override { return ""; }

private:
  friend class NoopTracer;
  friend class NoopSpan;

  NoopContext() {}
};

class KATANA_EXPORT NoopSpan : public ProgressSpan {
public:
  ~NoopSpan() override { Finish(); }

  void SetTags([[maybe_unused]] const Tags& tags) override {}

  void Log(
      [[maybe_unused]] const std::string& message,
      [[maybe_unused]] const Tags& tags) override {}

  const ProgressContext& GetContext() const noexcept override;

private:
  friend NoopTracer;

  NoopSpan() : ProgressSpan(nullptr), context_(NoopContext{}) {}
  NoopSpan(std::shared_ptr<ProgressSpan> parent)
      : ProgressSpan(std::move(parent)), context_(NoopContext{}) {}
  static std::shared_ptr<ProgressSpan> Make(
      std::shared_ptr<ProgressSpan> parent) {
    return std::shared_ptr<NoopSpan>(new NoopSpan(std::move(parent)));
  }
  static std::shared_ptr<ProgressSpan> Make() {
    return std::shared_ptr<NoopSpan>(new NoopSpan());
  }

  void Close() override{};

  NoopContext context_;
};

}  // namespace katana

#endif
