#include "katana/NoopTracer.h"

std::unique_ptr<katana::NoopTracer>
katana::NoopTracer::Make(uint32_t host_id, uint32_t num_hosts) {
  return std::unique_ptr<NoopTracer>(new NoopTracer(host_id, num_hosts));
}

std::shared_ptr<katana::ProgressSpan>
katana::NoopTracer::StartSpan(
    [[maybe_unused]] const std::string& span_name,
    [[maybe_unused]] const ProgressContext& child_of) {
  return NoopSpan::Make();
}

std::unique_ptr<katana::ProgressContext>
katana::NoopTracer::Extract([[maybe_unused]] const std::string& carrier) {
  return std::unique_ptr<NoopContext>(new NoopContext());
}

std::shared_ptr<katana::ProgressSpan>
katana::NoopTracer::StartSpan(
    [[maybe_unused]] const std::string& span_name,
    std::shared_ptr<ProgressSpan> child_of) {
  return NoopSpan::Make(std::move(child_of));
}

std::unique_ptr<katana::ProgressContext>
katana::NoopContext::Clone() const noexcept {
  return std::unique_ptr<NoopContext>(new NoopContext());
}

const katana::ProgressContext&
katana::NoopSpan::GetContext() const noexcept {
  return context_;
}
