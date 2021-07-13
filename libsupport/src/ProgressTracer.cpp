#include "katana/ProgressTracer.h"

#include "katana/Logging.h"

std::unique_ptr<katana::ProgressTracer> katana::ProgressTracer::tracer_ =
    nullptr;

void
katana::ProgressTracer::SetProgressTracer(
    std::unique_ptr<katana::ProgressTracer> tracer) {
  ProgressTracer::tracer_ = std::move(tracer);
}

void
katana::ProgressTracer::SetSpan(std::shared_ptr<katana::ProgressSpan> span) {
  if (span != nullptr) {
    span->SetActive();
  }
  active_span_ = span;
  if (active_span_ != nullptr && active_span_->ScopeClosed()) {
    active_span_->Finish();
  }
}

katana::ProgressScope
katana::ProgressTracer::StartActiveSpan(
    const std::string& span_name, bool finish_on_close) {
  return SetActiveSpan(StartSpan(span_name), finish_on_close);
}
katana::ProgressScope
katana::ProgressTracer::StartActiveSpan(
    const std::string& span_name, const katana::ProgressContext& child_of,
    bool finish_on_close) {
  return SetActiveSpan(StartSpan(span_name, child_of), finish_on_close);
}

katana::ProgressScope
katana::ProgressTracer::SetActiveSpan(
    std::shared_ptr<katana::ProgressSpan> span, bool finish_on_close) {
  if (span == nullptr) {
    KATANA_LOG_FATAL("nullptr span given to Tracer's SetActiveSpan");
  }
  span->SetActive();
  active_span_ = span;
  return ProgressScope(span, finish_on_close);
}

std::shared_ptr<katana::ProgressSpan>
katana::ProgressTracer::GetActiveSpan() {
  return active_span_;
}

katana::ProgressScope::~ProgressScope() {
  if (span_ != nullptr && finish_on_close_) {
    Close();
  }
}

void
katana::ProgressScope::Close() {
  if (finish_on_close_) {
    span_->MarkScopeClosed();
  }
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
katana::ProgressSpan::MarkScopeClosed() {
  scope_closed_ = true;
  if (is_active_span_) {
    Finish();
  }
}

void
katana::ProgressSpan::SetActive() {
  std::shared_ptr<ProgressSpan> active =
      ProgressTracer::GetProgressTracer().GetActiveSpan();
  if (active != nullptr) {
    active->is_active_span_ = false;
  }
  is_active_span_ = true;
}

void
katana::ProgressSpan::LogError(
    const std::string& message, const katana::ErrorInfo& error) {
  Log(message, {{"event", "error"},
                {"error.kind", error.error_code().message()},
                {"error.context", fmt::format("{}", error)}});
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
    if (is_active_span_) {
      ProgressTracer::GetProgressTracer().SetSpan(parent_);
    }
  }
}
