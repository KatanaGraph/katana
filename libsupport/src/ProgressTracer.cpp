#include "katana/ProgressTracer.h"

#include "katana/Logging.h"

std::unique_ptr<katana::ProgressTracer> katana::ProgressTracer::tracer_ =
    nullptr;

void
katana::ProgressTracer::SetProgressTracer(
    std::unique_ptr<katana::ProgressTracer> tracer) {
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
    if (!active_span_->IsFinished()) {
      active_span_->Finish();
    }
    active_span_ = active_span_->GetParentSpan();
    if (active_span_ != nullptr && active_span_->ScopeClosed()) {
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
katana::ProgressSpan::MarkScopeClosed() {
  if (!scope_closed_) {
    scope_closed_ = true;
    ProgressTracer& tracer = ProgressTracer::GetProgressTracer();
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

    ProgressTracer& tracer = ProgressTracer::GetProgressTracer();
    if (tracer.HasActiveSpan() && this == &tracer.GetActiveSpan()) {
      tracer.FinishActiveSpan();
    }
  }
}
