#ifndef KATANA_LIBSUPPORT_KATANA_PROGRESSTRACER_H_
#define KATANA_LIBSUPPORT_KATANA_PROGRESSTRACER_H_

#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "katana/Result.h"
#include "katana/config.h"

/// This tracer does not currently support thread-local tracers or concurrency controls
/// Functions should only be used in a single-threaded context
///
/// Best Practices:
///   If possible always avoid creating raw ProgressSpans from ProgressTracer
///   If possible always use ProgressScopes to handle ProgressSpans
///   Only use 1 ProgressTracer in an execution, so they should be created at entry points
///   Use ProgressScope's RAII to handle early returns (i.e. due to errors)
///   Raw ProgressSpans should be used for special scenarios like tracing asynchronous calls
///
/// Notes:
///   SharedMemSys and DistMemSys will initialize the global ProgressTracer to a JsonTracer
///     later it will be set to a No-op Tracer, on Fini or the destructor they will call
///     Close on the global ProgressTracer
///   This prevents GetProgressTracer() from returning nullptr and ensures Tracers are closed
///
///   ProgressScope's will only close their ProgressSpan when their ProgressSpan is the active span
///   This means that if a user exclusively uses ProgressScopes,
///   Then parent ProgressSpans will not be closed until their child ProgressSpan's close
///   i.e.
///   {
///      auto scope1 = tracer.StartActiveSpan("1");
///      auto scope2 = tracer.StartActiveSpan("2");
///      if (err) { return; }
///      scope2.Close();
///      auto scope3 = tracer.StartActiveSpan("3");
///   }
///   Always results in scope2's ProgressSpan finishing before scope1's
///   and scope3's ProgressSpan finishing before scope1's if there is no error

namespace katana {

// This is needed to properly handle being passed const char* values
// Once we submodule OpenTracing this will be removed and their Value class used instead
// TODO(Patrick)
using variant_type = std::variant<
    bool, int, int64_t, uint32_t, uint64_t, float, double, std::string,
    const char*>;
class Value : public variant_type {
public:
  Value(bool x) noexcept : variant_type(x) {}
  Value(int x) noexcept : variant_type(static_cast<int64_t>(x)) {}
  Value(int64_t x) noexcept : variant_type(x) {}
  Value(uint32_t x) noexcept : variant_type(static_cast<uint64_t>(x)) {}
  Value(uint64_t x) noexcept : variant_type(x) {}
  Value(double x) noexcept : variant_type(x) {}
  Value(std::string x) noexcept : variant_type(x) {}
  Value(const char* s) noexcept : variant_type(std::string(s)) {}
};
using Tags = std::vector<std::pair<std::string, Value>>;

class ProgressScope;
class ProgressSpan;
class ProgressContext;

class KATANA_EXPORT ProgressTracer {
protected:
  ProgressTracer(uint32_t host_id, uint32_t num_hosts)
      : host_id_(host_id), num_hosts_(num_hosts) {}

public:
  virtual ~ProgressTracer() = default;

  static ProgressTracer& GetProgressTracer() { return *tracer_; }
  static void SetProgressTracer(std::unique_ptr<ProgressTracer> tracer);

  // StartActiveSpan creates a new span. If there is not an active span,
  // create a new top-level span. Otherwise, this function creates a child
  // span of the active span. The returned scope will finish
  // the span on close..
  ProgressScope StartActiveSpan(const std::string& span_name);
  ProgressScope StartActiveSpan(
      const std::string& span_name, const ProgressContext& child_of);

  /// Set the current active thread-local span to provided span
  /// Returns a ProgressScope
  /// Will finish the span when the scope goes out of scope
  /// by RAII
  /// Throws a fatal error if called with a nullptr span
  ProgressScope SetActiveSpan(std::unique_ptr<ProgressSpan> span);
  /// Set the current active thread-local span to provided span
  /// Does not throw an error if called with a nullptr span
  /// Primarily for internal class use or for reactivating a span
  /// with an existing ProgressScope
  void SetActiveSpan(ProgressSpan* span);

  /// Create a new top level span if ignore_active_span=true and
  /// no child_of value is given
  /// Otherwise creates a child span of the child_of span or active span
  /// Should only be used to handle multiple active spans simultaneously
  virtual std::unique_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, bool ignore_active_span) = 0;
  std::unique_ptr<ProgressSpan> StartSpan(const std::string& span_name);
  virtual std::unique_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, ProgressSpan& child_of) = 0;
  virtual std::unique_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, const ProgressContext& child_of) = 0;

  // For passing spans across process/host boundaries
  // These functions are needed, but are implemented now for
  // debugging purposes, they will be replaced
  // Extract returns a nullptr on failure
  virtual std::string Inject(const ProgressContext& ctx) = 0;
  virtual std::unique_ptr<ProgressContext> Extract(
      const std::string& carrier) = 0;

  /// Get the current scope’s span to add tagging/logging without having
  /// to pass around scopes/spans as parameters
  /// If there is no active span, an unnamed root span of a new trace
  /// is created (in this case the program is probably not using tracing)
  virtual ProgressSpan& GetActiveSpan();
  bool HasActiveSpan() { return active_span_ != nullptr; }
  uint32_t GetHostID() { return host_id_; }
  uint32_t GetNumHosts() { return num_hosts_; }

  /// Close is called when a tracer is finished processing spans
  /// This should be called to ensure any and all buffered spans are
  /// flushed
  virtual void Close() = 0;

private:
  static std::unique_ptr<ProgressTracer> tracer_;

  ProgressSpan* active_span_ = nullptr;
  uint32_t host_id_;
  uint32_t num_hosts_;
  std::unique_ptr<ProgressSpan> default_active_span_ = nullptr;
};

class KATANA_EXPORT ProgressScope {
  friend class ProgressTracer;

  ProgressScope(std::unique_ptr<ProgressSpan> span) : span_(std::move(span)) {}

public:
  ~ProgressScope();
  ProgressScope(const ProgressScope&) = delete;
  ProgressScope(ProgressScope&&) = delete;
  ProgressScope& operator=(const ProgressScope&) = delete;
  ProgressScope& operator=(ProgressScope&&) = delete;

  /// Get the scope’s underlying span to add tagging/logging,
  /// span relationships, and handle multiple active spans at a time
  ProgressSpan& span() { return *span_; }

  /// Closes the underlying ProgressSpan if the ProgressScope was created
  /// with the flag finish_on_close=true
  /// Note that this will only close the underlying ProgressSpan when all
  /// of its active children ProgressSpans have been finished
  /// This will be called by RAII if it has not already been called
  void Close();

private:
  std::unique_ptr<ProgressSpan> span_;
};

class KATANA_EXPORT ProgressContext {
public:
  virtual ~ProgressContext() = default;
  virtual std::unique_ptr<ProgressContext> Clone() const noexcept = 0;
  virtual std::string GetTraceID() const noexcept;
  virtual std::string GetSpanID() const noexcept;
};

class KATANA_EXPORT ProgressSpan {
  virtual void Close() = 0;

protected:
  ProgressSpan(ProgressSpan* parent) : parent_(parent) {}

public:
  /// If Finish has not already been called for the ProgressSpan, it's
  /// destructor must do so.
  virtual ~ProgressSpan() = default;

  /// Adds a tag to the span.
  virtual void SetTags(const Tags& tags) = 0;
  void SetError() { SetTags({{"error", true}}); }

  /// Output logging as well as standard metrics and extra stats
  /// Current standard metrics: max_mem, mem, host, and timestamp.
  virtual void Log(const std::string& message, const Tags& tags) = 0;
  void Log(const std::string& message) { Log(message, {}); }
  void LogError(const std::string& message) {
    Log(message, {{"event", "error"}});
  }
  void LogError(const std::string& message, const ErrorInfo& error);

  /// Get span's context for propagating across process boundaries
  virtual const ProgressContext& GetContext() const noexcept = 0;

  /// Primarily for internal class use only
  void MarkScopeClosed();
  virtual bool ScopeClosed();

  /// Every ProgressSpan started must be finished
  ///
  /// If the ProgressScope for this ProgressSpan was created with
  /// finish_on_close=true then this will be called via the
  /// ProgressScope’s RAII if Close() has not already been called
  ///
  /// If there is an unclosed ProgressSpan at the end of
  /// execution then a warning will be given
  /// Note that this immediately finishes the span regardless of it
  /// having unfinished children ProgressSpans
  void Finish();

private:
  ProgressSpan* parent_ = nullptr;
  bool finished_ = false;
  bool scope_closed_ = false;
};

}  // namespace katana

#endif
