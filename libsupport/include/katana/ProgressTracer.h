#ifndef KATANA_LIBSUPPORT_KATANA_PROGRESSTRACER_H_
#define KATANA_LIBSUPPORT_KATANA_PROGRESSTRACER_H_

#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "katana/Result.h"
#include "katana/config.h"

/// Tracers do not currently support thread-local tracers or concurrency controls.
/// Starting/Finishing span functions should only be used in a single-threaded context.
/// However, logging and tagging existing spans are thread-safe.
///
/// The tracing classes are based on the OpenTracing
/// specifications, which can be found here: https://opentracing.io/docs/overview/what-is-tracing/
///
/// Spans are units of work with a defined start and stop point. Spans may have
/// associated log messages as well as tags.
///
/// A scope owns a span for its lifetime, and it closes the span when it goes
/// out of scope or the developer calls Close on it. Whenever possible scopes
/// should be used instead of raw spans
///
/// Tracers are used to control span logic. They create spans and maintain
/// the active span for ease of use for the developer.
///
/// Contexts are typically used to pass spans across process/thread boundaries
///
/// Best Practices:
///
/// - If possible, always avoid creating raw ProgressSpans from ProgressTracer
///
/// - If possible, always use ProgressScopes to handle ProgressSpans
///
/// - Only use 1 ProgressTracer in an execution. They should be created at entry points.
///
/// - Use ProgressScope's RAII to handle early returns (e.g., due to errors)
///
/// - Raw ProgressSpans should be used for special scenarios like tracing asynchronous calls
///
/// Notes:
///
/// SharedMemSys and DistMemSys will initialize the global ProgressTracer to a
/// JsonTracer. Later, it will be set to a no-op tracer. On Fini or the
/// destructor, SharedMemSys or DistMemSys will call Close on the global
/// ProgressTracer. This prevents GetProgressTracer() from returning nullptr
/// and ensures tracers are closed
///
/// ProgressScopes will only close their ProgressSpan when their ProgressSpan
/// is the active span. This means that if a user exclusively uses
/// ProgressScopes, then parent ProgressSpans will not be closed until their
/// child ProgressSpan's close.
///
/// For example, in the following,
///
///     {
///        auto scope1 = tracer.StartActiveSpan("1");
///        auto scope2 = tracer.StartActiveSpan("2");
///        if (err) { return; }
///        scope2.Close();
///        auto scope3 = tracer.StartActiveSpan("3");
///     }
///
/// Scope2's ProgressSpan will finish before scope1's ProgressSpan. If there is
/// no error, scope3's ProgressSpan will finish before scope1's ProgressSpan.
///
/// \file

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

struct HostStats {
  long nprocs{};
  long ram_gb{};
  std::string hostname;
  long pid{};
};

class ProgressScope;
class ProgressSpan;
class ProgressContext;

class KATANA_EXPORT ProgressTracer {
public:
  virtual ~ProgressTracer() = default;
  ProgressTracer(const ProgressTracer&) = delete;
  ProgressTracer(ProgressTracer&&) = delete;
  ProgressTracer& operator=(const ProgressTracer&) = delete;
  ProgressTracer& operator=(ProgressTracer&&) = delete;

  static ProgressTracer& Get() { return *tracer_; }
  static void Set(std::unique_ptr<ProgressTracer> tracer);

  static uint64_t ParseProcSelfRssBytes();
  static HostStats GetHostStats();
  static long GetMaxMem();
  static std::string GetValue(const Value& value);

  /// StartActiveSpan creates a new span. If there is not an active span,
  /// create a new top-level span. Otherwise, this function creates a child
  /// span of the active span. The returned scope will finish
  /// the span on close
  ProgressScope StartActiveSpan(const std::string& span_name);
  ProgressScope StartActiveSpan(
      const std::string& span_name, const ProgressContext& child_of);

  /// StartSpan creates a new span which is the child of the given span, but
  /// unlike StartActiveSpan, StartSpan does not change the active span. This
  /// method is used to create multiple active spans simultaneously.
  virtual std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, const ProgressContext& child_of) = 0;

  /// FinishActiveSpan finishes the active span and the parent of the erstwhile
  /// active span becomes the active span.
  ///
  /// The method is primarily for internal use only. Most users should use
  /// ProgressSpan::Finish().
  void FinishActiveSpan();

  /// Inject passes a span from one process or host to another. The return value
  /// of Inject should be passed to Extract.
  ///
  /// These functions are primarily for interest use only and they will be
  /// replaced in the future.
  virtual std::string Inject(const ProgressContext& ctx) = 0;

  // Extract receives context information from Inject. It returns a nullptr on
  // failure.
  virtual std::unique_ptr<ProgressContext> Extract(
      const std::string& carrier) = 0;

  /// GetActiveSpan returns the current scope’s span.
  ///
  /// If there is no active span, return an unnamed root span of a new trace.
  /// is created (in this case the program is probably not using tracing).
  virtual ProgressSpan& GetActiveSpan();
  bool HasActiveSpan() { return active_span_ != nullptr; }
  uint32_t GetHostID() const { return host_id_; }
  uint32_t GetNumHosts() const { return num_hosts_; }

  /// Finish closes the active span and its parent spans if present and flushes any
  /// buffered trace information.
  ///
  /// Finish resets the active span to the unnamed root span.
  void Finish();

protected:
  ProgressTracer(uint32_t host_id, uint32_t num_hosts)
      : host_id_(host_id), num_hosts_(num_hosts) {}

private:
  virtual std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, std::shared_ptr<ProgressSpan> child_of) = 0;
  ProgressScope SetActiveSpan(std::shared_ptr<ProgressSpan> span);

  /// Close flushes any buffered spans
  virtual void Close() = 0;

  static std::unique_ptr<ProgressTracer> tracer_;

  std::shared_ptr<ProgressSpan> active_span_ = nullptr;
  uint32_t host_id_;
  uint32_t num_hosts_;
  std::shared_ptr<ProgressSpan> default_active_span_ = nullptr;
};

KATANA_EXPORT ProgressTracer& GetTracer();

class KATANA_EXPORT [[nodiscard]] ProgressScope {
public:
  ~ProgressScope();
  ProgressScope(const ProgressScope&) = delete;
  ProgressScope(ProgressScope&&) = delete;
  ProgressScope& operator=(const ProgressScope&) = delete;
  ProgressScope& operator=(ProgressScope&&) = delete;

  /// span returns the span of the scope. Spans may have tags and logs
  /// associated with them.
  ProgressSpan& span() { return *span_; }

  /// Close marks the underlying ProgressSpan as complete. This method will
  /// only finish the underlying ProgressSpan when all of its active children
  /// ProgressSpans have been finished.
  ///
  /// This is called by ~ProgressScope if not called explicitly.
  void Close();

private:
  friend class ProgressTracer;

  ProgressScope(std::shared_ptr<ProgressSpan> span) : span_(std::move(span)) {}

  std::shared_ptr<ProgressSpan> span_;
};

class KATANA_EXPORT ProgressContext {
public:
  virtual ~ProgressContext() = default;
  virtual std::unique_ptr<ProgressContext> Clone() const noexcept = 0;
  virtual std::string GetTraceID() const noexcept;
  virtual std::string GetSpanID() const noexcept;
};

class KATANA_EXPORT ProgressSpan {
public:
  /// If Finish has not already been called for the ProgressSpan, it's
  /// destructor must do so.
  virtual ~ProgressSpan() = default;
  ProgressSpan(const ProgressSpan&) = delete;
  ProgressSpan(ProgressSpan&&) = delete;
  ProgressSpan& operator=(const ProgressSpan&) = delete;
  ProgressSpan& operator=(ProgressSpan&&) = delete;

  /// SetTag adds a tag to the span.
  virtual void SetTags(const Tags& tags) = 0;
  void SetError() { SetTags({{"error", true}}); }

  /// Log attaches a message with standard metrics and optional tags.
  ///
  /// The current standard metrics are max_mem, mem, host, and timestamp.
  virtual void Log(const std::string& message, const Tags& tags) = 0;
  void Log(const std::string& message) { Log(message, {}); }
  void LogError(const std::string& message) {
    Log(message, {{"event", "error"}});
  }
  void LogError(const std::string& message, const ErrorInfo& error);

  /// LogProfile optionally attaches detailed memory profiling information.
  ///
  /// This is a noop unless KATANA_USE_JEMALLOC is enabled and the environment
  /// variable MALLOC_CONF contains prof:true. See
  /// docs/contributing/performance.rst for more details.
  void LogProfile();

  /// GetContext returns the a context that can be used with
  /// ProgressScope::Inject.
  virtual const ProgressContext& GetContext() const noexcept = 0;

  /// Primarily for internal class use only
  void MarkScopeClosed();
  virtual bool ScopeClosed();
  bool IsFinished() const { return finished_; }
  const std::shared_ptr<ProgressSpan>& GetParentSpan() { return parent_; }

  /// Finish the ProgressSpan.
  ///
  /// Every ProgressSpan that has been created must be finished.
  ///
  /// Note that Finish immediately finishes the span even if it has unfinished
  /// child spans.
  ///
  /// This is called by ~ProgressSpan if not called explicitly.
  ///
  /// If there is an unclosed ProgressSpan at the end of execution then a
  /// warning is printed.
  void Finish();

protected:
  ProgressSpan(std::shared_ptr<ProgressSpan> parent)
      : parent_(std::move(parent)) {}

private:
  virtual void Close() = 0;

  std::shared_ptr<ProgressSpan> parent_ = nullptr;
  bool finished_ = false;
  bool scope_closed_ = false;
};

}  // namespace katana

#endif
