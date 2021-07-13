#include "katana/JSONTracer.h"

katana::Result<void>
CreateError() {
  katana::ProgressTracer::GetProgressTracer().StartActiveSpan("getting error");
  return KATANA_ERROR(
      katana::ErrorCode::ArrowError, "failed to make fixed size type");
}

katana::Result<void>
GetError() {
  katana::ProgressTracer::GetProgressTracer().StartActiveSpan("passing error");
  return CreateError().error().WithContext("passed along by GetError");
}

int
main() {
  katana::ProgressTracer::SetProgressTracer(katana::JSONTracer::Make());
  auto& tracer = katana::ProgressTracer::GetProgressTracer();
  auto scope = tracer.StartActiveSpan("first span");
  scope.span().SetTags(
      {{"life", static_cast<uint32_t>(42)},
       {"type", "test"},
       {"real", false},
       {"somethin", 2.0},
       {"hello", std::string{"world"}}});

  {
    auto list_scope = tracer.StartActiveSpan("reading query list");
    list_scope.span().Log("query parsed", {{"query", "CREATE"}, {"ops", 4}});

    for (auto i = 0; i < 2; i++) {
      auto query_scope = tracer.StartActiveSpan("running query");
      auto writing_scope = tracer.StartActiveSpan("writing query results");
      writing_scope.span().LogError("error writing", GetError().error());
      writing_scope.span().SetError();
    }
    auto span = tracer.StartSpan("is not initially active");
    auto span_scope = tracer.SetActiveSpan(span);
    auto no_raii_scope = tracer.StartActiveSpan("no raii", false);
  }
  tracer.GetActiveSpan()->Log("no raii is the active span");
  tracer.GetActiveSpan()->Finish();

  scope.Close();
  auto scope2 = tracer.StartActiveSpan("first span of second trace", false);
  std::string carrier = tracer.Inject(scope2.span().GetContext());
  std::unique_ptr<katana::ProgressContext> ctx = tracer.Extract(carrier);
  scope2.span().Log(
      "testing contexts",
      {{"trace_id", ctx->GetTraceID()}, {"span_id", ctx->GetSpanID()}});
  scope2.Close();  // this does nothing since open_on_close was set to false

  auto root_span = tracer.StartSpan("root span of new trace", true);
  auto scope3 = tracer.SetActiveSpan(root_span);
  tracer.GetActiveSpan()->Log("the new root span of trace 3 is active");

  auto scope2_child =
      tracer.StartActiveSpan("child of trace 2's root by context", *ctx);
  tracer.GetActiveSpan()->Log("child span of trace 2 is active");
  scope2_child.Close();
  root_span->Finish();
  scope2.span().Finish();
}
