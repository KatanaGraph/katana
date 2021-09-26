#include "katana/TextTracer.h"

katana::Result<void>
CreateError() {
  auto scope = katana::GetTracer().StartActiveSpan("getting error");
  return KATANA_ERROR(
      katana::ErrorCode::ArrowError, "failed to make fixed size type");
}

katana::Result<void>
GetError() {
  auto suppressor = katana::GetTracer().SuppressTracer();
  auto scope = katana::GetTracer().StartActiveSpan("passing error");
  return CreateError().error().WithContext("passed along by GetError");
}

int
main() {
  katana::ProgressTracer::Set(katana::TextTracer::Make());
  auto& tracer = katana::GetTracer();
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
    auto span = tracer.StartSpan(
        "is not initially active", tracer.GetActiveSpan().GetContext());
  }
  scope.Close();

  auto scope2 = tracer.StartActiveSpan("first span of second trace");
  std::string carrier = tracer.Inject(scope2.span().GetContext());
  std::unique_ptr<katana::ProgressContext> ctx = tracer.Extract(carrier);
  scope2.span().Log(
      "testing contexts",
      {{"trace_id", ctx->GetTraceID()}, {"span_id", ctx->GetSpanID()}});
  auto scope2_child =
      tracer.StartActiveSpan("child of trace 2's root by context", *ctx);
  tracer.GetActiveSpan().Log("child span of trace 2 is active");
  scope2_child.Close();
  scope2.Close();

  auto scope3 = tracer.StartActiveSpan("root span of third trace");
  tracer.GetActiveSpan().Log("the new root span of trace 3 is active");
}
