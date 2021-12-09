#include <benchmark/benchmark.h>

#include "katana/Galois.h"
#include "katana/Logging.h"
#include "katana/Range.h"
#include "katana/Reduction.h"
#include "katana/Threads.h"
#include "katana/Traits.h"

namespace {

void
MakeArguments(benchmark::internal::Benchmark* b) {
  for (long size : {1024, 64 * 1024, 1024 * 1024}) {
    b->Args({size});
  }
}

std::vector<int>
MakeInput(long size) {
  std::vector<int> ret;

  ret.resize(size);
  for (long i = 0; i < size; ++i) {
    ret[i] = i;
  }

  return ret;
}

std::vector<int>
MakeOutput(long size) {
  std::vector<int> ret;

  ret.resize(size);

  return ret;
}

void
VerifyOutput(const std::vector<int>& output) {
  for (int i = 0, n = output.size(); i < n; ++i) {
    KATANA_LOG_VASSERT(
        output[i] == i + 1, "at index {}: {} != {}", i, output[i], i + 1);
  }
}

void
RunStdForEach(const std::vector<int>& input, std::vector<int>& output) {
  std::for_each(
      input.begin(), input.end(), [&](int i) { output[i] = input[i] + 1; });
}

void
StdForEach(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);
  auto output = MakeOutput(size);

  for (auto _ : state) {
    RunStdForEach(input, output);
  }

  VerifyOutput(output);
  state.SetItemsProcessed(state.iterations() * size);
}

void
RunDoAll(const std::vector<int>& input, std::vector<int>& output) {
  katana::do_all(
      katana::MakeStandardRange(input.begin(), input.end()),
      [&](int i) { output[i] = input[i] + 1; });
}

void
SerialDoAll(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);
  auto output = MakeOutput(size);

  katana::setActiveThreads(1);

  for (auto _ : state) {
    RunDoAll(input, output);
  }

  VerifyOutput(output);
  state.SetItemsProcessed(state.iterations() * size);
}

void
DoAll(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);
  auto output = MakeOutput(size);

  katana::setActiveThreads(4);

  for (auto _ : state) {
    RunDoAll(input, output);
  }

  VerifyOutput(output);
  state.SetItemsProcessed(state.iterations() * size);
}

void
RunForEach(const std::vector<int>& input, std::vector<int>& output) {
  katana::for_each(
      katana::MakeStandardRange(input.begin(), input.end()),
      [&](int i, auto&) { output[i] = input[i] + 1; },
      katana::disable_conflict_detection(), katana::no_stats(),
      katana::no_pushes());
}

void
SerialForEach(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);
  auto output = MakeOutput(size);

  katana::setActiveThreads(1);

  for (auto _ : state) {
    RunForEach(input, output);
  }

  VerifyOutput(output);
  state.SetItemsProcessed(state.iterations() * size);
}

void
ForEach(benchmark::State& state) {
  long size = state.range(0);
  auto input = MakeInput(size);
  auto output = MakeOutput(size);

  katana::setActiveThreads(4);

  for (auto _ : state) {
    RunForEach(input, output);
  }

  VerifyOutput(output);
  state.SetItemsProcessed(state.iterations() * size);
}

BENCHMARK(StdForEach)->Apply(MakeArguments);
BENCHMARK(DoAll)->Apply(MakeArguments);
BENCHMARK(SerialDoAll)->Apply(MakeArguments);
BENCHMARK(ForEach)->Apply(MakeArguments);
BENCHMARK(SerialForEach)->Apply(MakeArguments);
}  // namespace

int
main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  katana::GaloisRuntime G;
  ::benchmark::RunSpecifiedBenchmarks();
}
