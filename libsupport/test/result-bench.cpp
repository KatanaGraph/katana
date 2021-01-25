#include <iostream>
#include <random>
#include <system_error>
#include <thread>

#include <benchmark/benchmark.h>
#include <boost/outcome/outcome.hpp>

#include "katana/Random.h"
#include "katana/Result.h"

namespace {

void
MakeArguments(benchmark::internal::Benchmark* b) {
  for (long num_threads : {1, 4}) {
    for (long size : {1024, 64 * 1024}) {
      for (long handle_ratio : {1, 3}) {
        for (long depth : {16, 32}) {
          for (long fail_delta : {-8, -1, 8}) {
            long fail_depth = depth + fail_delta;
            b->Args({num_threads, size, handle_ratio, depth, fail_depth});
          }
        }
      }
    }
  }
}

struct Random {
  Random() {
    int64_t max = katana::RandomUniformInt(std::numeric_limits<int64_t>::max());
    seed = max;
  }

  int64_t RandomInt(int64_t n) {
    if (n == 1) {
      return 0;
    }

    // This is not a good random number generator but to avoid being
    // bottlenecked on calls to std::uniform_int_distribution, we write
    // something wrong but inlinable here. The constant is from
    // std::minstd_rand.
    seed *= 48271;
    return seed % n;
  }

  bool ShouldStop(int depth, int max_depth) {
    if (depth >= max_depth) {
      return true;
    }

    int shift = max_depth - depth - 1;
    int64_t len = 1;
    len <<= shift;
    return RandomInt(len) == 0;
  }

  bool ShouldFail(int depth, int fail_depth) {
    return ShouldStop(depth, fail_depth);
  }

  uint64_t seed;
};

template <typename Scaffold>
void
Launch(int num_threads, int size, Scaffold& s) {
  if (num_threads == 1) {
    Random r;
    for (int i = 0; i < size; ++i) {
      s.Start(r);
    }
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      Random r;
      for (int i = 0; i < size; ++i) {
        s.Start(r);
      }
    });
  }

  for (std::thread& t : threads) {
    t.join();
  }
}

class ExceptionScaffold {
public:
  ExceptionScaffold(int handle_ratio, int max_depth, int fail_depth)
      : handle_ratio_(handle_ratio),
        max_depth_(max_depth),
        fail_depth_(fail_depth) {}

  int Go(Random& r, int depth) {
    int rv = r.RandomInt(handle_ratio_);

    if (r.ShouldStop(depth, max_depth_)) {
      return rv;
    }

    if (r.ShouldFail(depth, fail_depth_)) {
      throw depth;
    }

    if (rv) {
      return Go(r, depth + 1);
    }

    return GoWithHandler(r, depth + 1);
  }

  int GoWithHandler(Random& r, int depth) {
    int rv = r.RandomInt(handle_ratio_);

    if (r.ShouldStop(depth, max_depth_)) {
      return rv;
    }

    try {
      if (r.ShouldFail(depth, fail_depth_)) {
        throw depth;
      }

      if (rv) {
        return Go(r, depth + 1);
      }
      return GoWithHandler(r, depth + 1);
    } catch (const int& e) {
      return e;
    }

    // unreachable
    std::abort();
  }

  int Start(Random& r) { return GoWithHandler(r, 0); }

  int handle_ratio_;
  int max_depth_;
  int fail_depth_;
};

template <typename R, typename ErrorMaker>
class ResultScaffold {
public:
  ResultScaffold(int handle_ratio, int max_depth, int fail_depth)
      : handle_ratio_(handle_ratio),
        max_depth_(max_depth),
        fail_depth_(fail_depth) {}

  R Go(Random& r, int depth) {
    int rv = r.RandomInt(handle_ratio_);

    if (r.ShouldStop(depth, max_depth_)) {
      return rv;
    }

    if (r.ShouldFail(depth, fail_depth_)) {
      return error_maker();
    }

    if (rv) {
      return Go(r, depth + 1);
    }

    return GoWithHandler(r, depth + 1);
  }

  R GoWithHandler(Random& r, int depth) {
    int rv = r.RandomInt(handle_ratio_);

    if (r.ShouldStop(depth, max_depth_)) {
      return rv;
    }

    if (r.ShouldFail(depth, fail_depth_)) {
      if (depth == 0) {
        return rv;
      } else {
        return error_maker();
      }
    }

    if (rv) {
      if (auto res = Go(r, depth + 1); !res) {
        return rv;
      } else {
        return res.value();
      }
    }
    if (auto res = GoWithHandler(r, depth + 1); !res) {
      return rv;
    } else {
      return res.value();
    }
  }

  int Start(Random& r) { return GoWithHandler(r, 0).value(); }

  ErrorMaker error_maker;

  int handle_ratio_;
  int max_depth_;
  int fail_depth_;
};

class StringErrorInfo : public std::error_code {
public:
  StringErrorInfo() = default;

  template <
      typename ErrorEnum, typename U = std::enable_if_t<
                              std::is_error_code_enum_v<ErrorEnum> ||
                              std::is_error_condition_enum_v<ErrorEnum>>>
  StringErrorInfo(ErrorEnum&& e)
      : std::error_code(make_error_code(std::forward<ErrorEnum>(e))) {}

  StringErrorInfo WithContext(std::string message) {
    message_ = message;
    return *this;
  }

private:
  std::string message_;
};

void
ReturnException(benchmark::State& state) {
  ExceptionScaffold s(state.range(2), state.range(3), state.range(4));

  for (auto _ : state) {
    Launch(state.range(0), state.range(1), s);
  }
}

template <typename Result>
struct ErrorCodeMaker {
  Result operator()() const { return std::errc::invalid_argument; }
};

template <typename Result>
struct ErrorStringMaker {
  Result operator()() const {
    Result r = std::errc::invalid_argument;
    auto err = r.error();
    return err.WithContext(
        "a string longer than std::string's small string optimization");
  }
};

void
ReturnErrorCodeResult(benchmark::State& state) {
  using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<int>;

  ResultScaffold<Result, ErrorCodeMaker<Result>> s(
      state.range(2), state.range(3), state.range(4));

  for (auto _ : state) {
    Launch(state.range(0), state.range(1), s);
  }
}

void
ReturnStringResult(benchmark::State& state) {
  using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<int, StringErrorInfo>;

  ResultScaffold<Result, ErrorCodeMaker<Result>> s(
      state.range(2), state.range(3), state.range(4));

  for (auto _ : state) {
    Launch(state.range(0), state.range(1), s);
  }
}

void
ReturnStringResultWithContext(benchmark::State& state) {
  using Result = BOOST_OUTCOME_V2_NAMESPACE::std_result<int, StringErrorInfo>;

  ResultScaffold<Result, ErrorStringMaker<Result>> s(
      state.range(2), state.range(3), state.range(4));

  for (auto _ : state) {
    Launch(state.range(0), state.range(1), s);
  }
}

void
ReturnKatanaResult(benchmark::State& state) {
  using Result = katana::Result<int>;

  ResultScaffold<Result, ErrorCodeMaker<Result>> s(
      state.range(2), state.range(3), state.range(4));

  for (auto _ : state) {
    Launch(state.range(0), state.range(1), s);
  }
}

void
ReturnKatanaResultWithContext(benchmark::State& state) {
  using Result = katana::Result<int>;

  ResultScaffold<Result, ErrorStringMaker<Result>> s(
      state.range(2), state.range(3), state.range(4));

  for (auto _ : state) {
    Launch(state.range(0), state.range(1), s);
  }
}

BENCHMARK(ReturnException)->Apply(MakeArguments)->UseRealTime();
BENCHMARK(ReturnErrorCodeResult)->Apply(MakeArguments)->UseRealTime();
BENCHMARK(ReturnStringResult)->Apply(MakeArguments)->UseRealTime();
BENCHMARK(ReturnKatanaResult)->Apply(MakeArguments)->UseRealTime();
BENCHMARK(ReturnStringResultWithContext)->Apply(MakeArguments)->UseRealTime();
BENCHMARK(ReturnKatanaResultWithContext)->Apply(MakeArguments)->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
