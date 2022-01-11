#include <algorithm>
#include <functional>
#include <iostream>

#include "katana/Galois.h"
#include "katana/ResultReduction.h"

katana::Result<void>
ErrorOnTrue(bool error) {
  if (error) {
    return KATANA_ERROR(katana::ErrorCode::NotFound, "error");
  }
  return katana::ResultSuccess();
}

katana::Result<bool>
ErrorOnTrue(bool error, bool return_value) {
  if (error) {
    return KATANA_ERROR(katana::ErrorCode::NotFound, "error");
  }
  return return_value;
}

void
TestVoidFunc() {
  katana::CombinedErrorInfo combined_error;
  katana::do_all(katana::iterate(size_t{0}, size_t{100}), [&](size_t i) {
    KATANA_COMBINE_ERROR(combined_error, ErrorOnTrue((i % 10) == 0));
    KATANA_LOG_ASSERT((i % 10) != 0);
  });
  KATANA_LOG_ASSERT(combined_error() != std::nullopt);

  combined_error.reset();
  katana::do_all(katana::iterate(size_t{0}, size_t{100}), [&](size_t) {
    KATANA_COMBINE_ERROR(combined_error, ErrorOnTrue(false));
  });
  KATANA_LOG_ASSERT(combined_error() == std::nullopt);
}

void
TestBoolFunc() {
  katana::CombinedErrorInfo combined_error;
  katana::do_all(katana::iterate(size_t{0}, size_t{100}), [&](size_t i) {
    bool ret =
        KATANA_COMBINE_ERROR(combined_error, ErrorOnTrue((i % 10) == 0, true));
    KATANA_LOG_ASSERT(ret = ((i % 10) != 0));
  });
  KATANA_LOG_ASSERT(combined_error() != std::nullopt);

  combined_error.reset();
  katana::do_all(katana::iterate(size_t{0}, size_t{100}), [&](size_t i) {
    bool ret =
        KATANA_COMBINE_ERROR(combined_error, ErrorOnTrue(false, (i % 10) == 0));
    KATANA_LOG_ASSERT(ret == ((i % 10) == 0));
  });
  KATANA_LOG_ASSERT(combined_error() == std::nullopt);
}

void
TestNoFunc() {
  katana::CombinedErrorInfo combined_error;
  katana::do_all(katana::iterate(size_t{0}, size_t{100}), [&](size_t i) {
    bool ret = true;
    if ((i % 10) == 0) {
      combined_error.update(KATANA_ERROR(katana::ErrorCode::NotFound, "error"));
      return;
    }
    KATANA_LOG_ASSERT(ret);
  });
  KATANA_LOG_ASSERT(combined_error() != std::nullopt);

  combined_error.reset();
  katana::do_all(katana::iterate(size_t{0}, size_t{100}), [&](size_t i) {
    bool ret = true;
    if ((i % 10) == 20) {
      combined_error.update(KATANA_ERROR(katana::ErrorCode::NotFound, "error"));
      return;
    }
    KATANA_LOG_ASSERT(ret);
  });
  KATANA_LOG_ASSERT(combined_error() == std::nullopt);
}

int
main() {
  katana::GaloisRuntime sys;
  katana::setActiveThreads(2);

  TestVoidFunc();
  TestBoolFunc();
  TestNoFunc();

  return 0;
}
