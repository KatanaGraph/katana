#include "katana/ErrorCode.h"
#include "katana/Galois.h"
#include "katana/PerThreadStorage.h"
#include "katana/Result.h"

template <typename T>
void
TestConstructorDoesNotConflictWithResultConstruction() {
  auto f1 = []() -> katana::Result<T> {
    T ret;

    return ret;
  };

  auto f2 = []() -> katana::Result<T> { return katana::ErrorCode::NotFound; };

  (void)f1;
  (void)f2;
}

int
main() {
  katana::GaloisRuntime sys;

  TestConstructorDoesNotConflictWithResultConstruction<
      katana::PerThreadStorage<int>>();

  TestConstructorDoesNotConflictWithResultConstruction<
      katana::PerSocketStorage<int>>();

  return 0;
}
