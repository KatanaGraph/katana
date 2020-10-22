#include "galois/GetEnv.h"

#include "galois/Logging.h"

int
main() {
  GALOIS_LOG_ASSERT(galois::GetEnv("PATH"));

  std::string s;
  GALOIS_LOG_ASSERT(galois::GetEnv("PATH", &s));

  int i{};
  GALOIS_LOG_ASSERT(!galois::GetEnv("PATH", &i));

  double d{};
  GALOIS_LOG_ASSERT(!galois::GetEnv("PATH", &d));

  bool b{};
  GALOIS_LOG_ASSERT(!galois::GetEnv("PATH", &b));

  std::string new_val{"arf"};
  GALOIS_LOG_ASSERT(galois::SetEnv("PATH", new_val, false));
  GALOIS_LOG_ASSERT(galois::SetEnv("NOWAYTHISEXISTS", new_val, false));
  GALOIS_LOG_ASSERT(galois::SetEnv("NOWAYTHISEXISTS", new_val, true));
  GALOIS_LOG_ASSERT(galois::SetEnv("NOWAYTHISEXISTS", new_val, false));
  GALOIS_LOG_ASSERT(galois::GetEnv("NOWAYTHISEXISTS", &s));
  GALOIS_LOG_ASSERT(s == new_val);

  return 0;
}
