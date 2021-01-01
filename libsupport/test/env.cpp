#include "katana/Env.h"

#include "katana/Logging.h"

int
main() {
  KATANA_LOG_ASSERT(katana::GetEnv("PATH"));

  std::string s;
  KATANA_LOG_ASSERT(katana::GetEnv("PATH", &s));

  int i{};
  KATANA_LOG_ASSERT(!katana::GetEnv("PATH", &i));

  double d{};
  KATANA_LOG_ASSERT(!katana::GetEnv("PATH", &d));

  bool b{};
  KATANA_LOG_ASSERT(!katana::GetEnv("PATH", &b));

  std::string new_val{"arf"};
  KATANA_LOG_ASSERT(katana::SetEnv("PATH", new_val, false));
  KATANA_LOG_ASSERT(katana::SetEnv("NOWAYTHISEXISTS", new_val, false));
  KATANA_LOG_ASSERT(katana::SetEnv("NOWAYTHISEXISTS", new_val, true));
  KATANA_LOG_ASSERT(katana::SetEnv("NOWAYTHISEXISTS", new_val, false));
  KATANA_LOG_ASSERT(katana::GetEnv("NOWAYTHISEXISTS", &s));
  KATANA_LOG_ASSERT(s == new_val);

  return 0;
}
