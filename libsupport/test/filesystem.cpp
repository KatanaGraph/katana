#include "galois/FileSystem.h"
#include "galois/Logging.h"

int main() {
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path").value() ==
                    "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path/").value() ==
                    "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path//").value() ==
                    "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path///").value() ==
                    "path");

  GALOIS_LOG_ASSERT(galois::ExtractDirName("/some/long/path").value() ==
                    "/some/long");
  GALOIS_LOG_ASSERT(galois::ExtractDirName("/some/long/path/").value() ==
                    "/some/long");
  GALOIS_LOG_ASSERT(galois::ExtractDirName("/some/long/path//").value() ==
                    "/some/long");
  GALOIS_LOG_ASSERT(galois::ExtractDirName("/some/long/path///").value() ==
                    "/some/long");

  return 0;
}
