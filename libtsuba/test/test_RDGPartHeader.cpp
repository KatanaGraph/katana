#include "RDGPartHeader.h"
#include "katana/Uri.h"
#include "tsuba/tsuba.h"
int
main(int argc, char** md_paths) {
  if (argc < 2) {
    KATANA_LOG_FATAL("usage: {} MD_PATHS...", md_paths[0]);
  }
  if (auto res = tsuba::Init(); !res) {
    KATANA_LOG_FATAL("tsuba::Init: {}", res.error());
  }
  for (int i = 1; i < argc; i++) {
    char* md_path = md_paths[i];
    auto header_uri_res = katana::Uri::Make(md_path);
    katana::Uri header_uri_Val = header_uri_res.value();
    if (auto header = tsuba::RDGPartHeader::Make(header_uri_Val); !header) {
      KATANA_LOG_FATAL("Failed to load RDGPartHeader: {}", md_path);
    }
  }
  return 0;
}
