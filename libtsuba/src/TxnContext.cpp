#include "katana/TxnContext.h"

#include "GlobalState.h"
#include "katana/file.h"

katana::Result<void>
katana::TxnContext::Commit() {
  for (auto info : manifest_info_) {
    URI rdg_dir = info.first;
    if (!manifest_uptodate_.at(rdg_dir)) {
      URI manifest_file = info.second.manifest_file;
      KATANA_LOG_DEBUG_ASSERT(!manifest_file.empty());
      KATANA_CHECKED(katana::OneHostOnly([&]() -> katana::Result<void> {
        std::string curr_s = info.second.rdg_manifest.ToJsonString();
        KATANA_CHECKED_CONTEXT(
            katana::FileStore(manifest_file, curr_s),
            "CommitRDG future failed {}", manifest_file);
        return katana::ResultSuccess();
      }));

      manifest_uptodate_[rdg_dir] = true;
    }
  }
  return katana::ResultSuccess();
}
