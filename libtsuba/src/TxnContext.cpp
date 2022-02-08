#include "katana/TxnContext.h"

#include "GlobalState.h"
#include "katana/file.h"

katana::Result<void>
katana::TxnContext::Commit() {
  if (!manifest_cached_ || manifest_uptodate_) {
    return katana::ResultSuccess();
  }

  KATANA_LOG_DEBUG_ASSERT(!manifest_file_.empty());
  katana::Result<void> ret = katana::OneHostOnly([&]() -> katana::Result<void> {
    std::string curr_s = rdg_manifest_.ToJsonString();
    KATANA_CHECKED_CONTEXT(
        katana::FileStore(
            manifest_file_.string(),
            reinterpret_cast<const uint8_t*>(curr_s.data()), curr_s.size()),
        "CommitRDG future failed {}", manifest_file_);
    return katana::ResultSuccess();
  });

  manifest_uptodate_ = true;
  return ret;
}
