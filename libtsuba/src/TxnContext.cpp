#include "katana/TxnContext.h"

#include "GlobalState.h"
#include "katana/file.h"

katana::Result<void>
katana::TxnContext::Commit() const {
  if (!manifest_cached_ || manifest_file_.empty()) {
    return katana::ResultSuccess();
  }

  katana::Result<void> ret = katana::OneHostOnly([&]() -> katana::Result<void> {
    std::string curr_s = rdg_manifest_.ToJsonString();
    KATANA_CHECKED_CONTEXT(
        katana::FileStore(
            manifest_file_.string(),
            reinterpret_cast<const uint8_t*>(curr_s.data()), curr_s.size()),
        "CommitRDG future failed {}", manifest_file_);
    return katana::ResultSuccess();
  });
  return ret;
}
