#ifndef KATANA_LIBTSUBA_KATANA_RDGOPTIONALDATASTRUCTURE_H_
#define KATANA_LIBTSUBA_KATANA_RDGOPTIONALDATASTRUCTURE_H_

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "katana/AtomicWrapper.h"
#include "katana/DynamicBitset.h"
#include "katana/ErrorCode.h"
#include "katana/FileView.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/NUMAArray.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/WriteGroup.h"
#include "katana/config.h"
#include "katana/tsuba.h"

namespace katana {

/// Base class for all optional datastructures.
/// Paths to the RDGOptionalDatastructure files are stored in the RDGPartHeader::optional_datastructure_manifests_
class KATANA_EXPORT RDGOptionalDatastructure {
public:
  std::map<std::string, std::string> paths() const { return paths_; }
  void set_paths(std::map<std::string, std::string> paths) { paths_ = paths; }

  /// Load() should make the Optional Data structure fully available to the caller,
  /// Mapping any additional files defined in paths_ into memory as necessary
  static katana::Result<RDGOptionalDatastructure> Load(
      const katana::URI& rdg_dir_path, const std::string& path) = delete;

  /// Write() should immediately persist the optional data structure to disk
  /// Users of RDGOptionalDatastructures should ensure Write() is called before RDG::Store()
  katana::Result<std::string> Write(katana::URI rdg_dir_path) = delete;

  static katana::Result<void> ChangeStorageLocation(
      const std::string& manifest_relpath, const katana::URI& old_loc,
      const katana::URI& new_loc) {
    katana::URI old_manifest_path = old_loc.Join(manifest_relpath);
    katana::FileView fv;
    KATANA_CHECKED(fv.Bind(old_manifest_path.string(), true));
    RDGOptionalDatastructure data;
    KATANA_CHECKED(katana::JsonParse<RDGOptionalDatastructure>(fv, &data));

    // copy over any extra files the optional datastructure relies on
    // Assumes that all OptionalDatastructures properly extend the RDGOptionalDatastructure class
    for (const auto& file : data.paths_) {
      katana::FileView fvtmp;
      katana::URI old_path = old_loc.Join(file.second);
      KATANA_CHECKED(fvtmp.Bind(old_path.string(), true));
      katana::URI new_path = new_loc.Join(file.second);
      KATANA_CHECKED(katana::FileStore(
          new_path.string(), fvtmp.ptr<uint8_t>(), fvtmp.size()));
      KATANA_CHECKED(fvtmp.Unbind());
    }

    // copy out the manifest itself
    katana::URI new_manifest_path = new_loc.Join(manifest_relpath);
    KATANA_CHECKED(katana::FileStore(
        new_manifest_path.string(), fv.ptr<uint8_t>(), fv.size()));
    KATANA_CHECKED(fv.Unbind());

    return katana::ResultSuccess();
  }

  friend void to_json(nlohmann::json& j, const RDGOptionalDatastructure& data);
  friend void from_json(
      const nlohmann::json& j, RDGOptionalDatastructure& data);

protected:
  // map of exta files this optional datastructure will load
  // { "file_name" : "rdg-relative_path" }
  // track these so that when we move the RDG, we also move these extra files
  std::map<std::string, std::string> paths_;
};

}  // namespace katana

#endif
