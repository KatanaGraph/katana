#ifndef KATANA_LIBTSUBA_RDGMETA_H_
#define KATANA_LIBTSUBA_RDGMETA_H_

#include <cstdint>
#include <regex>
#include <set>

#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/Uri.h"
#include "katana/config.h"
#include "tsuba/NameServerClient.h"
#include "tsuba/RDGLineage.h"
#include "tsuba/tsuba.h"

namespace tsuba {

// Struct version of main graph metadatafile
class KATANA_EXPORT RDGMeta {
  // meta files look like this: `meta_N` where `N` is the version number
  static const std::regex kMetaVersion;

  katana::Uri dir_;  // not persisted; inferred from name

  //
  // Persisted
  //
  uint64_t version_{0};
  uint64_t previous_version_{0};
  uint32_t num_hosts_{0};  // 0 is a reserved value for the empty RDG when
  // tsuba views policy_id as zero (not partitioned) or not zero (partitioned
  // according to a CuSP-specific policy)
  uint32_t policy_id_{0};
  bool transpose_{false};
  RDGLineage lineage_;

  RDGMeta(katana::Uri dir) : dir_(std::move(dir)) {}

  RDGMeta(
      uint64_t version, uint64_t previous_version, uint32_t num_hosts,
      uint32_t policy_id, bool transpose, katana::Uri dir, RDGLineage lineage)
      : dir_(std::move(dir)),
        version_(version),
        previous_version_(previous_version),
        num_hosts_(num_hosts),
        policy_id_(policy_id),
        transpose_(transpose),
        lineage_(std::move(lineage)) {}

  static katana::Result<RDGMeta> MakeFromStorage(const katana::Uri& uri);

  static std::string PartitionFileName(uint32_t node_id, uint64_t version);

public:
  RDGMeta() = default;

  RDGMeta NextVersion(
      uint32_t num_hosts, uint32_t policy_id, bool transpose,
      const RDGLineage& lineage) const {
    return RDGMeta(
        version_ + 1, version_, num_hosts, policy_id, transpose, dir_, lineage);
  }

  bool IsEmptyRDG() const { return num_hosts() == 0; }

  static katana::Result<RDGMeta> Make(RDGHandle handle);

  /// Create an RDGMeta
  /// \param uri a uri that either names a registered RDG or an explicit
  ///    rdg file
  /// \returns the constructed RDGMeta and the directory of its contents
  static katana::Result<RDGMeta> Make(const katana::Uri& uri);

  /// Create an RDGMeta
  /// \param uri is a storage prefix where the RDG is stored
  /// \param version is the version of the meta_dir to load
  /// \returns the constructed RDGMeta and the directory of its contents
  static katana::Result<RDGMeta> Make(const katana::Uri& uri, uint64_t version);

  const katana::Uri& dir() const { return dir_; }
  uint64_t version() const { return version_; }
  uint32_t num_hosts() const { return num_hosts_; }
  uint32_t policy_id() const { return policy_id_; }
  uint64_t previous_version() const { return previous_version_; }
  bool transpose() const { return transpose_; }

  void set_dir(katana::Uri dir) { dir_ = std::move(dir); }

  katana::Uri PartitionFileName(uint32_t host_id) const;

  katana::Uri FileName() { return FileName(dir_, version_); }

  // Canonical naming
  static katana::Uri FileName(const katana::Uri& uri, uint64_t version);

  static katana::Uri PartitionFileName(
      const katana::Uri& uri, uint32_t node_id, uint64_t version);

  static katana::Result<uint64_t> ParseVersionFromName(const std::string& file);

  static bool IsMetaUri(const katana::Uri& uri);

  std::string ToJsonString() const;

  /// Return the set of file names that hold this RDG's data by reading partition files
  /// Useful to garbage collect unused files
  katana::Result<std::set<std::string>> FileNames();

  // Required by nlohmann
  friend void to_json(nlohmann::json& j, const RDGMeta& meta);
  friend void from_json(const nlohmann::json& j, RDGMeta& meta);
};

KATANA_EXPORT void to_json(nlohmann::json& j, const RDGMeta& meta);
KATANA_EXPORT void from_json(const nlohmann::json& j, RDGMeta& meta);

}  // namespace tsuba

#endif
