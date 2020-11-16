#ifndef GALOIS_LIBTSUBA_TSUBA_RDGMETA_H_
#define GALOIS_LIBTSUBA_TSUBA_RDGMETA_H_

#include <cstdint>
#include <regex>

#include "galois/JSON.h"
#include "galois/Logging.h"
#include "galois/Uri.h"
#include "galois/config.h"
#include "tsuba/RDGLineage.h"

namespace tsuba {

// Struct version of main graph metadatafile
class GALOIS_EXPORT RDGMeta {
  // meta files look like this: `meta_N` where `N` is the version number
  static const std::regex kMetaVersion;

  galois::Uri dir_;  // not persisted; inferred from name

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

  RDGMeta(galois::Uri dir) : dir_(std::move(dir)) {}

  RDGMeta(
      uint64_t version, uint64_t previous_version, uint32_t num_hosts,
      uint32_t policy_id, bool transpose, galois::Uri dir, RDGLineage lineage)
      : dir_(std::move(dir)),
        version_(version),
        previous_version_(previous_version),
        num_hosts_(num_hosts),
        policy_id_(policy_id),
        transpose_(transpose),
        lineage_(std::move(lineage)) {}

  static galois::Result<RDGMeta> MakeFromStorage(const galois::Uri& uri);

  static std::string PartitionFileName(uint32_t node_id, uint64_t version);

public:
  RDGMeta() = default;

  RDGMeta NextVersion(
      uint32_t num_hosts, uint32_t policy_id, bool transpose,
      const RDGLineage& lineage) const {
    return RDGMeta(
        version_ + 1, version_, num_hosts, policy_id, transpose, dir_, lineage);
  }

  /// Create an RDGMeta
  /// \param uri a uri that either names a registered RDG or an explicit
  ///    rdg file
  /// \returns the constructed RDGMeta and the directory of its contents
  static galois::Result<RDGMeta> Make(const galois::Uri& uri);

  /// Create an RDGMeta
  /// \param uri is a storage prefix where the RDG is stored
  /// \param version is the version of the meta_dir to load
  /// \returns the constructed RDGMeta and the directory of its contents
  static galois::Result<RDGMeta> Make(const galois::Uri& uri, uint64_t version);

  const galois::Uri& dir() const { return dir_; }
  uint64_t version() const { return version_; }
  uint32_t num_hosts() const { return num_hosts_; }
  uint32_t policy_id() const { return policy_id_; }
  uint64_t previous_version() const { return previous_version_; }
  bool transpose() const { return transpose_; }

  void set_dir(galois::Uri dir) { dir_ = std::move(dir); }

  galois::Result<galois::Uri> PartitionFileName(
      bool intend_partial_read = false) const;

  // Canonical naming
  static galois::Uri FileName(const galois::Uri& uri, uint64_t version);

  static galois::Uri PartitionFileName(
      const galois::Uri& uri, uint32_t node_id, uint64_t version);

  static galois::Result<uint64_t> ParseVersionFromName(const std::string& file);

  static bool IsMetaUri(const galois::Uri& uri);

  std::string ToJsonString() const;

  // Required by nlohmann
  friend void to_json(nlohmann::json& j, const RDGMeta& meta);
  friend void from_json(const nlohmann::json& j, RDGMeta& meta);
};

void to_json(nlohmann::json& j, const RDGMeta& meta);
void from_json(const nlohmann::json& j, RDGMeta& meta);

}  // namespace tsuba

#endif
