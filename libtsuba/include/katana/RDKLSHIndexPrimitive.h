#ifndef KATANA_LIBTSUBA_KATANA_RDKLSHINDEXPRIMITIVE_H_
#define KATANA_LIBTSUBA_KATANA_RDKLSHINDEXPRIMITIVE_H_

#include <cstdint>
#include <vector>

#include "katana/AtomicWrapper.h"
#include "katana/DynamicBitset.h"
#include "katana/ErrorCode.h"
#include "katana/FileView.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/NUMAArray.h"
#include "katana/RDGOptionalDatastructure.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/WriteGroup.h"
#include "katana/config.h"
#include "katana/tsuba.h"

namespace katana {

const std::string kOptionalDatastructureRDKLSHIndexPrimitive =
    "kg.v1.rdk_lsh_index";
const std::string kOptionalDatastructureRDKLSHIndexPrimitiveFilename =
    "rdk_lsh_index_manifest";

class KATANA_EXPORT RDKLSHIndexPrimitive
    : private katana::RDGOptionalDatastructure {
public:
  static katana::Result<RDKLSHIndexPrimitive> Load(
      const katana::URI& rdg_dir_path, const std::string& path) {
    RDKLSHIndexPrimitive index =
        KATANA_CHECKED(LoadJson(rdg_dir_path.Join(path)));
    return index;
  }

  katana::Result<std::string> Write(katana::URI rdg_dir_path) {
    // Write out our json manifest
    katana::URI manifest_path = rdg_dir_path.RandFile(
        kOptionalDatastructureRDKLSHIndexPrimitiveFilename);
    KATANA_CHECKED(WriteManifest(manifest_path));
    return manifest_path.BaseName();
  }

  uint64_t num_hashes_per_bucket() const { return num_hashes_per_bucket_; }
  void set_num_hashes_per_bucket(uint64_t num) { num_hashes_per_bucket_ = num; }

  uint64_t num_buckets() const { return num_buckets_; }
  void set_num_buckets(const uint64_t num) { num_buckets_ = num; }

  uint64_t fingerprint_length() const { return fingerprint_length_; }
  void set_fingerprint_length(const uint64_t len) { fingerprint_length_ = len; }

  size_t num_fingerprints() const { return num_fingerprints_; }
  void set_num_fingerprints(const size_t num) { num_fingerprints_ = num; }

  std::vector<std::map<uint64_t, std::vector<uint64_t>>>& hash_structure() {
    return hash_structure_;
  }
  void set_hash_structure(
      std::vector<std::map<uint64_t, std::vector<uint64_t>>> hash_struct) {
    hash_structure_ = std::move(hash_struct);
  }

  std::vector<katana::DynamicBitset>& fingerprints() { return fingerprints_; }
  void set_fingerprints(std::vector<katana::DynamicBitset> prints) {
    fingerprints_ = std::move(prints);
  }

  std::vector<std::string> smiles() { return smiles_; }
  void set_smiles(std::vector<std::string> smiles) {
    smiles_ = std::move(smiles);
  }

  friend void to_json(nlohmann::json& j, const RDKLSHIndexPrimitive& index);
  friend void from_json(const nlohmann::json& j, RDKLSHIndexPrimitive& index);

private:
  uint64_t num_hashes_per_bucket_;
  uint64_t num_buckets_;
  uint64_t fingerprint_length_;
  size_t num_fingerprints_;

  std::vector<std::string> smiles_;

  /// data structures dumped to their own files

  std::vector<std::map<uint64_t, std::vector<uint64_t>>> hash_structure_;

  // Array of fingerprint bitsets indexed on num_fingerprints_
  std::vector<katana::DynamicBitset> fingerprints_;

  static katana::Result<RDKLSHIndexPrimitive> LoadJson(const URI& path) {
    katana::FileView fv;
    KATANA_CHECKED(fv.Bind(path, true));

    if (fv.size() == 0) {
      return RDKLSHIndexPrimitive();
    }

    RDKLSHIndexPrimitive index;
    KATANA_CHECKED(katana::JsonParse<RDKLSHIndexPrimitive>(fv, &index));

    return index;
  }

  katana::Result<void> WriteManifest(const URI& path) const {
    std::string serialized = KATANA_CHECKED(katana::JsonDump(*this));
    // POSIX files end with newlines
    serialized = serialized + "\n";

    auto ff = std::make_unique<katana::FileFrame>();
    KATANA_CHECKED(ff->Init(serialized.size()));
    if (auto res = ff->Write(serialized.data(), serialized.size()); !res.ok()) {
      return KATANA_ERROR(
          katana::ArrowToKatana(res.code()), "arrow error: {}", res);
    }
    ff->Bind(path);
    // persist now
    KATANA_CHECKED(ff->Persist());

    return katana::ResultSuccess();
  }
};

}  // namespace katana

#endif
