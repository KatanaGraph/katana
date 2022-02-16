#ifndef KATANA_LIBTSUBA_KATANA_RDKSUBSTRUCTUREINDEXPRIMITIVE_H_
#define KATANA_LIBTSUBA_KATANA_RDKSUBSTRUCTUREINDEXPRIMITIVE_H_

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

const std::string kOptionalDatastructureRDKSubstructureIndexPrimitive =
    "kg.v1.rdk_substructure_index";
const std::string kOptionalDatastructureRDKSubstructureIndexPrimitiveFilename =
    "rdk_substructure_index_manifest";

class KATANA_EXPORT RDKSubstructureIndexPrimitive
    : private katana::RDGOptionalDatastructure {
public:
  static katana::Result<RDKSubstructureIndexPrimitive> Load(
      const katana::Uri& rdg_dir_path, const std::string& path) {
    RDKSubstructureIndexPrimitive substructure_index =
        KATANA_CHECKED(LoadJson(rdg_dir_path.Join(path).string()));
    return substructure_index;
  }

  katana::Result<std::string> Write(katana::Uri rdg_dir_path) {
    // Write out our json manifest
    katana::Uri manifest_path = rdg_dir_path.RandFile(
        kOptionalDatastructureRDKSubstructureIndexPrimitiveFilename);
    KATANA_CHECKED(WriteManifest(manifest_path.string()));
    return manifest_path.BaseName();
  }

  size_t fp_size() const { return fp_size_; }
  void set_fp_size(size_t size) { fp_size_ = size; }

  size_t num_entries() const { return num_entries_; }
  void set_num_entries(size_t num) { num_entries_ = num; }

  std::vector<std::vector<std::uint64_t>>& index() { return index_; }
  void set_index(std::vector<std::vector<std::uint64_t>> index) {
    index_ = std::move(index);
  }

  std::vector<katana::DynamicBitset>& fingerprints() { return fingerprints_; }
  void set_fingerprints(std::vector<katana::DynamicBitset> prints) {
    fingerprints_ = std::move(prints);
  }

  std::vector<std::string> smiles() { return smiles_; }
  void set_smiles(std::vector<std::string> smiles) {
    smiles_ = std::move(smiles);
  }

  friend void to_json(
      nlohmann::json& j, const RDKSubstructureIndexPrimitive& index);
  friend void from_json(
      const nlohmann::json& j, RDKSubstructureIndexPrimitive& index);

private:
  size_t fp_size_;

  size_t num_entries_;

  // Array of smiles strings indexed on num_entries
  std::vector<std::string> smiles_;

  // Array of fingerprint bitsets indexed on num_entries
  std::vector<katana::DynamicBitset> fingerprints_;

  // has size fp_size
  std::vector<std::vector<std::uint64_t>> index_;

  static katana::Result<RDKSubstructureIndexPrimitive> LoadJson(
      const std::string& path) {
    katana::FileView fv;
    KATANA_CHECKED(fv.Bind(path, true));

    if (fv.size() == 0) {
      return RDKSubstructureIndexPrimitive();
    }

    RDKSubstructureIndexPrimitive substructure_index;
    KATANA_CHECKED(katana::JsonParse<RDKSubstructureIndexPrimitive>(
        fv, &substructure_index));

    return substructure_index;
  }

  katana::Result<void> WriteManifest(const std::string& path) const {
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
