#include <cstdint>
#include <filesystem>
#include <vector>

#include <boost/filesystem.hpp>

#include "../test-rdg.h"
#include "katana/Experimental.h"
#include "katana/Galois.h"
#include "katana/Logging.h"
#include "katana/ProgressTracer.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/RDGStorageFormatVersion.h"
#include "katana/RDKLSHIndexPrimitive.h"
#include "katana/RDKSubstructureIndexPrimitive.h"
#include "katana/Result.h"
#include "katana/TextTracer.h"
#include "katana/URI.h"

std::vector<std::map<uint64_t, std::vector<uint64_t>>>
GenerateHashes() {
  std::vector<std::map<uint64_t, std::vector<uint64_t>>> hashes(128);
  for (uint64_t i = 0; i < 128; i++) {
    for (uint64_t j = 0; j < 64; j++) {
      std::map<uint64_t, std::vector<uint64_t>> tmp;
      tmp[j] = {i, j, i + j};
      hashes.emplace_back(tmp);
    }
  }
  return hashes;
}

std::vector<katana::DynamicBitset>
GenerateFingerprints() {
  std::vector<katana::DynamicBitset> fingerprints;

  for (size_t i = 0; i < 4; i++) {
    katana::DynamicBitset bset;
    for (size_t j = 0; j < i; j++) {
      bset.resize(j + 1);
      bset.set(j);
    }
    fingerprints.emplace_back(std::move(bset));
  }

  return fingerprints;
}

std::vector<std::string>
GenerateSmiles() {
  std::vector<std::string> smiles = {"smile1", "smile2", "smile3", "smile4"};
  return smiles;
}

std::vector<std::vector<std::uint64_t>>
GenerateIndices() {
  std::vector<std::vector<std::uint64_t>> indices(
      128, std::vector<uint64_t>(64));
  for (size_t i = 0; i < 128; i++) {
    for (size_t j = 0; j < 64; j++) {
      indices[i][j] = i + j;
    }
  }
  return indices;
}

katana::RDKLSHIndexPrimitive
GenerateLSHIndex() {
  katana::RDKLSHIndexPrimitive index;

  std::vector<katana::DynamicBitset> fingerprints = GenerateFingerprints();
  index.set_num_hashes_per_bucket(16);
  index.set_num_buckets(96);
  index.set_fingerprint_length(42);
  index.set_num_fingerprints(fingerprints.size());
  index.set_hash_structure(GenerateHashes());
  index.set_fingerprints(std::move(fingerprints));
  index.set_smiles(GenerateSmiles());
  return index;
}

katana::RDKSubstructureIndexPrimitive
GenerateSubstructIndex() {
  katana::RDKSubstructureIndexPrimitive index;

  std::vector<katana::DynamicBitset> fingerprints = GenerateFingerprints();
  auto smiles = GenerateSmiles();
  auto indices = GenerateIndices();
  KATANA_LOG_VASSERT(
      smiles.size() == fingerprints.size(), "smiles = {}, finger  = {}",
      smiles.size(), fingerprints.size());

  index.set_fp_size(indices.size());
  index.set_num_entries(smiles.size());
  index.set_index(std::move(indices));
  index.set_fingerprints(std::move(fingerprints));
  index.set_smiles(std::move(smiles));
  return index;
}

void
ValidateLSHIndex(katana::RDKLSHIndexPrimitive& index) {
  KATANA_LOG_ASSERT(index.num_hashes_per_bucket() == 16);
  KATANA_LOG_ASSERT(index.num_buckets() == 96);
  KATANA_LOG_ASSERT(index.fingerprint_length() == 42);
  KATANA_LOG_ASSERT(index.num_fingerprints() == 4);
  KATANA_LOG_ASSERT(index.hash_structure() == GenerateHashes());
  KATANA_LOG_ASSERT(index.fingerprints() == GenerateFingerprints());
  KATANA_LOG_ASSERT(index.smiles() == GenerateSmiles());
}

void
ValidateSubstructIndex(katana::RDKSubstructureIndexPrimitive& index) {
  KATANA_LOG_ASSERT(index.fp_size() == 128);
  KATANA_LOG_ASSERT(index.num_entries() == 4);
  KATANA_LOG_ASSERT(index.index() == GenerateIndices());
  KATANA_LOG_ASSERT(index.fingerprints() == GenerateFingerprints());
  KATANA_LOG_ASSERT(index.smiles() == GenerateSmiles());
}
