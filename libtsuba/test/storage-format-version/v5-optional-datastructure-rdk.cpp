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
ValidateSubstructIndex(katana::RDKSubstructureIndexPrimitive& index) {
  KATANA_LOG_ASSERT(index.fp_size() == 128);
  KATANA_LOG_ASSERT(index.num_entries() == 4);
  KATANA_LOG_ASSERT(index.index() == GenerateIndices());
  KATANA_LOG_ASSERT(index.fingerprints() == GenerateFingerprints());
  KATANA_LOG_ASSERT(index.smiles() == GenerateSmiles());
}

/*
 * Tests: Optional Datastructure, RDKLSHIndexPrimitive, RDKSubstructureIndexPrimitive functionality
 *
 * 1) loading an RDG without an optional index and adding one to it (RDKLSHIndexPrimitive)
 * 2) storing an RDG with an optional index (RDKLSHIndexPrimitive)
 * 3) loading an RDG with an optional index (RDKLSHIndexPrimitive)
 * 4) storing an RDG with 2 optional indices (RDKLSHIndexPrimitive, RDKSubstructureIndexPrimitive)
 * 5) loading an RDG with 2 optional indices (RDKLSHIndexPrimitive, RDKSubstructureIndexPrimitive)
 */
katana::Result<void>
TestRoundTripRDKIndex(const std::string& rdg_dir) {
  KATANA_LOG_ASSERT(!rdg_dir.empty());
  katana::RDKLSHIndexPrimitive lsh_index = GenerateLSHIndex();
  ValidateLSHIndex(lsh_index);

  // load the rdg, no optional indices present
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_dir));

  // write out an optional index
  KATANA_CHECKED(rdg.WriteRDKLSHIndexPrimitive(lsh_index));

  // Read the index back and ensure it matches what we put in
  std::optional<katana::RDKLSHIndexPrimitive> lsh_index_res_2 =
      KATANA_CHECKED(rdg.LoadRDKLSHIndexPrimitive());
  KATANA_LOG_ASSERT(lsh_index_res_2);
  katana::RDKLSHIndexPrimitive& lsh_index_2 = lsh_index_res_2.value();
  ValidateLSHIndex(lsh_index_2);

  // Store the RDG in a new location
  std::string rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg)));

  // Load the RDG from the new location
  katana::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));

  // Ensure our index is still correct
  std::optional<katana::RDKLSHIndexPrimitive> lsh_index_res_3 =
      KATANA_CHECKED(rdg2.LoadRDKLSHIndexPrimitive());
  KATANA_LOG_ASSERT(lsh_index_res_3);
  katana::RDKLSHIndexPrimitive& lsh_index_3 = lsh_index_res_3.value();
  ValidateLSHIndex(lsh_index_3);

  // Add a different optional index
  katana::RDKSubstructureIndexPrimitive substruct_index =
      GenerateSubstructIndex();
  KATANA_CHECKED(rdg2.WriteRDKSubstructureIndexPrimitive(substruct_index));

  // Read it back right away and ensure it matches what we put in
  std::optional<katana::RDKSubstructureIndexPrimitive> substruct_index_res_2 =
      KATANA_CHECKED(rdg2.LoadRDKSubstructureIndexPrimitive());
  KATANA_LOG_ASSERT(substruct_index_res_2);
  katana::RDKSubstructureIndexPrimitive& substruct_index_2 =
      substruct_index_res_2.value();
  ValidateSubstructIndex(substruct_index_2);

  // Store the RDG in a new location
  std::string rdg_dir3 = KATANA_CHECKED(WriteRDG(std::move(rdg2)));

  // Load the RDG from the new location
  katana::RDG rdg3 = KATANA_CHECKED(LoadRDG(rdg_dir3));

  // Ensure both of our indices are still correct
  std::optional<katana::RDKSubstructureIndexPrimitive> substruct_index_res_3 =
      KATANA_CHECKED(rdg3.LoadRDKSubstructureIndexPrimitive());
  KATANA_LOG_ASSERT(substruct_index_res_3);
  katana::RDKSubstructureIndexPrimitive& substruct_index_3 =
      substruct_index_res_3.value();
  ValidateSubstructIndex(substruct_index_3);
  std::optional<katana::RDKLSHIndexPrimitive> lsh_index_res_4 =
      KATANA_CHECKED(rdg3.LoadRDKLSHIndexPrimitive());
  KATANA_LOG_ASSERT(lsh_index_res_4);
  katana::RDKLSHIndexPrimitive& lsh_index_4 = lsh_index_res_4.value();
  ValidateLSHIndex(lsh_index_4);

  return katana::ResultSuccess();
}

/*
 * Tests that we fail loading an invalid version of an optional topology
 * and that we fail in a way that the caller can recover from
 */
katana::Result<void>
TestLoadFail(const std::string& rdg_dir) {
  // make a copy of the RDG in a new location
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_dir));
  // add an optional index
  katana::RDKLSHIndexPrimitive lsh_index = GenerateLSHIndex();
  ValidateLSHIndex(lsh_index);
  KATANA_CHECKED(rdg.WriteRDKLSHIndexPrimitive(lsh_index));
  std::string rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg)));

  // Load the RDG from the new location
  katana::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));

  // make a garbage json file in the place of an optional datastructure
  std::vector dummy = {"these", "are", "some", "bad", "values"};
  std::string serialized = KATANA_CHECKED(katana::JsonDump(dummy));
  // POSIX files end with newlines
  serialized = serialized + "\n";

  auto ff = std::make_unique<katana::FileFrame>();
  KATANA_CHECKED(ff->Init(serialized.size()));
  if (auto res = ff->Write(serialized.data(), serialized.size()); !res.ok()) {
    return KATANA_ERROR(
        katana::ArrowToKatana(res.code()), "arrow error: {}", res);
  }

  // write garbage over existing optional datastructure manifest
  // rdg must have only one of these manifests available for this test to function propertly
  std::string path =
      KATANA_CHECKED(find_file(rdg_dir2, "rdk_lsh_index_manifest"));
  std::filesystem::remove(path);
  ff->Bind(path);
  KATANA_CHECKED(ff->Persist());

  // expect this to fail
  auto res = rdg2.LoadRDKLSHIndexPrimitive();
  if (res) {
    KATANA_LOG_ASSERT("Loading the garbage manifest should fail!");
  }

  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = katana::InitTsuba(); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }
  katana::GaloisRuntime Katana_runtime;

  if (argc <= 1) {
    KATANA_LOG_FATAL("missing rdg file directory");
  }
  katana::ProgressTracer::Set(katana::TextTracer::Make());
  katana::ProgressScope host_scope =
      katana::GetTracer().StartActiveSpan("rdg-slice test");

  // Ensure the feature flag is actually set
  KATANA_LOG_ASSERT(KATANA_EXPERIMENTAL_ENABLED(UnstableRDGStorageFormat));

  const std::string& rdg = argv[1];

  auto res = TestRoundTripRDKIndex(rdg);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  res = TestLoadFail(rdg);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
