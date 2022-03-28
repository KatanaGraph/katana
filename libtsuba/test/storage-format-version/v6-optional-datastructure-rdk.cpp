#include "v6-optional-datastructure-rdk.h"

#include <cstdint>
#include <filesystem>
#include <optional>
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
TestRoundTripRDKIndex(const katana::URI& rdg_dir) {
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
  auto rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg)));

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
  auto rdg_dir3 = KATANA_CHECKED(WriteRDG(std::move(rdg2)));

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
TestLoadFail(const katana::URI& rdg_dir) {
  // make a copy of the RDG in a new location
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_dir));
  // add an optional index
  katana::RDKLSHIndexPrimitive lsh_index = GenerateLSHIndex();
  ValidateLSHIndex(lsh_index);
  KATANA_CHECKED(rdg.WriteRDKLSHIndexPrimitive(lsh_index));
  auto rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg)));

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
      KATANA_CHECKED(find_file(rdg_dir2.path(), "rdk_lsh_index_manifest"));
  KATANA_LOG_DEBUG("replacing manifest file at {}", path);
  std::filesystem::remove(path);
  ff->Bind(path);
  KATANA_CHECKED(ff->Persist());

  // expect this to fail
  auto res = rdg2.LoadRDKLSHIndexPrimitive();
  if (res) {
    KATANA_LOG_FATAL("Loading the bad index should fail");
    return KATANA_ERROR(katana::ErrorCode::InvalidArgument, "should fail");
  }

  return katana::ResultSuccess();
}

katana::Result<void>
Run(const std::string& rdg_str) {
  auto rdg_dir = KATANA_CHECKED(katana::URI::Make(rdg_str));

  KATANA_CHECKED(TestRoundTripRDKIndex(rdg_dir));
  KATANA_CHECKED(TestLoadFail(rdg_dir));
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

  if (auto res = Run(argv[1]); !res) {
    KATANA_LOG_FATAL("run failed: {}", res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
