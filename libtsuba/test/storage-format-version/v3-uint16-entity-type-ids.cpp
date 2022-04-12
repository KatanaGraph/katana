#include "v3-uint16-entity-type-ids.h"

#include <algorithm>
#include <string>
#include <vector>

#include <boost/concept_check.hpp>
#include <boost/filesystem.hpp>

#include "../test-rdg.h"
#include "katana/EntityTypeManager.h"
#include "katana/Logging.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/RDGTopology.h"
#include "katana/Result.h"
#include "katana/URI.h"

/*
 * Tests to validate uint16_t EntityTypeIDs added in storage_format_version=3
 * Input can be any rdg with storage_format_version < 3
 */

namespace fs = boost::filesystem;

// Test cases

// Ensure that uint16_t EntityTypeIDs survive the store/load cycle
katana::Result<void>
TestEntityTypeManagerRoundTrip(const katana::URI& rdg_name) {
  KATANA_LOG_DEBUG("***** TestBasicEntityTypeIDConversion *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  katana::RDG rdg_orig = KATANA_CHECKED(LoadRDG(rdg_name));

  // Ensure we are working on a graph that already has EntityTypeIDs
  // libgalois is required to generate EntityTypeIDs, so tests for generation/storage
  // can be found there
  KATANA_LOG_ASSERT(rdg_orig.IsUint16tEntityTypeIDs());

  katana::EntityTypeManager edge_manager_orig =
      KATANA_CHECKED(rdg_orig.edge_entity_type_manager());
  katana::EntityTypeManager node_manager_orig =
      KATANA_CHECKED(rdg_orig.node_entity_type_manager());

  // write back the converted RDG
  auto rdg_dir_converted = KATANA_CHECKED(WriteRDG(std::move(rdg_orig)));

  katana::RDG rdg_converted = KATANA_CHECKED(LoadRDG(rdg_dir_converted));

  katana::EntityTypeManager edge_manager_converted =
      KATANA_CHECKED(rdg_converted.edge_entity_type_manager());

  katana::EntityTypeManager node_manager_converted =
      KATANA_CHECKED(rdg_converted.node_entity_type_manager());

  KATANA_LOG_VASSERT(
      edge_manager_orig.IsIsomorphicTo(edge_manager_converted),
      "original edge EntityTypeManager does not match the stored converted "
      "edge EntityTypeManager");
  KATANA_LOG_VASSERT(
      node_manager_orig.IsIsomorphicTo(node_manager_converted),
      "original node EntityTypeManager does not match the stored converted "
      "node EntityTypeManager");

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir_converted);
  fs::remove_all(rdg_dir_converted.path());

  return katana::ResultSuccess();
}

// Ensure rdg with maximum EntityTypeIDs, aka max(uint16_t) survive the store/load cycle
katana::Result<void>
TestMaxNumberEntityTypeIDs(const katana::URI& rdg_dir) {
  KATANA_LOG_DEBUG("***** TestMaxNumberEntityTypeIDs *****");

  KATANA_LOG_ASSERT(!rdg_dir.empty());

  // conversion of properties from uint8_t -> uint16_t in memory happens in load
  katana::RDG rdg_orig = KATANA_CHECKED(LoadRDG(rdg_dir));

  // Ensure we are working on a graph that already has EntityTypeIDs
  // libgalois is required to generate EntityTypeIDs, so tests for generation/storage
  // can be found there
  KATANA_LOG_ASSERT(rdg_orig.IsUint16tEntityTypeIDs());

  katana::EntityTypeManager edge_manager_orig =
      KATANA_CHECKED(rdg_orig.edge_entity_type_manager());
  katana::EntityTypeManager node_manager_orig =
      KATANA_CHECKED(rdg_orig.node_entity_type_manager());

  // fill the EntityTypeManagers to max size

  size_t add_num_edge_entity_type_id =
      katana::kInvalidEntityType - edge_manager_orig.GetNumEntityTypes();

  size_t add_num_node_entity_type_id =
      katana::kInvalidEntityType - node_manager_orig.GetNumEntityTypes();

  // use pre-generated vector of EntityTypeNames to see our vectors
  // Generating these on the fly would add multiple minutes to the test
  // See the header file where the vector is declared for the functions used to
  // generate the vector

  size_t num_to_gen = 0;
  if (add_num_node_entity_type_id > add_num_edge_entity_type_id) {
    num_to_gen = add_num_node_entity_type_id;
  } else {
    num_to_gen = add_num_edge_entity_type_id;
  }

  std::vector<std::string> generated_entity_type_names =
      vector_unique_strings(num_to_gen);

  std::vector<std::string> node_entity_type_names(
      generated_entity_type_names.begin(),
      generated_entity_type_names.begin() + add_num_node_entity_type_id);

  std::vector<std::string> edge_entity_type_names(
      generated_entity_type_names.begin(),
      generated_entity_type_names.begin() + add_num_edge_entity_type_id);

  KATANA_LOG_VASSERT(
      add_num_edge_entity_type_id == edge_entity_type_names.size(),
      "number of EntityTypeIDs to add does not equal the size of our "
      "pre-generated name vector. Must generate a new vector. Expected "
      "size = "
      "{}, actual size = {}",
      add_num_edge_entity_type_id, edge_entity_type_names.size());

  KATANA_LOG_VASSERT(
      add_num_node_entity_type_id == node_entity_type_names.size(),
      "number of EntityTypeIDs to add does not equal the size of our "
      "pre-generated name vector. Must generate a new vector. Expected size = "
      "{}, actual size = {}",
      add_num_node_entity_type_id, node_entity_type_names.size());

  katana::SetOfEntityTypeIDs added_edge_ids = KATANA_CHECKED_CONTEXT(
      edge_manager_orig.GetOrAddEntityTypeIDs(edge_entity_type_names),
      "Failed adding {} EntityType names to the edge manager",
      add_num_edge_entity_type_id);

  katana::SetOfEntityTypeIDs added_node_ids = KATANA_CHECKED_CONTEXT(
      node_manager_orig.GetOrAddEntityTypeIDs(node_entity_type_names),
      "Failed adding {} EntityType names to the node manager",
      add_num_node_entity_type_id);

  KATANA_LOG_VASSERT(
      edge_manager_orig.GetNumEntityTypes() ==
          std::numeric_limits<katana::EntityTypeID>::max(),
      "manager size = {}, max num = {}", edge_manager_orig.GetNumEntityTypes(),
      std::numeric_limits<katana::EntityTypeID>::max());

  KATANA_LOG_VASSERT(
      node_manager_orig.GetNumEntityTypes() ==
          std::numeric_limits<katana::EntityTypeID>::max(),
      "manager size = {}, max num = {}", node_manager_orig.GetNumEntityTypes(),
      std::numeric_limits<katana::EntityTypeID>::max());

  // Ensure all of our EntityTypes are actually present in the managers
  katana::SetOfEntityTypeIDs edge_ids = KATANA_CHECKED(
      edge_manager_orig.GetEntityTypeIDs(edge_entity_type_names));
  KATANA_LOG_ASSERT(edge_ids == added_edge_ids);

  katana::SetOfEntityTypeIDs node_ids = KATANA_CHECKED(
      node_manager_orig.GetEntityTypeIDs(node_entity_type_names));
  KATANA_LOG_ASSERT(node_ids == added_node_ids);

  // store our full EntityTypeManagers
  auto rdg_dir_again = KATANA_CHECKED(
      WriteRDG(std::move(rdg_orig), node_manager_orig, edge_manager_orig));

  katana::RDG rdg_full_entity_type_managers =
      KATANA_CHECKED(LoadRDG(rdg_dir_again));

  katana::EntityTypeManager edge_manager =
      KATANA_CHECKED(rdg_full_entity_type_managers.edge_entity_type_manager());
  katana::EntityTypeManager node_manager =
      KATANA_CHECKED(rdg_full_entity_type_managers.node_entity_type_manager());

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir_again);
  fs::remove_all(rdg_dir_again.path());

  KATANA_LOG_VASSERT(
      edge_manager.GetNumEntityTypes() ==
          std::numeric_limits<katana::EntityTypeID>::max(),
      "manager size = {}, max num = {}", edge_manager.GetNumEntityTypes(),
      std::numeric_limits<katana::EntityTypeID>::max());

  KATANA_LOG_VASSERT(
      node_manager.GetNumEntityTypes() ==
          std::numeric_limits<katana::EntityTypeID>::max(),
      "manager size = {}, max num = {}", node_manager.GetNumEntityTypes(),
      std::numeric_limits<katana::EntityTypeID>::max());

  KATANA_LOG_VASSERT(
      edge_manager_orig.IsIsomorphicTo(edge_manager),
      "original edge EntityTypeManager does not match the stored "
      "edge EntityTypeManager");
  KATANA_LOG_VASSERT(
      node_manager_orig.IsIsomorphicTo(node_manager),
      "original node EntityTypeManager does not match the stored "
      "node EntityTypeManager");

  return katana::ResultSuccess();
}

katana::Result<void>
Run(const std::string& rdg_str) {
  auto rdg_dir = KATANA_CHECKED(katana::URI::Make(rdg_str));

  KATANA_CHECKED(TestEntityTypeManagerRoundTrip(rdg_dir));
  KATANA_CHECKED(TestMaxNumberEntityTypeIDs(rdg_dir));
  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = katana::InitTsuba(); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }

  if (argc <= 1) {
    KATANA_LOG_FATAL("missing rdg file directory");
  }

  if (auto res = Run(argv[1]); !res) {
    KATANA_LOG_FATAL("run failed: {}", res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
