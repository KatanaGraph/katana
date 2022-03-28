#ifndef KATANA_LIBGRAPH_STORAGEFORMATVERSIONENTITYTYPEIDS_H_
#define KATANA_LIBGRAPH_STORAGEFORMATVERSIONENTITYTYPEIDS_H_

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "storage-format-version.h"

/*LDBC_003 Known EntityType Values*/

// id=0 is unknown/invalid and so is not an atomic entity type
static const size_t LDBC_003_EDGE_ENTITY_TYPE_COUNT = 16;
static const size_t LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_COUNT = 15;
static const size_t LDBC_003_NODE_ENTITY_TYPE_COUNT = 22;
static const size_t LDBC_003_NODE_ATOMIC_ENTITY_TYPE_COUNT = 14;

static const std::vector<std::string> LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_NAMES = {
    "",
    "CONTAINER_OF",
    "HAS_CREATOR",
    "HAS_INTEREST",
    "HAS_MEMBER",
    "HAS_MODERATOR",
    "HAS_TAG",
    "HAS_TYPE",
    "IS_LOCATED_IN",
    "IS_PART_OF",
    "IS_SUBCLASS_OF",
    "KNOWS",
    "LIKES",
    "REPLY_OF",
    "STUDY_AT",
    "WORK_AT"};

static const std::vector<std::string> LDBC_003_NODE_ATOMIC_ENTITY_TYPE_NAMES = {
    "",        "City",  "Comment", "Company",      "Continent",
    "Country", "Forum", "Message", "Organisation", "Person",
    "Place",   "Post",  "Tag",     "TagClass",     "University"};

static const std::vector<std::vector<katana::EntityTypeID>>
    LDBC_003_EDGE_ENTITY_TYPE_ID_TO_ATOMIC_ENTITY_TYPE_ID_MAP = {
        {},  {1}, {2},  {3},  {4},  {5},  {6},  {7},
        {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15},
};

static const std::vector<std::vector<katana::EntityTypeID>>
    LDBC_003_NODE_ENTITY_TYPE_ID_TO_ATOMIC_ENTITY_TYPE_ID_MAP = {
        {},     {1},    {2},     {3},     {4},     {5},    {6},  {7},
        {8},    {9},    {10},    {11},    {12},    {13},   {14}, {1, 10},
        {2, 7}, {3, 8}, {4, 10}, {5, 10}, {7, 11}, {8, 14}};

void
ValidateLDBC003EntityTypeManagers(
    katana::EntityTypeManager node_manager,
    katana::EntityTypeManager edge_manager) {
  // Validate Size
  KATANA_LOG_VASSERT(
      edge_manager.GetNumEntityTypes() == LDBC_003_EDGE_ENTITY_TYPE_COUNT,
      "edge_manager.GetNumEntityTypes() != LDBC_003_EDGE_ENTITY_TYPE_COUNT, {} "
      "!= {}",
      edge_manager.GetNumEntityTypes(), LDBC_003_EDGE_ENTITY_TYPE_COUNT);
  KATANA_LOG_VASSERT(
      node_manager.GetNumEntityTypes() == LDBC_003_NODE_ENTITY_TYPE_COUNT,
      "node_manager.GetNumEntityTypes() != LDBC_003_NODE_ENTITY_TYPE_COUNT, {} "
      "!= {}",
      node_manager.GetNumEntityTypes(), LDBC_003_NODE_ENTITY_TYPE_COUNT);

  KATANA_LOG_VASSERT(
      edge_manager.GetNumAtomicTypes() ==
          LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_COUNT,
      "edge_manager.GetNumAtomicTypes() != "
      "LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_COUNT, {} != {} ",
      edge_manager.GetNumAtomicTypes(), LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_COUNT);
  KATANA_LOG_VASSERT(
      node_manager.GetNumAtomicTypes() ==
          LDBC_003_NODE_ATOMIC_ENTITY_TYPE_COUNT,
      "node_manager.GetNumAtomicTypes() != "
      "LDBC_003_NODE_ATOMIC_ENTITY_TYPE_COUNT, {} != {}",
      node_manager.GetNumAtomicTypes(), LDBC_003_NODE_ATOMIC_ENTITY_TYPE_COUNT);

  // Validate Names
  // Atomic EntityTypeIDs start at id = 1, id = 0 is invalid/unknown
  for (katana::EntityTypeID id = 1;
       id <= LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_COUNT; id++) {
    auto res = edge_manager.GetAtomicTypeName(id);
    KATANA_LOG_VASSERT(
        res.has_value(), "Edge EntityTypeID {} does not have a valid name", id);
    KATANA_LOG_VASSERT(
        res.value() == LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_NAMES.at(id),
        "Edge EntityTypeID = {}, known_name = {}, manager_name = {}", id,
        LDBC_003_EDGE_ATOMIC_ENTITY_TYPE_NAMES.at(id), res.value());
  }

  for (katana::EntityTypeID id = 1;
       id <= LDBC_003_NODE_ATOMIC_ENTITY_TYPE_COUNT; id++) {
    auto res = node_manager.GetAtomicTypeName(id);
    KATANA_LOG_VASSERT(
        res.has_value(), "Node EntityTypeID {} does not have a valid name", id);
    KATANA_LOG_VASSERT(
        res.value() == LDBC_003_NODE_ATOMIC_ENTITY_TYPE_NAMES.at(id),
        "Node EntityTypeID = {}, known_name = {}, manager_name = {}", id,
        LDBC_003_NODE_ATOMIC_ENTITY_TYPE_NAMES.at(id), res.value());
  }

  // Validate EntityTypeID to AtomicEntityTypeID Mapping

  for (katana::EntityTypeID id = 0; id < LDBC_003_EDGE_ENTITY_TYPE_COUNT;
       id++) {
    auto set = edge_manager.GetAtomicSubtypes(id);
    for (auto it :
         LDBC_003_EDGE_ENTITY_TYPE_ID_TO_ATOMIC_ENTITY_TYPE_ID_MAP.at(id)) {
      KATANA_LOG_VASSERT(
          set.test(it),
          "Edge EntityTypeID = {} should map to Atomic EntityTypeID {}", id,
          it);
    }
  }

  for (katana::EntityTypeID id = 0; id < LDBC_003_NODE_ENTITY_TYPE_COUNT;
       id++) {
    auto set = node_manager.GetAtomicSubtypes(id);
    for (auto it :
         LDBC_003_NODE_ENTITY_TYPE_ID_TO_ATOMIC_ENTITY_TYPE_ID_MAP.at(id)) {
      KATANA_LOG_VASSERT(
          set.test(it),
          "Node EntityTypeID = {} should map to Atomic EntityTypeID {}", id,
          it);
    }
  }
}

void
TestConvertGraphStorageFormat(const katana::URI& input_rdg) {
  // Load existing "old" graph, which converts all uint8/bool properties into types
  // store it as a new file
  // load the new file
  // ensure the converted old graph, and the loaded new graph match

  KATANA_LOG_WARN("***** TestConvertGraphStorageFormat *****");

  katana::TxnContext txn_ctx;

  katana::PropertyGraph g = LoadGraph(input_rdg);
  ValidateLDBC003EntityTypeManagers(
      g.GetNodeTypeManager(), g.GetEdgeTypeManager());

  auto g2_rdg_file = StoreGraph(&g);
  katana::PropertyGraph g2 = LoadGraph(g2_rdg_file);
  ValidateLDBC003EntityTypeManagers(
      g2.GetNodeTypeManager(), g2.GetEdgeTypeManager());

  // This takes ~20 seconds
  KATANA_LOG_WARN("{}", g.ReportDiff(&g2));

  // Equals takes over a minute
  KATANA_LOG_ASSERT(g.Equals(&g2));
}

void
TestRoundTripNewStorageFormat(const katana::URI& input_rdg) {
  // Test store/load cycle of a graph with the new storage format
  // To do this, we first must first convert an old graph.
  // Steps:
  // Load existing "old" graph, which converts all uint8/bool properties into types
  // Store it as a new file
  // Load the new file
  // Ensure the converted old graph, and the loaded new graph match.
  // this should be trivially true if TestLoadGraphWithoutExternalTypes() passed
  // Now store the new graph
  // Noad the new graph

  KATANA_LOG_WARN("***** TestRoundTripNewStorageFormat *****");

  katana::TxnContext txn_ctx;

  // first cycle converts old->new
  katana::PropertyGraph g = LoadGraph(input_rdg);
  ValidateLDBC003EntityTypeManagers(
      g.GetNodeTypeManager(), g.GetEdgeTypeManager());

  auto g2_rdg_file = StoreGraph(&g);
  katana::PropertyGraph g2 = LoadGraph(g2_rdg_file);
  ValidateLDBC003EntityTypeManagers(
      g2.GetNodeTypeManager(), g2.GetEdgeTypeManager());

  // second cycle doesn't do any conversion, but tests storing/loading a "new format" graph
  auto g3_rdg_file = StoreGraph(&g2);
  katana::PropertyGraph g3 = LoadGraph(g3_rdg_file);
  ValidateLDBC003EntityTypeManagers(
      g3.GetNodeTypeManager(), g3.GetEdgeTypeManager());

  // This takes ~20 seconds
  KATANA_LOG_WARN("{}", g.ReportDiff(&g3));
  // Equals takes over a minute
  KATANA_LOG_ASSERT(g.Equals(&g3));
}

#endif
