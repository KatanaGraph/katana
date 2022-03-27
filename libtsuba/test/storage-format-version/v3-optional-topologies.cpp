#include <algorithm>

#include <boost/filesystem.hpp>

#include "../test-rdg.h"
#include "katana/Logging.h"
#include "katana/ProgressTracer.h"
#include "katana/RDG.h"
#include "katana/RDGManifest.h"
#include "katana/RDGTopology.h"
#include "katana/Result.h"
#include "katana/TextTracer.h"
#include "katana/URI.h"

/*
 * Tests to validate optional topology storage added in storage_format_version=3
 * Input can be any rdg with storage_format_version < 3
 */

namespace fs = boost::filesystem;

// number of nodes/edges in the default csr topology for the expected input rdg
// if the input changes, these must be changed
static constexpr size_t csr_num_nodes = 29946;
static constexpr size_t csr_num_edges = 43072;

void
ValidateBaseTopologyData(katana::RDGTopology* topo) {
  // Validate number of  nodes/edges matches the default number in the csr
  KATANA_LOG_ASSERT(topo->num_nodes() == csr_num_nodes);
  KATANA_LOG_ASSERT(topo->num_edges() == csr_num_edges);
}

katana::Result<katana::RDGTopology*>
GetCSR(katana::RDG* rdg) {
  katana::RDGTopology shadow_csr = katana::RDGTopology::MakeShadowCSR();
  katana::RDGTopology* csr = KATANA_CHECKED_CONTEXT(
      rdg->GetTopology(shadow_csr),
      "unable to find csr topology, must have csr topology");
  ValidateBaseTopologyData(csr);
  return csr;
}

katana::Result<void>
CSRPresent(katana::RDG* rdg) {
  katana::RDGTopology* csr = KATANA_CHECKED(GetCSR(rdg));
  KATANA_CHECKED(csr->unbind_file_storage());
  return katana::ResultSuccess();
}

void
CleanupRDGDirs(std::vector<katana::URI> dirs) {
  for (auto rdg_dir : dirs) {
    KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir);
    fs::remove_all(rdg_dir.path());
  }
}

/// Load a graph that was stored without optional topology support
/// Ensure it survives a store/load cycle
katana::Result<void>
TestGraphBackwardsCompatabilityRoundTrip(const katana::URI& rdg_name) {
  KATANA_LOG_DEBUG("***** Testing Backwards Compatability *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  // load old rdg
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));

  // load old csr topology
  KATANA_CHECKED(CSRPresent(&rdg));

  // write out converted rdg
  auto rdg_dir1 = KATANA_CHECKED(WriteRDG(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load converted rdg
  katana::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  // ensure we can still find the csr
  KATANA_CHECKED(CSRPresent(&rdg));

  return katana::ResultSuccess();
}

/// Load a graph, add a complex optional topology and store it
/// Ensure added optional topology didn't change
/// Since we added the optional topology to a graph that originally did not have
katana::Result<void>
TestGraphComplexOptionalTopologyRoundTrip(const katana::URI& rdg_name) {
  KATANA_LOG_DEBUG(
      "***** Testing Complex Optional Topology Support Roundtrip *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  // load rdg, give it an optional topology
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));
  katana::RDGTopology* csr = KATANA_CHECKED(GetCSR(&rdg));

  // create new topology with partial optional data and store it
  uint64_t dummy_edge_property_index_value = 0x09F9;
  uint64_t dummy_edge_property_index[csr->num_edges()];
  std::fill_n(
      dummy_edge_property_index, csr->num_edges(),
      dummy_edge_property_index_value);

  uint64_t dummy_node_property_index_value = 0x0ddba11;
  uint64_t dummy_node_property_index[csr->num_nodes()];
  std::fill_n(
      dummy_node_property_index, csr->num_nodes(),
      dummy_node_property_index_value);

  // make copies of the csr data since we unbind all topologies before calling store
  uint64_t adj_indices_copy[csr_num_nodes];
  for (size_t i = 0; i < csr_num_nodes; i++) {
    adj_indices_copy[i] = csr->adj_indices()[i];
  }

  uint32_t dests_copy[csr_num_edges];
  for (size_t i = 0; i < csr_num_nodes; i++) {
    dests_copy[i] = csr->dests()[i];
  }

  katana::RDGTopology topo = KATANA_CHECKED(katana::RDGTopology::Make(
      &adj_indices_copy[0], csr->num_nodes(), &dests_copy[0], csr->num_edges(),
      katana::RDGTopology::TopologyKind::kShuffleTopology,
      katana::RDGTopology::TransposeKind::kNo,
      katana::RDGTopology::EdgeSortKind::kSortedByDestID,
      katana::RDGTopology::NodeSortKind::kSortedByDegree,
      &dummy_edge_property_index[0], &dummy_node_property_index[0]));

  rdg.AddTopology(std::move(topo));

  // now that we are done with the csr, unbind it since we expect all
  // topology file stores to be unbound before calling RDG::Store
  KATANA_CHECKED(csr->unbind_file_storage());
  csr = nullptr;

  auto rdg_dir1 = KATANA_CHECKED(WriteRDG(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load rdg with optional topology and verify it
  katana::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  KATANA_CHECKED(CSRPresent(&rdg1));

  katana::RDGTopology shadow_optional_topology =
      katana::RDGTopology::MakeShadow(
          katana::RDGTopology::TopologyKind::kShuffleTopology,
          katana::RDGTopology::TransposeKind::kNo,
          katana::RDGTopology::EdgeSortKind::kSortedByDestID,
          katana::RDGTopology::NodeSortKind::kSortedByDegree);

  katana::RDGTopology* optional_topology = KATANA_CHECKED_CONTEXT(
      rdg1.GetTopology(shadow_optional_topology),
      "unable to find optional topology we just added");

  // since we built our optional topology from the default csr, validate the base data
  ValidateBaseTopologyData(optional_topology);

  // validate the optional data
  for (size_t i = 0; i < optional_topology->num_edges(); i++) {
    KATANA_LOG_ASSERT(
        optional_topology->edge_index_to_property_index_map()[i] ==
        dummy_edge_property_index_value);
  }

  for (size_t i = 0; i < optional_topology->num_nodes(); i++) {
    KATANA_LOG_ASSERT(
        optional_topology->node_index_to_property_index_map()[i] ==
        dummy_node_property_index_value);
  }

  CleanupRDGDirs({rdg_dir1});

  return katana::ResultSuccess();
}

/// Load a graph, add an optional topology and store it
/// Ensure added optional topology didn't change
/// Since we added the optional topology to a graph that originally did not have
/// optional topology support, store/load the graph again
katana::Result<void>
TestGraphOptionalTopologyRoundTrip(const katana::URI& rdg_name) {
  KATANA_LOG_DEBUG("***** Testing Optional Topology Support Roundtrip *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  // load rdg, give it an optional topology
  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));
  katana::RDGTopology* csr = KATANA_CHECKED(GetCSR(&rdg));

  // create new topology with partial optional data and store it
  uint64_t dummy_edge_property_index_value = 0x09F9;
  uint64_t dummy_edge_property_index[csr->num_edges()];
  std::fill_n(
      dummy_edge_property_index, csr->num_edges(),
      dummy_edge_property_index_value);

  // make copies of the csr data since we unbind all topologies before calling store
  uint64_t adj_indices_copy[csr_num_nodes];
  for (size_t i = 0; i < csr_num_nodes; i++) {
    adj_indices_copy[i] = csr->adj_indices()[i];
  }

  uint32_t dests_copy[csr_num_edges];
  for (size_t i = 0; i < csr_num_nodes; i++) {
    dests_copy[i] = csr->dests()[i];
  }

  katana::RDGTopology topo = KATANA_CHECKED(katana::RDGTopology::Make(
      &adj_indices_copy[0], csr->num_nodes(), &dests_copy[0], csr->num_edges(),
      katana::RDGTopology::TopologyKind::kEdgeShuffleTopology,
      katana::RDGTopology::TransposeKind::kYes,
      katana::RDGTopology::EdgeSortKind::kSortedByDestID,
      &dummy_edge_property_index[0]));

  rdg.AddTopology(std::move(topo));

  // now that we are done with the csr, unbind it since we expect all
  // topology file stores to be unbound before calling RDG::Store
  KATANA_CHECKED(csr->unbind_file_storage());
  csr = nullptr;

  auto rdg_dir1 = KATANA_CHECKED(WriteRDG(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load rdg with optional topology and verify it
  katana::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  KATANA_CHECKED(CSRPresent(&rdg1));

  katana::RDGTopology shadow_optional_topology =
      katana::RDGTopology::MakeShadow(
          katana::RDGTopology::TopologyKind::kEdgeShuffleTopology,
          katana::RDGTopology::TransposeKind::kYes,
          katana::RDGTopology::EdgeSortKind::kSortedByDestID,
          katana::RDGTopology::NodeSortKind::kAny);

  katana::RDGTopology* optional_topology = KATANA_CHECKED_CONTEXT(
      rdg1.GetTopology(shadow_optional_topology),
      "unable to find optional topology we just added");

  // since we built our optional topology from the default csr, validate the base data
  ValidateBaseTopologyData(optional_topology);

  // validate the optional data
  for (size_t i = 0; i < optional_topology->num_edges(); i++) {
    KATANA_LOG_ASSERT(
        optional_topology->edge_index_to_property_index_map()[i] ==
        dummy_edge_property_index_value);
  }

  // now that we are done with the topology, unbind it since we expect all
  // topology file stores to be unbound before calling RDG::Store
  KATANA_CHECKED(optional_topology->unbind_file_storage());
  optional_topology = nullptr;

  // write out rdg with optional topology
  auto rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg1)));
  KATANA_LOG_ASSERT(!rdg_dir2.empty());

  // load rdg again, and verify the optional topology
  katana::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));
  KATANA_CHECKED(CSRPresent(&rdg2));

  shadow_optional_topology = katana::RDGTopology::MakeShadow(
      katana::RDGTopology::TopologyKind::kEdgeShuffleTopology,
      katana::RDGTopology::TransposeKind::kYes,
      katana::RDGTopology::EdgeSortKind::kSortedByDestID,
      katana::RDGTopology::NodeSortKind::kAny);

  optional_topology = KATANA_CHECKED_CONTEXT(
      rdg2.GetTopology(shadow_optional_topology),
      "unable to find optional topology we just added");

  // since we built our optional topology from the default csr, validate the base data
  ValidateBaseTopologyData(optional_topology);

  // validate the optional data
  for (size_t i = 0; i < optional_topology->num_edges(); i++) {
    KATANA_LOG_ASSERT(
        optional_topology->edge_index_to_property_index_map()[i] ==
        dummy_edge_property_index_value);
  }

  CleanupRDGDirs({rdg_dir1, rdg_dir2});

  return katana::ResultSuccess();
}

/// Load a graph that was stored without optional topology support
/// Store it so we get a graph with optional topology support
/// Ensure graph with optional topology support survives store/load cycle
katana::Result<void>
TestGraphBasicRoundTrip(const katana::URI& rdg_name) {
  KATANA_LOG_DEBUG("***** Testing Basic Roundtrip *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  katana::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));
  KATANA_CHECKED(CSRPresent(&rdg));
  auto rdg_dir1 = KATANA_CHECKED(WriteRDG(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load converted rdg
  katana::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  // ensure we can still find the csr
  KATANA_CHECKED(CSRPresent(&rdg1));

  // write out converted rdg

  auto rdg_dir2 = KATANA_CHECKED(WriteRDG(std::move(rdg1)));
  KATANA_LOG_ASSERT(!rdg_dir2.empty());

  // load converted rdg
  katana::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));

  // ensure we can still find the csr
  KATANA_CHECKED(CSRPresent(&rdg2));

  CleanupRDGDirs({rdg_dir1, rdg_dir2});

  return katana::ResultSuccess();
}

katana::Result<void>
Run(const std::string& rdg_str) {
  const katana::URI rdg_dir = KATANA_CHECKED(katana::URI::Make(rdg_str));
  KATANA_CHECKED(TestGraphBasicRoundTrip(rdg_dir));
  KATANA_CHECKED(TestGraphOptionalTopologyRoundTrip(rdg_dir));
  KATANA_CHECKED(TestGraphComplexOptionalTopologyRoundTrip(rdg_dir));
  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = katana::InitTsuba(); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }
  katana::ProgressTracer::Set(katana::TextTracer::Make());
  katana::ProgressScope host_scope =
      katana::GetTracer().StartActiveSpan("rdg-slice test");

  if (argc <= 1) {
    KATANA_LOG_FATAL("missing rdg file directory");
  }

  if (auto res = Run(argv[1]); !res) {
    KATANA_LOG_FATAL("URI from string {} failed: {}", argv[1], res.error());
  }

  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_FATAL("katana::FiniTsuba: {}", fini_good.error());
  }

  return 0;
}
