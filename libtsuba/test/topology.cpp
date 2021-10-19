#include <algorithm>

#include <boost/filesystem.hpp>

#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/RDG.h"
#include "tsuba/RDGManifest.h"
#include "tsuba/RDGTopology.h"

namespace fs = boost::filesystem;

katana::Result<std::string>
Write(tsuba::RDG&& rdg_) {
  auto uri_res = katana::Uri::MakeRand("/tmp/propertyfilegraph");
  KATANA_LOG_ASSERT(uri_res);
  std::string tmp_rdg_dir(uri_res.value().path());  // path() because local
  std::string command_line;

  // Store graph. If there is a new storage format then storing it is enough to bump the version up.
  KATANA_LOG_WARN("writing graph at temp file {}", tmp_rdg_dir);

  if (auto res = tsuba::Create(tmp_rdg_dir); !res) {
    return res.error();
  }

  tsuba::RDGManifest manifest =
      KATANA_CHECKED(tsuba::FindManifest(tmp_rdg_dir));
  auto open_res = tsuba::Open(std::move(manifest), tsuba::kReadWrite);
  if (!open_res) {
    return open_res.error();
  }
  auto new_file = std::make_unique<tsuba::RDGFile>(open_res.value());

  auto res = rdg_.Store(
      *new_file, command_line,
      tsuba::RDG::RDGVersioningPolicy::IncrementVersion, nullptr, nullptr,
      KATANA_CHECKED(rdg_.node_entity_type_manager()),
      KATANA_CHECKED(rdg_.edge_entity_type_manager()));

  if (!res) {
    return res.error();
  }
  return tmp_rdg_dir;
}

katana::Result<tsuba::RDG>
LoadRDG(const std::string& rdg_name) {
  tsuba::RDGManifest manifest = KATANA_CHECKED(tsuba::FindManifest(rdg_name));
  tsuba::RDGFile rdg_file{
      KATANA_CHECKED(tsuba::Open(std::move(manifest), tsuba::kReadWrite))};
  tsuba::RDG rdg =
      KATANA_CHECKED(tsuba::RDG::Make(rdg_file, tsuba::RDGLoadOptions()));

  return tsuba::RDG(std::move(rdg));
}

katana::Result<tsuba::RDGTopology*>
GetCSR(tsuba::RDG* rdg) {
  tsuba::RDGTopology shadow_csr = tsuba::RDGTopology::MakeShadowCSR();
  tsuba::RDGTopology* csr = KATANA_CHECKED_CONTEXT(
      rdg->GetTopology(shadow_csr),
      "unable to find csr topology, must have csr topology");

  KATANA_LOG_ASSERT(csr->num_nodes() == 29946);
  KATANA_LOG_ASSERT(csr->num_edges() == 43072);

  return csr;
}

/// Load a graph that was stored without optional topology support
/// Ensure it survives a store/load cycle
katana::Result<void>
TestGraphBackwardsCompatabilityRoundTrip(const std::string& rdg_name) {
  KATANA_LOG_DEBUG("***** Testing Backwards Compatability *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  // load old rdg
  tsuba::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));

  // load old csr topology
  tsuba::RDGTopology* csr = KATANA_CHECKED(GetCSR(&rdg));

  // write out converted rdg
  std::string rdg_dir1 = KATANA_CHECKED(Write(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load converted rdg
  tsuba::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  // ensure we can still find the csr

  tsuba::RDGTopology* csr1 = KATANA_CHECKED(GetCSR(&rdg1));

  KATANA_LOG_ASSERT(csr1->num_edges() == csr->num_edges());
  KATANA_LOG_ASSERT(csr1->num_nodes() == csr->num_nodes());

  return katana::ResultSuccess();
}

/// Load a graph, add a complex optional topology and store it
/// Ensure added optional topology didn't change
/// Since we added the optional topology to a graph that originally did not have
katana::Result<void>
TestGraphComplexOptionalTopologyRoundTrip(const std::string& rdg_name) {
  KATANA_LOG_DEBUG(
      "***** Testing Complex Optional Topology Support Roundtrip *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  // load rdg, give it an optional topology
  tsuba::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));
  tsuba::RDGTopology* csr = KATANA_CHECKED(GetCSR(&rdg));

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

  tsuba::RDGTopology topo = KATANA_CHECKED(tsuba::RDGTopology::Make(
      csr->adj_indices(), csr->num_nodes(), csr->dests(), csr->num_edges(),
      tsuba::RDGTopology::TopologyKind::kShuffleTopology,
      tsuba::RDGTopology::TransposeKind::kNo,
      tsuba::RDGTopology::EdgeSortKind::kSortedByDestID,
      tsuba::RDGTopology::NodeSortKind::kSortedByDegree,
      &dummy_edge_property_index[0], &dummy_node_property_index[0]));

  rdg.AddTopology(std::move(topo));

  std::string rdg_dir1 = KATANA_CHECKED(Write(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load rdg with optional topology and verify it
  tsuba::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  KATANA_CHECKED(GetCSR(&rdg1));

  tsuba::RDGTopology shadow_optional_topology = tsuba::RDGTopology::MakeShadow(
      tsuba::RDGTopology::TopologyKind::kShuffleTopology,
      tsuba::RDGTopology::TransposeKind::kNo,
      tsuba::RDGTopology::EdgeSortKind::kSortedByDestID,
      tsuba::RDGTopology::NodeSortKind::kSortedByDegree);

  tsuba::RDGTopology* optional_topology = KATANA_CHECKED_CONTEXT(
      rdg1.GetTopology(shadow_optional_topology),
      "unable to find optional topology we just added");

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir1);
  fs::remove_all(rdg_dir1);
  rdg_dir1 = "";

  KATANA_LOG_ASSERT(csr->num_nodes() == optional_topology->num_nodes());
  KATANA_LOG_ASSERT(csr->num_edges() == optional_topology->num_edges());

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

  return katana::ResultSuccess();
}

/// Load a graph, add an optional topology and store it
/// Ensure added optional topology didn't change
/// Since we added the optional topology to a graph that originally did not have
/// optional topology support, store/load the graph again
katana::Result<void>
TestGraphOptionalTopologyRoundTrip(const std::string& rdg_name) {
  KATANA_LOG_DEBUG("***** Testing Optional Topology Support Roundtrip *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  // load rdg, give it an optional topology
  tsuba::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));
  tsuba::RDGTopology* csr = KATANA_CHECKED(GetCSR(&rdg));

  // create new topology with partial optional data and store it
  uint64_t dummy_edge_property_index_value = 0x09F9;
  uint64_t dummy_edge_property_index[csr->num_edges()];
  std::fill_n(
      dummy_edge_property_index, csr->num_edges(),
      dummy_edge_property_index_value);

  tsuba::RDGTopology topo = KATANA_CHECKED(tsuba::RDGTopology::Make(
      csr->adj_indices(), csr->num_nodes(), csr->dests(), csr->num_edges(),
      tsuba::RDGTopology::TopologyKind::kEdgeShuffleTopology,
      tsuba::RDGTopology::TransposeKind::kYes,
      tsuba::RDGTopology::EdgeSortKind::kSortedByDestID,
      &dummy_edge_property_index[0]));

  rdg.AddTopology(std::move(topo));

  std::string rdg_dir1 = KATANA_CHECKED(Write(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load rdg with optional topology and verify it
  tsuba::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));

  KATANA_CHECKED(GetCSR(&rdg1));

  tsuba::RDGTopology shadow_optional_topology = tsuba::RDGTopology::MakeShadow(
      tsuba::RDGTopology::TopologyKind::kEdgeShuffleTopology,
      tsuba::RDGTopology::TransposeKind::kYes,
      tsuba::RDGTopology::EdgeSortKind::kSortedByDestID,
      tsuba::RDGTopology::NodeSortKind::kAny);

  tsuba::RDGTopology* optional_topology = KATANA_CHECKED_CONTEXT(
      rdg1.GetTopology(shadow_optional_topology),
      "unable to find optional topology we just added");

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir1);
  fs::remove_all(rdg_dir1);
  rdg_dir1 = "";

  KATANA_LOG_ASSERT(csr->num_nodes() == optional_topology->num_nodes());
  KATANA_LOG_ASSERT(csr->num_edges() == optional_topology->num_edges());

  for (size_t i = 0; i < optional_topology->num_edges(); i++) {
    KATANA_LOG_ASSERT(
        optional_topology->edge_index_to_property_index_map()[i] ==
        dummy_edge_property_index_value);
  }

  // write out rdg with optional topology

  std::string rdg_dir2 = KATANA_CHECKED(Write(std::move(rdg1)));
  KATANA_LOG_ASSERT(!rdg_dir2.empty());

  // load rdg again, and verify the optional topology
  tsuba::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));
  KATANA_CHECKED(GetCSR(&rdg2));

  shadow_optional_topology = tsuba::RDGTopology::MakeShadow(
      tsuba::RDGTopology::TopologyKind::kEdgeShuffleTopology,
      tsuba::RDGTopology::TransposeKind::kYes,
      tsuba::RDGTopology::EdgeSortKind::kSortedByDestID,
      tsuba::RDGTopology::NodeSortKind::kAny);

  optional_topology = KATANA_CHECKED_CONTEXT(
      rdg2.GetTopology(shadow_optional_topology),
      "unable to find optional topology we just added");

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir2);
  fs::remove_all(rdg_dir2);
  rdg_dir2 = "";

  KATANA_LOG_ASSERT(csr->num_nodes() == optional_topology->num_nodes());
  KATANA_LOG_ASSERT(csr->num_edges() == optional_topology->num_edges());

  for (size_t i = 0; i < optional_topology->num_edges(); i++) {
    KATANA_LOG_ASSERT(
        optional_topology->edge_index_to_property_index_map()[i] ==
        dummy_edge_property_index_value);
  }

  return katana::ResultSuccess();
}

/// Load a graph that was stored without optional topology support
/// Store it so we get a graph with optional topology support
/// Ensure graph with optional topology support survives store/load cycle
katana::Result<void>
TestGraphBasicRoundTrip(const std::string& rdg_name) {
  KATANA_LOG_DEBUG("***** Testing Basic Roundtrip *****");

  KATANA_LOG_ASSERT(!rdg_name.empty());

  tsuba::RDG rdg = KATANA_CHECKED(LoadRDG(rdg_name));
  KATANA_CHECKED(GetCSR(&rdg));
  std::string rdg_dir1 = KATANA_CHECKED(Write(std::move(rdg)));
  KATANA_LOG_ASSERT(!rdg_dir1.empty());

  // load converted rdg
  tsuba::RDG rdg1 = KATANA_CHECKED(LoadRDG(rdg_dir1));
  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir1);

  // ensure we can still find the csr
  KATANA_CHECKED(GetCSR(&rdg1));

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir1);
  fs::remove_all(rdg_dir1);
  rdg_dir1 = "";

  // write out converted rdg

  std::string rdg_dir2 = KATANA_CHECKED(Write(std::move(rdg1)));
  KATANA_LOG_ASSERT(!rdg_dir2.empty());

  // load converted rdg
  tsuba::RDG rdg2 = KATANA_CHECKED(LoadRDG(rdg_dir2));
  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir2);

  // ensure we can still find the csr
  KATANA_CHECKED(GetCSR(&rdg2));

  KATANA_LOG_DEBUG("removing rdg dir: {}", rdg_dir2);
  fs::remove_all(rdg_dir2);
  rdg_dir2 = "";

  return katana::ResultSuccess();
}

int
main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    KATANA_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  if (argc <= 1) {
    KATANA_LOG_FATAL("missing rdg file directory");
  }

  auto res = TestGraphBasicRoundTrip(argv[1]);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  res = TestGraphOptionalTopologyRoundTrip(argv[1]);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  res = TestGraphComplexOptionalTopologyRoundTrip(argv[1]);
  if (!res) {
    KATANA_LOG_FATAL("test failed: {}", res.error());
  }

  if (auto fini_good = tsuba::Fini(); !fini_good) {
    KATANA_LOG_FATAL("tsuba::Fini: {}", fini_good.error());
  }

  return 0;
}
