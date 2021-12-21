#ifndef KATANA_LIBGRAPH_STORAGEFORMATVERSIONOPTIONALTOPOLOGIES_H_
#define KATANA_LIBGRAPH_STORAGEFORMATVERSIONOPTIONALTOPOLOGIES_H_

#include "katana/Logging.h"
#include "katana/PropertyGraph.h"
#include "storage-format-version.h"

template <typename View>
void
verify_view(View generated_view, View loaded_view) {
  KATANA_LOG_ASSERT(generated_view.num_edges() == loaded_view.num_edges());
  KATANA_LOG_ASSERT(generated_view.num_nodes() == loaded_view.num_nodes());

  auto beg_edge = katana::make_zip_iterator(
      generated_view.all_edges().begin(), loaded_view.all_edges().begin());
  auto end_edge = katana::make_zip_iterator(
      generated_view.all_edges().end(), loaded_view.all_edges().end());

  for (auto i = beg_edge; i != end_edge; i++) {
    KATANA_LOG_ASSERT(std::get<0>(*i) == std::get<1>(*i));
  }

  auto beg_node = katana::make_zip_iterator(
      generated_view.all_nodes().begin(), loaded_view.all_nodes().begin());
  auto end_node = katana::make_zip_iterator(
      generated_view.all_nodes().end(), loaded_view.all_nodes().end());

  for (auto i = beg_node; i != end_node; i++) {
    KATANA_LOG_ASSERT(std::get<0>(*i) == std::get<1>(*i));
  }
}

void
TestOptionalTopologyStorageEdgeShuffleTopology(std::string inputFile) {
  KATANA_LOG_WARN("***** Testing EdgeShuffleTopology *****");

  tsuba::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(inputFile, &txn_ctx);

  // Build a EdgeSortedByDestID view, which uses GraphTopology EdgeShuffleTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgesSortedByDestID;

  SortedGraphView generated_sorted_view = pg.BuildView<SortedGraphView>();
  // TODO: ensure this view was generated, not loaded
  // generated_sorted_view.Print();

  std::string g2_rdg_file = StoreGraph(&pg);
  katana::PropertyGraph pg2 = LoadGraph(g2_rdg_file, &txn_ctx);

  SortedGraphView loaded_sorted_view = pg2.BuildView<SortedGraphView>();

  //TODO: emcginnis need some way to verify we loaded this view, vs just generating it again

  verify_view(generated_sorted_view, loaded_sorted_view);
}

void
TestOptionalTopologyStorageShuffleTopology(std::string inputFile) {
  KATANA_LOG_WARN("***** Testing ShuffleTopology *****");

  tsuba::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(inputFile, &txn_ctx);

  // Build a NodesSortedByDegreeEdgesSortedByDestID view, which uses GraphTopology ShuffleTopology in the background
  using SortedGraphView =
      katana::PropertyGraphViews::NodesSortedByDegreeEdgesSortedByDestID;

  SortedGraphView generated_sorted_view = pg.BuildView<SortedGraphView>();
  // TODO: ensure this view was generated, not loaded
  // generated_sorted_view.Print();

  std::string g2_rdg_file = StoreGraph(&pg);
  katana::PropertyGraph pg2 = LoadGraph(g2_rdg_file, &txn_ctx);

  SortedGraphView loaded_sorted_view = pg2.BuildView<SortedGraphView>();

  //TODO: emcginnis need some way to verify we loaded this view, vs just generating it again

  verify_view(generated_sorted_view, loaded_sorted_view);
}

void
TestOptionalTopologyStorageEdgeTypeAwareTopology(std::string inputFile) {
  KATANA_LOG_WARN("***** Testing EdgeTypeAware Topology *****");

  tsuba::TxnContext txn_ctx;
  katana::PropertyGraph pg = LoadGraph(inputFile, &txn_ctx);

  // Build a EdgeTypeAwareBiDir view, which uses GraphTopology EdgeTypeAwareTopology in the background
  using SortedGraphView = katana::PropertyGraphViews::EdgeTypeAwareBiDir;

  SortedGraphView generated_sorted_view = pg.BuildView<SortedGraphView>();
  // generated_sorted_view.Print();

  std::string g2_rdg_file = StoreGraph(&pg);
  katana::PropertyGraph pg2 = LoadGraph(g2_rdg_file, &txn_ctx);

  SortedGraphView loaded_sorted_view = pg2.BuildView<SortedGraphView>();

  //TODO: emcginnis need some way to verify we loaded this view, vs just generating it again

  verify_view(generated_sorted_view, loaded_sorted_view);
}

#endif
