/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

#include "katana/analytics/k_shortest_paths/ksssp.h"

#include "katana/AtomicHelpers.h"
#include "katana/ParquetWriter.h"
#include "katana/Reduction.h"
#include "katana/Statistics.h"
#include "katana/TypedPropertyGraph.h"
#include "katana/analytics/BfsSsspImplementationBase.h"
#include "katana/analytics/kSsspImplementationBase.h"
#include "katana/gstl.h"

using namespace katana::analytics;

typedef uint32_t Parent;

struct Path {
  Parent parent;
  const Path* last{nullptr};
};

struct NodeCount : public katana::AtomicPODProperty<uint32_t> {};

template <typename Weight>
struct NodeMax : public katana::AtomicPODProperty<Weight> {};

template <typename Weight>
using EdgeWeight = katana::PODProperty<Weight>;

template <typename Weight>
using NodeData = std::tuple<NodeCount, NodeMax<Weight>>;
template <typename Weight>
using EdgeData = std::tuple<EdgeWeight<Weight>>;

constexpr static const bool kTrackWork = false;
constexpr static const unsigned kChunkSize = 64U;

using PSchunk = katana::PerSocketChunkFIFO<kChunkSize>;

class PathAlloc {
public:
  Path* NewPath() {
    Path* path = allocator_.allocate(1);
    allocator_.construct(path, Path());
    return path;
  }

  void DeletePath(Path* path) {
    allocator_.destroy(path);
    allocator_.deallocate(path, 1);
  }

private:
  katana::FixedSizeAllocator<Path> allocator_;
};

/**
 * Checks if source node can reach report
 * for asynchronous graphs
 *
 * @param graph typed graph
 * @param source Beginning node in graph
 * @param report Final node to look for
 * @param push_wrap Function to get the next updated path
 * @param edge_range Range of edge nodes to explore
 */
template <
    typename GraphTy, typename Item, typename PushWrap, typename EdgeRange>
bool
CheckReachabilityAsync(
    GraphTy* graph, const typename GraphTy::Node& source,
    const typename GraphTy::Node& report, const PushWrap& push_wrap,
    const EdgeRange& edge_range) {
  using FIFO = katana::PerSocketChunkFIFO<kChunkSize>;
  using WL = FIFO;

  using Loop = katana::ForEach;

  using GNode = typename GraphTy::Node;

  Loop loop;

  graph->template GetData<NodeCount>(source) = 1;
  katana::InsertBag<Item> initBag;

  push_wrap(initBag, source, 1, "parallel");

  loop(
      katana::iterate(initBag),
      [&](const Item& item, auto& ctx) {
        for (auto ii : edge_range(item)) {
          GNode dst = EdgeDst(*graph, ii);
          if (graph->template GetData<NodeCount>(dst) == 0) {
            graph->template GetData<NodeCount>(dst) = 1;
            push_wrap(ctx, dst, 1);
          }
        }
      },
      katana::wl<WL>(), katana::loopname("runBFS"),
      katana::disable_conflict_detection());

  if (graph->template GetData<NodeCount>(report) == 0) {
    return false;
  }

  katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
    graph->template GetData<NodeCount>(n) = 0;
  });

  return true;
}

/**
 * Checks if source node can reach report
 * for synchronous graphs
 *
 * @param graph typed graph
 * @param source Beginning node in graph
 * @param report Final node to look for
 */
template <typename GraphTy>
bool
CheckReachabilitySync(
    GraphTy* graph, const typename GraphTy::Node& source,
    const typename GraphTy::Node& report) {
  using GNode = typename GraphTy::Node;

  katana::InsertBag<GNode> current_bag;
  katana::InsertBag<GNode> next_bag;

  current_bag.push(source);
  graph->template GetData<NodeCount>(source) = 1;

  while (current_bag.begin() != current_bag.end()) {
    katana::do_all(
        katana::iterate(current_bag),
        [&](GNode n) {
          for (auto edge : Edges(*graph, n)) {
            auto dest = EdgeDst(*graph, edge);
            if (graph->template GetData<NodeCount>(dest) == 0) {
              graph->template GetData<NodeCount>(dest) = 1;
              next_bag.push(dest);
            }
          }
        },
        katana::steal());

    current_bag.clear();
    std::swap(current_bag, next_bag);
  }

  if (graph->template GetData<NodeCount>(report) == 0) {
    return false;
  }

  katana::do_all(katana::iterate(*graph), [&graph](GNode n) {
    graph->template GetData<NodeCount>(n) = 0;
  });

  return true;
}

/**
 * Checks if source node can reach report
 * for asynchronous graphs
 *
 * @param graph typed graph
 * @param source Beginning node in graph
 * @param report Final node to look for
 * @param push_wrap Function to get the next updated path
 * @param edge_range Range of edge nodes to explore
 * @param report_paths_bag Total paths (and weights) from source to report
 * @param path_pointers Pointers for each path
 * @param path_alloc Allocates paths in graph
 * @param num_paths Number of paths to look for
 * @param step_shift Shift value for deltastep
 */
template <
    typename GraphTy, typename Weight, typename Item, typename OBIMTy,
    typename PushWrap, typename EdgeRange>
void
DeltaStepAlgo(
    GraphTy* graph, const typename GraphTy::Node& source,
    const typename GraphTy::Node& report, const PushWrap& push_wrap,
    const EdgeRange& edge_range,
    katana::InsertBag<std::pair<Weight, Path*>>* report_paths_bag,
    katana::InsertBag<Path*>* path_pointers, PathAlloc& path_alloc,
    size_t num_paths, uint32_t step_shift) {
  using GNode = typename GraphTy::Node;

  using kSSSP = katana::analytics::KSsspImplementationBase<
      GraphTy, Weight, const Path, true>;
  using kSSSPUpdateRequestIndexer = typename kSSSP::UpdateRequestIndexer;

  using OBIM =
      katana::OrderedByIntegerMetric<kSSSPUpdateRequestIndexer, PSchunk>;

  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> bad_work;
  //! [reducible for self-defined stats]
  katana::GAccumulator<size_t> wl_empty_work;

  graph->template GetData<NodeCount>(source) = 1;

  katana::InsertBag<Item> init_bag;

  Path* path = path_alloc.NewPath();
  path->last = nullptr;
  path->parent = source;

  path_pointers->push(path);

  push_wrap(init_bag, source, 0, path, "parallel");

  katana::for_each(
      katana::iterate(init_bag),
      [&](const Item& item, auto& ctx) {
        for (auto ii : edge_range(item)) {
          GNode dst = EdgeDst(*graph, ii);
          auto& ddata_count = graph->template GetData<NodeCount>(dst);
          auto& ddata_max = graph->template GetData<NodeMax<Weight>>(dst);

          Weight ew = graph->template GetEdgeData<EdgeWeight<Weight>>(ii);
          const Weight new_dist = item.distance + ew;

          if ((ddata_count >= num_paths) && (ddata_max <= new_dist))
            continue;

          Path* path = path_alloc.NewPath();
          path->parent = item.src;
          path->last = item.path;
          path_pointers->push(path);

          if (ddata_count < num_paths) {
            katana::atomicAdd<uint32_t>(ddata_count, (Weight)1);
            katana::atomicMax<Weight>(ddata_max, new_dist);
          }

          if (dst == report) {
            report_paths_bag->push(std::make_pair(new_dist, path));
          }

          //check if this new extended path needs to be added to the worklist
          bool should_add =
              (graph->template GetData<NodeCount>(report) < num_paths) ||
              ((graph->template GetData<NodeCount>(report) >= num_paths) &&
               (graph->template GetData<NodeMax<Weight>>(report) > new_dist));

          if (should_add) {
            const Path* const_path = path;
            push_wrap(ctx, dst, new_dist, const_path);
          }
        }
      },
      katana::wl<OBIM>(kSSSPUpdateRequestIndexer{step_shift}),
      katana::disable_conflict_detection(), katana::loopname("kSSSP"));

  if (kTrackWork) {
    //! [report self-defined stats]
    katana::ReportStatSingle("kSSSP", "BadWork", bad_work.reduce());
    //! [report self-defined stats]
    katana::ReportStatSingle("kSSSP", "WLEmptyWork", wl_empty_work.reduce());
  }
}

katana::Result<void>
GetPath(const Path* path, arrow::UInt64Builder& builder) {
  if (path->last->last != nullptr) {
    GetPath(path->last, builder);
  }

  KATANA_CHECKED(builder.Append(path->parent));
}

/**
 * Sets up and runs implementation of ksssp
 *
 * @param graph typed graph
 * @param start_node Beginning node in graph
 * @param report_node Final node to look for
 * @param num_paths Number of paths to look for
 * @param plan Algorithm to get path
 */
template <typename GraphTy, typename Weight>
katana::Result<std::shared_ptr<arrow::Table>>
KssspImpl(
    GraphTy graph, size_t start_node, size_t report_node, size_t num_paths,
    KssspPlan plan) {
  using GNode = typename GraphTy::Node;

  using kSSSP = katana::analytics::KSsspImplementationBase<
      GraphTy, Weight, const Path, true>;
  using kSSSPUpdateRequest = typename kSSSP::UpdateRequest;
  using kSSSPUpdateRequestIndexer = typename kSSSP::UpdateRequestIndexer;
  using kSSSPSrcEdgeTile = typename kSSSP::SrcEdgeTile;
  using kSSSPSrcEdgeTilePushWrap = typename kSSSP::SrcEdgeTilePushWrap;
  using kSSSPReqPushWrap = typename kSSSP::ReqPushWrap;
  using kSSSPOutEdgeRangeFn = typename kSSSP::OutEdgeRangeFn;
  using kSSSPTileRangeFn = typename kSSSP::TileRangeFn;

  using OBIM =
      katana::OrderedByIntegerMetric<kSSSPUpdateRequestIndexer, PSchunk>;
  using OBIM_Barrier = typename katana::OrderedByIntegerMetric<
      kSSSPUpdateRequestIndexer, PSchunk>::template with_barrier<true>::type;

  using BFS = BfsSsspImplementationBase<GraphTy, unsigned int, false>;
  using BFSUpdateRequest = typename BFS::UpdateRequest;
  using BFSReqPushWrap = typename BFS::ReqPushWrap;
  using BFSOutEdgeRangeFn = typename BFS::OutEdgeRangeFnUndirected;

  auto it = graph.begin();
  std::advance(it, start_node);
  GNode source = *it;
  it = graph.begin();
  std::advance(it, report_node);
  GNode report = *it;

  size_t approxNodeData = graph.size() * 64;
  katana::Prealloc(1, approxNodeData);
  katana::ReportPageAllocGuard page_alloc;

  katana::do_all(katana::iterate(graph), [&](const GNode& n) {
    graph.template GetData<NodeMax<Weight>>(n) = 0;
    graph.template GetData<NodeCount>(n) = 0;
  });

  katana::StatTimer execTime("kSSSP");
  execTime.start();

  katana::InsertBag<std::pair<Weight, Path*>> paths;
  katana::InsertBag<Path*> path_pointers;

  bool reachable = true;

  switch (plan.reachability()) {
  case KssspPlan::asyncLevel:
    reachable = CheckReachabilityAsync<GraphTy, BFSUpdateRequest>(
        &graph, source, report, BFSReqPushWrap(), BFSOutEdgeRangeFn{&graph});
    break;
  case KssspPlan::syncLevel:
    reachable = CheckReachabilitySync<GraphTy>(&graph, source, report);
    break;
  default:
    std::abort();
  }

  PathAlloc path_alloc;

  if (reachable) {
    switch (plan.algorithm()) {
    case KssspPlan::kDeltaTile:
      DeltaStepAlgo<GraphTy, Weight, kSSSPSrcEdgeTile, OBIM>(
          &graph, source, report, kSSSPSrcEdgeTilePushWrap{&graph},
          kSSSPTileRangeFn(), &paths, &path_pointers, path_alloc, num_paths,
          plan.delta());
      break;
    case KssspPlan::kDeltaStep:
      DeltaStepAlgo<GraphTy, Weight, kSSSPUpdateRequest, OBIM>(
          &graph, source, report, kSSSPReqPushWrap(),
          kSSSPOutEdgeRangeFn{&graph}, &paths, &path_pointers, path_alloc,
          num_paths, plan.delta());
      break;
    case KssspPlan::kDeltaStepBarrier:
      katana::gInfo("Using OBIM with barrier\n");
      DeltaStepAlgo<GraphTy, Weight, kSSSPUpdateRequest, OBIM_Barrier>(
          &graph, source, report, kSSSPReqPushWrap(),
          kSSSPOutEdgeRangeFn{&graph}, &paths, &path_pointers, path_alloc,
          num_paths, plan.delta());
      break;

    default:
      return katana::ErrorCode::InvalidArgument;
    }
  }

  execTime.stop();
  page_alloc.Report();

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("path", arrow::large_list(arrow::uint64()))};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::shared_ptr<arrow::Array> arr = {};

  if (reachable) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    KATANA_CHECKED(arrow::MakeBuilder(
        arrow::default_memory_pool(), arrow::large_list(arrow::uint64()),
        &builder));
    auto& outer_builder = dynamic_cast<arrow::LargeListBuilder&>(*builder);
    auto& inner_builder =
        dynamic_cast<arrow::UInt64Builder&>(*(outer_builder.value_builder()));

    for (auto pair : paths) {
      KATANA_CHECKED(outer_builder.Append());

      GetPath(pair.second, inner_builder);
      KATANA_CHECKED(inner_builder.Append(report));
    }

    arr = KATANA_CHECKED(builder->Finish());

    katana::do_all(katana::iterate(path_pointers), [&](Path* p) {
      path_alloc.DeletePath(p);
    });
  }

  return arrow::Table::Make(schema, {arr});
}

/**
 * Wrapper for ksssp that sets up and runs either a symmetric or asymmetric graph
 *
 * @param pg property graph
 * @param edge_weight_property_name edge weights
 * @param start_node Beginning node in graph
 * @param report_node Final node to look for
 * @param num_paths Number of paths to look for
 * @param is_symmetric Whether or not the path is symmetric
 * @param plan Algorithm to get path
 */
template <typename Weight>
katana::Result<std::shared_ptr<arrow::Table>>
kSSSPWithWrap(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    size_t start_node, size_t report_node, size_t num_paths,
    const bool& is_symmetric, katana::TxnContext* txn_ctx, KssspPlan plan) {
  static_assert(std::is_integral_v<Weight> || std::is_floating_point_v<Weight>);

  std::vector<TemporaryPropertyGuard> temp_node_properties(2);
  std::generate_n(
      temp_node_properties.begin(), temp_node_properties.size(),
      [&]() { return TemporaryPropertyGuard{pg->NodeMutablePropertyView()}; });
  std::vector<std::string> temp_node_property_names(
      temp_node_properties.size());
  std::transform(
      temp_node_properties.begin(), temp_node_properties.end(),
      temp_node_property_names.begin(),
      [](const TemporaryPropertyGuard& p) { return p.name(); });

  KATANA_CHECKED(pg->ConstructNodeProperties<NodeData<Weight>>(
      txn_ctx, temp_node_property_names));

  if (is_symmetric) {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Default, NodeData<Weight>,
        EdgeData<Weight>>;
    Graph graph = KATANA_CHECKED(
        Graph::Make(pg, temp_node_property_names, {edge_weight_property_name}));

    return KssspImpl<Graph, Weight>(
        graph, start_node, report_node, num_paths, plan);
  } else {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Undirected, NodeData<Weight>,
        EdgeData<Weight>>;

    Graph graph = KATANA_CHECKED(
        Graph::Make(pg, temp_node_property_names, {edge_weight_property_name}));

    return KssspImpl<Graph, Weight>(
        graph, start_node, report_node, num_paths, plan);
  }
}

/**
 * Runs a ksssp algorithm based on its weight
 *
 * @param pg property graph
 * @param edge_weight_property_name edge weights
 * @param start_node Beginning node in graph
 * @param report_node Final node to look for
 * @param num_paths Number of paths to look for
 * @param is_symmetric Whether or not the path is symmetric
 * @param plan Algorithm to get path
 */
katana::Result<std::shared_ptr<arrow::Table>>
katana::analytics::Ksssp(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    size_t start_node, size_t report_node, size_t num_paths,
    const bool& is_symmetric, katana::TxnContext* txn_ctx, KssspPlan plan) {
  if (!edge_weight_property_name.empty() &&
      !pg->HasEdgeProperty(edge_weight_property_name)) {
    return KATANA_ERROR(
        katana::ErrorCode::NotFound, "Edge Property: {} Not found",
        edge_weight_property_name);
  }

  if (edge_weight_property_name.empty()) {
    TemporaryPropertyGuard temporary_edge_property{
        pg->EdgeMutablePropertyView()};
    using EdgeWeightType = int64_t;
    KATANA_CHECKED(katana::analytics::AddDefaultEdgeWeight<EdgeWeightType>(
        pg, temporary_edge_property.name(), 1, txn_ctx));
    return kSSSPWithWrap<int64_t>(
        pg, temporary_edge_property.name(), start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  }

  switch (KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
              ->type()
              ->id()) {
  case arrow::UInt32Type::type_id:
    return kSSSPWithWrap<uint32_t>(
        pg, edge_weight_property_name, start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  case arrow::Int32Type::type_id:
    return kSSSPWithWrap<int32_t>(
        pg, edge_weight_property_name, start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  case arrow::UInt64Type::type_id:
    return kSSSPWithWrap<uint64_t>(
        pg, edge_weight_property_name, start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  case arrow::Int64Type::type_id:
    return kSSSPWithWrap<int64_t>(
        pg, edge_weight_property_name, start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  case arrow::FloatType::type_id:
    return kSSSPWithWrap<float>(
        pg, edge_weight_property_name, start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  case arrow::DoubleType::type_id:
    return kSSSPWithWrap<double>(
        pg, edge_weight_property_name, start_node, report_node, num_paths,
        is_symmetric, txn_ctx, plan);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
}

void
katana::analytics::KssspStatistics::Print(std::ostream& os) const {
  os << "Node " << report_node << " has these k paths:" << std::endl;
  for (katana::analytics::KssspStatistics::PathStats path : paths) {
    for (uint64_t node : path.path) {
      os << " " << node;
    }
    os << std::endl << "Weight: " << path.weight << std::endl;
  }
}

template <typename GraphTy, typename Weight>
katana::Result<katana::analytics::KssspStatistics>
ComputeStatistics(
    GraphTy graph, std::shared_ptr<arrow::Table> table, size_t report_node) {
  std::vector<katana::analytics::KssspStatistics::PathStats> paths = {};
  auto node_list =
      std::static_pointer_cast<arrow::ListArray>(table->column(0)->chunk(0));
  auto all_nodes =
      std::static_pointer_cast<arrow::UInt64Array>(node_list->values());
  int64_t i = 0;
  uint64_t j = 0;
  while (i < table->num_rows()) {
    std::vector<uint64_t> path = {};
    katana::GAccumulator<Weight> weight;
    while (all_nodes->Value(j) != report_node) {
      path.push_back(all_nodes->Value(j));
      weight +=
          graph.template GetEdgeData<EdgeWeight<Weight>>(all_nodes->Value(j));
      j++;
    }
    path.push_back(report_node);
    paths.push_back({path, double(weight.reduce())});
    i++;
    j++;
  }

  return KssspStatistics{paths, report_node};
}

template <typename Weight>
katana::Result<katana::analytics::KssspStatistics>
ComputeWithWrap(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    std::shared_ptr<arrow::Table> table, size_t report_node,
    const bool& is_symmetric, katana::TxnContext* txn_ctx) {
  static_assert(std::is_integral_v<Weight> || std::is_floating_point_v<Weight>);

  std::vector<TemporaryPropertyGuard> temp_node_properties(2);
  std::generate_n(
      temp_node_properties.begin(), temp_node_properties.size(),
      [&]() { return TemporaryPropertyGuard{pg->NodeMutablePropertyView()}; });
  std::vector<std::string> temp_node_property_names(
      temp_node_properties.size());
  std::transform(
      temp_node_properties.begin(), temp_node_properties.end(),
      temp_node_property_names.begin(),
      [](const TemporaryPropertyGuard& p) { return p.name(); });

  KATANA_CHECKED(pg->ConstructNodeProperties<NodeData<Weight>>(
      txn_ctx, temp_node_property_names));

  if (is_symmetric) {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Default, NodeData<Weight>,
        EdgeData<Weight>>;
    Graph graph = KATANA_CHECKED(
        Graph::Make(pg, temp_node_property_names, {edge_weight_property_name}));

    return ComputeStatistics<Graph, Weight>(graph, table, report_node);
  } else {
    using Graph = katana::TypedPropertyGraphView<
        katana::PropertyGraphViews::Undirected, NodeData<Weight>,
        EdgeData<Weight>>;

    Graph graph = KATANA_CHECKED(
        Graph::Make(pg, temp_node_property_names, {edge_weight_property_name}));

    return ComputeStatistics<Graph, Weight>(graph, table, report_node);
  }
}

katana::Result<katana::analytics::KssspStatistics>
katana::analytics::KssspStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& edge_weight_property_name,
    std::shared_ptr<arrow::Table> table, size_t report_node,
    const bool& is_symmetric, katana::TxnContext* txn_ctx) {
  if (!edge_weight_property_name.empty() &&
      !pg->HasEdgeProperty(edge_weight_property_name)) {
    return KATANA_ERROR(
        katana::ErrorCode::NotFound, "Edge Property: {} Not found",
        edge_weight_property_name);
  }

  if (edge_weight_property_name.empty()) {
    TemporaryPropertyGuard temporary_edge_property{
        pg->EdgeMutablePropertyView()};
    using EdgeWeightType = int64_t;
    KATANA_CHECKED(katana::analytics::AddDefaultEdgeWeight<EdgeWeightType>(
        pg, temporary_edge_property.name(), 1, txn_ctx));
    return ComputeWithWrap<int64_t>(
        pg, temporary_edge_property.name(), table, report_node, is_symmetric,
        txn_ctx);
  }

  switch (KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
              ->type()
              ->id()) {
  case arrow::UInt32Type::type_id:
    return ComputeWithWrap<uint32_t>(
        pg, edge_weight_property_name, table, report_node, is_symmetric,
        txn_ctx);
  case arrow::Int32Type::type_id:
    return ComputeWithWrap<int32_t>(
        pg, edge_weight_property_name, table, report_node, is_symmetric,
        txn_ctx);
  case arrow::UInt64Type::type_id:
    return ComputeWithWrap<uint64_t>(
        pg, edge_weight_property_name, table, report_node, is_symmetric,
        txn_ctx);
  case arrow::Int64Type::type_id:
    return ComputeWithWrap<int64_t>(
        pg, edge_weight_property_name, table, report_node, is_symmetric,
        txn_ctx);
  case arrow::FloatType::type_id:
    return ComputeWithWrap<float>(
        pg, edge_weight_property_name, table, report_node, is_symmetric,
        txn_ctx);
  case arrow::DoubleType::type_id:
    return ComputeWithWrap<double>(
        pg, edge_weight_property_name, table, report_node, is_symmetric,
        txn_ctx);
  default:
    return KATANA_ERROR(
        katana::ErrorCode::TypeError, "Unsupported type: {}",
        KATANA_CHECKED(pg->GetEdgeProperty(edge_weight_property_name))
            ->type()
            ->ToString());
  }
}
