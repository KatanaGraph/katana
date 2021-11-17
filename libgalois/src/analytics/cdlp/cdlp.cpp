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

#include <boost/unordered_map.hpp>
#include "katana/analytics/cdlp/cdlp.h"

#include "katana/ArrowRandomAccessBuilder.h"
#include "katana/TypedPropertyGraph.h"

using namespace katana::analytics;

namespace {
const unsigned int kInfinity = std::numeric_limits<unsigned int>::max();

/// Limited number of iterations to limit the oscillation of the label
/// in Synchronous algorithm. We dont need to limit it in Asynchronous algorithm.
/// Set to 10 same as Graphalytics benchmark.
const unsigned int kMaxIterations = 10;	

struct CdlpSynchronousAlgo {
  using CommunityType = uint64_t;
  struct NodeCommunity : public katana::PODProperty<CommunityType> {};


  using NodeData = std::tuple<NodeCommunity>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using BiDirGraphView = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::BiDirectional, NodeData,
    EdgeData>;

  CdlpPlan& plan_;
  CdlpSynchronousAlgo(CdlpPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto &ndata = graph->GetData<NodeCommunity>(node);
	  ndata = node;
    });
  }

  void Deallocate(Graph*) {}

  void operator()(Graph* graph, const BiDirGraphView& bidir_view,
		          size_t max_iterations = kMaxIterations) {
	if (max_iterations==0) return;  
	
	struct NodeDataPair {
		GNode node;
		CommunityType data;
        NodeDataPair(GNode node, CommunityType data)
            : node(node), data(data) {}
	};

	size_t iterations = 0;
    katana::InsertBag<NodeDataPair> applyBag;
	

	/// FIXME: in this implementation, in each iteration, all the nodes are active
	/// for gather phase. If InsertBag does not accept duplicate items then this
	/// can be improved to have only the affected nodes to be active in next iteration 
	while (iterations < max_iterations){
		// Gather Phase
	    katana::do_all(
		    katana::iterate(*graph),
		    [&](const GNode& node) {
		        const auto ndata_current_comm = graph->GetData<NodeCommunity>(node);
			    typedef boost::unordered_map<CommunityType, size_t> Histogram_type;
			    Histogram_type histogram;
				// Incoming edges
			    for (auto e : bidir_view.in_edges(node)){
			        auto src = bidir_view.in_edge_dest(e);
			        const auto sdata = graph->GetData<NodeCommunity>(src);
					histogram[sdata]++;
			    }

				// Outgoing edges
			    for (auto e : bidir_view.edges(node)){
			        auto dest = bidir_view.edge_dest(e);
			        const auto ddata = graph->GetData<NodeCommunity>(dest);
					histogram[ddata]++;
			    }

				// Pick the most frequent communtiy as the new community for node
				// pick the smallest one if more than one max frequent exist.
				auto ndata_new_comm = ndata_current_comm;
				size_t best_freq = 0;
				for (Histogram_type::const_iterator it = histogram.begin(); it != histogram.end(); it++) {
				    const auto comm  = it->first;
					size_t freq = it->second;

					if (freq > best_freq || (freq == best_freq && comm < ndata_new_comm)) {
						ndata_new_comm = comm;
						best_freq = freq;
					}
				}
			
				if (ndata_new_comm != ndata_current_comm)
				    applyBag.push(NodeDataPair(node, (CommunityType) ndata_new_comm));
		    },
		    katana::loopname("CDLP_Gather"));
        
		// No change! break!
		if (applyBag.empty()) break;

		// Apply Phase
		katana::do_all(
			katana::iterate(applyBag),
			[&](const NodeDataPair nodeData){
			   GNode node = nodeData.node;
			   auto &ndata = graph->GetData<NodeCommunity>(node);
			   ndata = nodeData.data;
			},
			katana::loopname("CDLP_Apply"));

		applyBag.clear();
        iterations += 1;
	}
    katana::ReportStatSingle("CDLP_Synchronous", "iterations", iterations);
  }
};

struct CdlpAsynchronousAlgo {
  using CommunityType = uint64_t;
  struct NodeCommunity : public katana::PODProperty<CommunityType> {};

  using NodeData = std::tuple<NodeCommunity>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;
  using BiDirGraphView = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::BiDirectional, NodeData,
    EdgeData>;

  CdlpPlan& plan_;
  CdlpAsynchronousAlgo(CdlpPlan& plan)
      : plan_(plan) {}

  void Initialize(Graph* graph) {
    katana::do_all(katana::iterate(*graph), [&](const GNode& node) {
      auto &ndata = graph->GetData<NodeCommunity>(node);
	  ndata = node;
    });
  }

  void Deallocate(Graph*) {}

  void operator()(Graph*, const BiDirGraphView&,
		          size_t) {}
};

}  //namespace

template <typename Algorithm>
static katana::Result<void>
CdlpWithWrap(
    katana::PropertyGraph* pg, std::string output_property_name,
    CdlpPlan plan, size_t max_iterations) {
  katana::EnsurePreallocated(
      2,
      pg->topology().num_nodes() * sizeof(typename Algorithm::NodeCommunity));
  katana::ReportPageAllocGuard page_alloc;

  if (auto r = ConstructNodeProperties<
          std::tuple<typename Algorithm::NodeCommunity>>(
          pg, {output_property_name});
      !r) {
    return r.error();
  }
  auto pg_result = Algorithm::Graph::Make(pg, {output_property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }
  auto graph = pg_result.value();

  using BiDirGraphView = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::BiDirectional, typename Algorithm::NodeData,
    typename Algorithm::EdgeData>;

  auto bidir_view =
      KATANA_CHECKED(BiDirGraphView::Make(pg, {output_property_name}, {}));

  Algorithm algo(plan);

  algo.Initialize(&graph);

  katana::StatTimer execTime("CDLP");

  execTime.start();
  algo(&graph, bidir_view, max_iterations);
  execTime.stop();

  algo.Deallocate(&graph);
  return katana::ResultSuccess();
}

katana::Result<void>
katana::analytics::Cdlp(
    PropertyGraph* pg, const std::string& output_property_name,
    size_t max_iterations, CdlpPlan plan) {
  switch (plan.algorithm()) {
  case CdlpPlan::kSynchronous:
    return CdlpWithWrap<CdlpSynchronousAlgo>(
        pg, output_property_name, plan, max_iterations);
  /// TODO: Asynchronous Algorithm will be implemented later after Synchronous
  /// is done for both shared and distributed versions.
  /*
  case CdlpPlan::kAsynchronous:
    return CdlpWithWrap<CdlpAsynchronousAlgo>(
        pg, output_property_name, plan, max_iterations);
  */
  default:
    return ErrorCode::InvalidArgument;
  }
}

katana::Result<void>
katana::analytics::CdlpAssertValid(
    PropertyGraph* pg, const std::string& property_name) {
  using CommunityType = uint64_t;
  struct NodeCommunity : public katana::PODProperty<CommunityType> {};

  using NodeData = std::tuple<NodeCommunity>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  auto pg_result = Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  using BiDirGraphView = katana::TypedPropertyGraphView<
    katana::PropertyGraphViews::BiDirectional, NodeData,
    EdgeData>;

  auto bidir_view =
      KATANA_CHECKED(BiDirGraphView::Make(pg, {property_name}, {}));

  auto is_bad = [&graph, &bidir_view](const GNode& node) {
    const auto ndata = graph.template GetData<NodeCommunity>(node);
	typedef boost::unordered_map<CommunityType, size_t> Histogram_type;
	Histogram_type histogram;
	// Incoming edges
    for (auto e : bidir_view.in_edges(node)) {
      auto src = bidir_view.in_edge_dest(e);
      const auto sdata = graph.template GetData<NodeCommunity>(src);
	  histogram[sdata]++;
	}

	// Outgoing edges
	for (auto e : bidir_view.edges(node)){
		auto dest = bidir_view.edge_dest(e);
		const auto ddata = graph.template GetData<NodeCommunity>(dest);
		histogram[ddata]++;
	}

	/// Pick the most frequent communtiy for node
	/// Pick the smallest one if more than one max frequent exist.
	/// TODO: This needs to be fix for the oscillation of the label cases
	/// It returns Failed for those cases.
	auto ndata_correct = kInfinity;
	size_t best_freq = 0;
	for (Histogram_type::const_iterator it = histogram.begin(); it != histogram.end(); it++) {
		const auto comm  = it->first;
		size_t freq = it->second;

		if (freq > best_freq || (freq == best_freq && comm < ndata_correct)) {
			ndata_correct = comm;
			best_freq = freq;
		}
	}
			
	if (ndata_correct != ndata){
		KATANA_LOG_DEBUG(
            "{} (community: {}) must be in the most frequent community in its immediate neighborhood (community: "
            "{})",
            node, ndata, ndata_correct);
        return true;
    }
    return false;
  };

  if (katana::ParallelSTL::find_if(graph.begin(), graph.end(), is_bad) !=
      graph.end()) {
    return katana::ErrorCode::AssertionFailed;
  }

  return katana::ResultSuccess();
}

katana::Result<CdlpStatistics>
katana::analytics::CdlpStatistics::Compute(
    katana::PropertyGraph* pg, const std::string& property_name) {
  using CommunityType = uint64_t;
  struct NodeCommunity : public katana::PODProperty<CommunityType> {};

  using NodeData = std::tuple<NodeCommunity>;
  using EdgeData = std::tuple<>;
  typedef katana::TypedPropertyGraph<NodeData, EdgeData> Graph;
  typedef typename Graph::Node GNode;

  auto pg_result = Graph::Make(pg, {property_name}, {});
  if (!pg_result) {
    return pg_result.error();
  }

  auto graph = pg_result.value();

  using Map = katana::gstl::Map<CommunityType, int>;

  auto reduce = [](Map& lhs, Map&& rhs) -> Map& {
    Map v{std::move(rhs)};

    for (auto& kv : v) {
      if (lhs.count(kv.first) == 0) {
        lhs[kv.first] = 0;
      }
      lhs[kv.first] += kv.second;
    }

    return lhs;
  };

  auto mapIdentity = []() { return Map(); };

  auto accumMap = katana::make_reducible(reduce, mapIdentity);

  katana::do_all(
      katana::iterate(graph),
      [&](const GNode& x) {
        auto& n = graph.template GetData<NodeCommunity>(x);
        accumMap.update(Map{std::make_pair(n, 1)});
      },
      katana::loopname("CountLargest"));

  Map& map = accumMap.reduce();
  size_t reps = map.size();

  using CommunitySizePair = std::pair<CommunityType, int>;

  auto sizeMax = [](const CommunitySizePair& a, const CommunitySizePair& b) {
    if (a.second > b.second) {
      return a;
    }
    return b;
  };

  auto identity = []() { return CommunitySizePair{}; };

  auto maxComm = katana::make_reducible(sizeMax, identity);

  katana::GAccumulator<uint64_t> non_trivial_communities;
  katana::do_all(katana::iterate(map), [&](const CommunitySizePair& x) {
    maxComm.update(x);
    if (x.second > 1) {
      non_trivial_communities += 1;
    }
  });

  CommunitySizePair largest = maxComm.reduce();

  size_t largest_community_size = largest.second;
  double largest_community_ratio = 0;
  if (!graph.empty()) {
    largest_community_ratio = double(largest_community_size) / graph.size();
  }

  return CdlpStatistics{
      reps, non_trivial_communities.reduce(), largest_community_size,
      largest_community_ratio};
}

void
katana::analytics::CdlpStatistics::Print(
    std::ostream& os) const {
  os << "Total number of communities = " << total_communities << std::endl;
  os << "Total number of non trivial communities = "
     << total_non_trivial_communities << std::endl;
  os << "Number of nodes in the largest community = " << largest_community_size
     << std::endl;
  os << "Ratio of nodes in the largest community = " << largest_community_ratio
     << std::endl;
}
