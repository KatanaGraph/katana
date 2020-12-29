#include "galois/graphs/PropertyFileGraph.h"

#include <sys/mman.h>

#include "galois/Logging.h"
#include "galois/Loops.h"
#include "galois/Platform.h"
#include "galois/Properties.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FileFrame.h"
#include "tsuba/RDG.h"
#include "tsuba/tsuba.h"

namespace {

constexpr uint64_t
GetGraphSize(uint64_t num_nodes, uint64_t num_edges) {
  /// version, sizeof_edge_data, num_nodes, num_edges
  constexpr int mandatory_fields = 4;

  return (mandatory_fields + num_nodes) * sizeof(uint64_t) +
         (num_edges * sizeof(uint32_t));
}

/// MapTopology takes a file buffer of a topology file and extracts the
/// topology files.
///
/// Format of a topology file (borrowed from the original FileGraph.cpp:
///
///   uint64_t version: 1
///   uint64_t sizeof_edge_data: size of edge data element
///   uint64_t num_nodes: number of nodes
///   uint64_t num_edges: number of edges
///   uint64_t[num_nodes] out_indices: start and end of the edges for a node
///   uint32_t[num_edges] out_dests: destinations (node indexes) of each edge
///   uint32_t padding if num_edges is odd
///   void*[num_edges] edge_data: edge data
///
/// Since property graphs store their edge data separately, we will consider
/// any topology file with non-zero sizeof_edge_data invalid.
galois::Result<galois::graphs::GraphTopology>
MapTopology(const tsuba::FileView& file_view) {
  const auto* data = file_view.ptr<uint64_t>();
  if (file_view.size() < 4) {
    return galois::ErrorCode::InvalidArgument;
  }

  if (data[0] != 1) {
    return galois::ErrorCode::InvalidArgument;
  }

  if (data[1] != 0) {
    return galois::ErrorCode::InvalidArgument;
  }

  uint64_t num_nodes = data[2];
  uint64_t num_edges = data[3];

  uint64_t expected_size = GetGraphSize(num_nodes, num_edges);

  if (file_view.size() < expected_size) {
    return galois::ErrorCode::InvalidArgument;
  }

  uint64_t* out_indices = const_cast<uint64_t*>(&data[4]);

  auto* out_dests = reinterpret_cast<uint32_t*>(out_indices + num_nodes);

  auto indices_buffer = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(out_indices), num_nodes);

  auto dests_buffer = std::make_shared<arrow::MutableBuffer>(
      reinterpret_cast<uint8_t*>(out_dests), num_edges);

  return galois::graphs::GraphTopology{
      .out_indices = std::make_shared<arrow::UInt64Array>(
          indices_buffer->size(), indices_buffer),
      .out_dests = std::make_shared<arrow::UInt32Array>(
          dests_buffer->size(), dests_buffer),
  };
}

galois::Result<void>
LoadTopology(
    galois::graphs::GraphTopology* topology,
    const tsuba::FileView& topology_file_storage) {
  auto map_result = MapTopology(topology_file_storage);
  if (!map_result) {
    return map_result.error();
  }
  *topology = std::move(map_result.value());

  return galois::ResultSuccess();
}

galois::Result<std::unique_ptr<tsuba::FileFrame>>
WriteTopology(const galois::graphs::GraphTopology& topology) {
  auto ff = std::make_unique<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error();
  }
  uint64_t num_nodes = topology.num_nodes();
  uint64_t num_edges = topology.num_edges();

  uint64_t data[4] = {1, 0, num_nodes, num_edges};
  arrow::Status aro_sts = ff->Write(&data, 4 * sizeof(uint64_t));
  if (!aro_sts.ok()) {
    return tsuba::ArrowToTsuba(aro_sts.code());
  }

  if (num_nodes) {
    const auto* raw = topology.out_indices->raw_values();
    static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint64_t>);
    auto buf = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(raw), num_nodes * sizeof(uint64_t));
    aro_sts = ff->Write(buf);
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }
  }

  if (num_edges) {
    const auto* raw = topology.out_dests->raw_values();
    static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint32_t>);
    auto buf = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(raw), num_edges * sizeof(uint32_t));
    aro_sts = ff->Write(buf);
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }
  }
  return std::unique_ptr<tsuba::FileFrame>(std::move(ff));
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
MakePropertyFileGraph(
    std::unique_ptr<tsuba::RDGFile> rdg_file,
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {
  auto rdg_result =
      tsuba::RDG::Make(*rdg_file, &node_properties, &edge_properties);
  if (!rdg_result) {
    return rdg_result.error();
  }

  return galois::graphs::PropertyFileGraph::Make(
      std::move(rdg_file), std::move(rdg_result.value()));
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
MakePropertyFileGraph(std::unique_ptr<tsuba::RDGFile> rdg_file) {
  auto rdg_result = tsuba::RDG::Make(*rdg_file);
  if (!rdg_result) {
    return rdg_result.error();
  }

  return galois::graphs::PropertyFileGraph::Make(
      std::move(rdg_file), std::move(rdg_result.value()));
}

}  // namespace

galois::graphs::PropertyFileGraph::PropertyFileGraph() = default;

galois::graphs::PropertyFileGraph::PropertyFileGraph(
    std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg)
    : rdg_(std::move(rdg)), file_(std::move(rdg_file)) {}

galois::Result<void>
galois::graphs::PropertyFileGraph::Validate() {
  // TODO (thunt) check that arrow table sizes match topology
  // if (topology_.out_dests &&
  //    topology_.out_dests->length() != table->num_rows()) {
  //  return ErrorCode::InvalidArgument;
  //}
  // if (topology_.out_indices &&
  //    topology_.out_indices->length() != table->num_rows()) {
  //  return ErrorCode::InvalidArgument;
  //}
  return galois::ResultSuccess();
}

galois::Result<void>
galois::graphs::PropertyFileGraph::DoWrite(
    tsuba::RDGHandle handle, const std::string& command_line) {
  if (!rdg_.topology_file_storage().Valid()) {
    auto result = WriteTopology(topology_);
    if (!result) {
      return result.error();
    }
    return rdg_.Store(handle, command_line, std::move(result.value()));
  }

  return rdg_.Store(handle, command_line);
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(
    std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg) {
  auto g = std::unique_ptr<PropertyFileGraph>(
      new PropertyFileGraph(std::move(rdg_file), std::move(rdg)));

  auto load_result =
      LoadTopology(&g->topology_, g->rdg_.topology_file_storage());
  if (!load_result) {
    return load_result.error();
  }

  if (auto good = g->Validate(); !good) {
    return good.error();
  }
  return std::unique_ptr<PropertyFileGraph>(std::move(g));
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(const std::string& rdg_name) {
  auto handle = tsuba::Open(rdg_name, tsuba::kReadWrite);
  if (!handle) {
    return handle.error();
  }

  return MakePropertyFileGraph(
      std::make_unique<tsuba::RDGFile>(handle.value()));
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(
    const std::string& rdg_name,
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {
  auto handle = tsuba::Open(rdg_name, tsuba::kReadWrite);
  if (!handle) {
    return handle.error();
  }

  return MakePropertyFileGraph(
      std::make_unique<tsuba::RDGFile>(handle.value()), node_properties,
      edge_properties);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::WriteGraph(
    const std::string& uri, const std::string& command_line) {
  auto open_res = tsuba::Open(uri, tsuba::kReadWrite);
  if (!open_res) {
    return open_res.error();
  }
  auto new_file = std::make_unique<tsuba::RDGFile>(open_res.value());

  if (auto res = DoWrite(*new_file, command_line); !res) {
    return res.error();
  }

  file_ = std::move(new_file);

  return galois::ResultSuccess();
}

galois::Result<void>
galois::graphs::PropertyFileGraph::Commit(const std::string& command_line) {
  if (file_ == nullptr) {
    if (rdg_.rdg_dir().empty()) {
      GALOIS_LOG_ERROR("RDG commit but rdg_dir_ is empty");
      return ErrorCode::InvalidArgument;
    }
    return WriteGraph(rdg_.rdg_dir().string(), command_line);
  }
  return DoWrite(*file_, command_line);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::Write(
    const std::string& rdg_name, const std::string& command_line) {
  if (auto res = tsuba::Create(rdg_name); !res) {
    return res.error();
  }
  return WriteGraph(rdg_name, command_line);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& table) {
  if (topology_.out_indices &&
      topology_.out_indices->length() != table->num_rows()) {
    GALOIS_LOG_DEBUG(
        "expected {} rows found {} instead", topology_.out_indices->length(),
        table->num_rows());
    return ErrorCode::InvalidArgument;
  }
  return rdg_.AddNodeProperties(table);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& table) {
  if (topology_.out_dests &&
      topology_.out_dests->length() != table->num_rows()) {
    GALOIS_LOG_DEBUG(
        "expected {} rows found {} instead", topology_.out_dests->length(),
        table->num_rows());
    return ErrorCode::InvalidArgument;
  }
  return rdg_.AddEdgeProperties(table);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::SetTopology(
    const galois::graphs::GraphTopology& topology) {
  if (auto res = rdg_.UnbindTopologyFileStorage(); !res) {
    return res.error();
  }
  topology_ = topology;

  return galois::ResultSuccess();
}

galois::Result<std::vector<uint64_t>>
galois::graphs::SortAllEdgesByDest(galois::graphs::PropertyFileGraph* pfg) {
  auto view_result_dests =
      galois::ConstructPropertyView<galois::UInt32Property>(
          pfg->topology().out_dests.get());
  if (!view_result_dests) {
    return view_result_dests.error();
  }

  auto out_dests_view = std::move(view_result_dests.value());

  std::vector<uint64_t> permutation_vec(pfg->topology().num_edges());
  std::iota(permutation_vec.begin(), permutation_vec.end(), 0);
  auto comparator = [&](uint64_t a, uint64_t b) {
    return out_dests_view[a] < out_dests_view[b];
  };

  galois::do_all(
      galois::iterate(uint64_t{0}, pfg->topology().num_nodes()),
      [&](uint64_t n) {
        auto edge_range = pfg->topology().edge_range(n);
        std::sort(
            permutation_vec.begin() + edge_range.first,
            permutation_vec.begin() + edge_range.second, comparator);
        std::sort(
            &out_dests_view[0] + edge_range.first,
            &out_dests_view[0] + edge_range.second);
      },
      galois::steal());

  return permutation_vec;
}

uint64_t
galois::graphs::FindEdgeSortedByDest(
    const galois::graphs::PropertyFileGraph& graph, uint32_t node,
    uint32_t node_to_find) {
  auto view_result_dests =
      galois::ConstructPropertyView<galois::UInt32Property>(
          graph.topology().out_dests.get());
  if (!view_result_dests) {
    GALOIS_LOG_FATAL(
        "Unable to construct property view on topology destinations : {}",
        view_result_dests.error());
  }

  auto out_dests_view = std::move(view_result_dests.value());

  auto edge_range = graph.topology().edge_range(node);
  using edge_iterator = boost::counting_iterator<uint64_t>;
  auto edge_matched = std::lower_bound(
      edge_iterator(edge_range.first), edge_iterator(edge_range.second),
      node_to_find,
      [=](edge_iterator e, uint32_t n) { return out_dests_view[*e] < n; });

  return (
      out_dests_view[*edge_matched] == node_to_find ? *edge_matched
                                                    : edge_range.second);
}

galois::Result<void>
galois::graphs::SortNodesByDegree(galois::graphs::PropertyFileGraph* pfg) {
  uint64_t num_nodes = pfg->topology().num_nodes();
  uint64_t num_edges = pfg->topology().num_edges();

  using DegreeNodePair = std::pair<uint64_t, uint32_t>;
  std::vector<DegreeNodePair> dn_pairs(num_nodes);
  galois::do_all(galois::iterate(uint64_t{0}, num_nodes), [&](size_t node) {
    auto node_edge_range = pfg->topology().edge_range(node);
    size_t node_degree = node_edge_range.second - node_edge_range.first;
    dn_pairs[node] = DegreeNodePair(node_degree, node);
  });

  // sort by degree (first item)
  galois::ParallelSTL::sort(
      dn_pairs.begin(), dn_pairs.end(), std::greater<DegreeNodePair>());

  // create mapping, get degrees out to another vector to get prefix sum
  std::vector<uint32_t> old_to_new_mapping(num_nodes);
  galois::LargeArray<uint64_t> new_prefix_sum;
  new_prefix_sum.allocateBlocked(num_nodes);
  galois::do_all(galois::iterate(uint64_t{0}, num_nodes), [&](uint64_t index) {
    // save degree, which is pair.first
    new_prefix_sum[index] = dn_pairs[index].first;
    // save mapping; original index is in .second, map it to current index
    old_to_new_mapping[dn_pairs[index].second] = index;
  });

  galois::ParallelSTL::partial_sum(
      new_prefix_sum.begin(), new_prefix_sum.end(), new_prefix_sum.begin());

  galois::LargeArray<uint32_t> new_out_dest;
  new_out_dest.allocateBlocked(num_edges);

  auto view_result_indices =
      ConstructPropertyView<UInt64Property>(pfg->topology().out_indices.get());
  if (!view_result_indices) {
    return view_result_indices.error();
  }

  auto out_indices_view = std::move(view_result_indices.value());

  auto view_result_dests =
      ConstructPropertyView<UInt32Property>(pfg->topology().out_dests.get());
  if (!view_result_dests) {
    return view_result_dests.error();
  }

  auto out_dests_view = std::move(view_result_dests.value());

  galois::do_all(
      galois::iterate(uint64_t{0}, num_nodes),
      [&](uint32_t old_node_id) {
        uint32_t new_node_id = old_to_new_mapping[old_node_id];

        // get the start location of this reindex'd nodes edges
        uint64_t new_out_index =
            (new_node_id == 0) ? 0 : new_prefix_sum[new_node_id - 1];

        // construct the graph, reindexing as it goes along
        auto node_edge_range = pfg->topology().edge_range(old_node_id);
        for (auto e = node_edge_range.first; e != node_edge_range.second; ++e) {
          // get destination, reindex
          uint32_t old_edge_dest = out_dests_view[e];
          uint32_t new_edge_dest = old_to_new_mapping[old_edge_dest];

          new_out_dest[new_out_index] = new_edge_dest;

          new_out_index++;
        }
        // this assert makes sure reindex was correct + makes sure all edges
        // are accounted for
        assert(new_out_index == new_prefix_sum[new_node_id]);
      },
      galois::steal());

  //Update the underlying propertyFileGraph topology
  galois::do_all(
      galois::iterate(uint64_t{0}, num_nodes), [&](uint32_t node_id) {
        out_indices_view[node_id] = new_prefix_sum[node_id];
      });

  galois::do_all(
      galois::iterate(uint64_t{0}, num_edges), [&](uint32_t edge_id) {
        out_dests_view[edge_id] = new_out_dest[edge_id];
      });

  return galois::ResultSuccess();
}
