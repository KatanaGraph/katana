#include "galois/graphs/PropertyFileGraph.h"

#include <sys/mman.h>

#include "galois/ErrorCode.h"
#include "galois/Logging.h"
#include "galois/Platform.h"
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

galois::Result<std::shared_ptr<tsuba::FileFrame>>
WriteTopology(const galois::graphs::GraphTopology& topology) {
  auto ff = std::make_shared<tsuba::FileFrame>();
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
  return ff;
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
MakePropertyFileGraph(
    std::unique_ptr<tsuba::RDGFile> rdg_file,
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) {
  auto rdg_result =
      tsuba::RDG::Load(*rdg_file, &node_properties, &edge_properties);
  if (!rdg_result) {
    return rdg_result.error();
  }

  return galois::graphs::PropertyFileGraph::Make(
      std::move(rdg_file), std::move(rdg_result.value()));
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
MakePropertyFileGraph(std::unique_ptr<tsuba::RDGFile> rdg_file) {
  auto rdg_result = tsuba::RDG::Load(*rdg_file);
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
galois::graphs::PropertyFileGraph::DoWrite(tsuba::RDGHandle handle) {
  if (!rdg_.topology_file_storage_.Valid()) {
    auto result = WriteTopology(topology_);
    if (!result) {
      return result.error();
    }
    return rdg_.Store(handle, result.value().get());
  }

  return rdg_.Store(handle);
}

galois::Result<std::unique_ptr<galois::graphs::PropertyFileGraph>>
galois::graphs::PropertyFileGraph::Make(
    std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg) {
  auto g = std::unique_ptr<PropertyFileGraph>(
      new PropertyFileGraph(std::move(rdg_file), std::move(rdg)));

  auto load_result =
      LoadTopology(&g->topology_, g->rdg_.topology_file_storage_);
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
galois::graphs::PropertyFileGraph::Write() {
  return DoWrite(*file_);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::Write(const std::string& rdg_name) {
  if (auto res = tsuba::Create(rdg_name); !res) {
    return res.error();
  }
  auto open_res = tsuba::Open(rdg_name, tsuba::kReadWrite);
  if (!open_res) {
    return open_res.error();
  }
  auto new_file = std::make_unique<tsuba::RDGFile>(open_res.value());

  if (auto res = DoWrite(*new_file); !res) {
    return res.error();
  }

  file_ = std::move(new_file);

  return galois::ResultSuccess();
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
galois::graphs::PropertyFileGraph::RemoveNodeProperty(int i) {
  return rdg_.DropNodeProperty(i);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::RemoveEdgeProperty(int i) {
  return rdg_.DropEdgeProperty(i);
}

galois::Result<void>
galois::graphs::PropertyFileGraph::SetTopology(
    const galois::graphs::GraphTopology& topology) {
  if (auto res = rdg_.topology_file_storage_.Unbind(); !res) {
    return res.error();
  }
  topology_ = topology;

  return galois::ResultSuccess();
}

galois::Result<std::vector<arrow::Array*>>
galois::graphs::PropertyFileGraph::ExtractArrays(arrow::Table* table) {
  std::vector<arrow::Array*> ret;
  for (auto& column : table->columns()) {
    if (column->num_chunks() != 1) {
      return std::errc::invalid_argument;
    }
    ret.emplace_back(column->chunks()[0].get());
  }

  return ret;
}
