#include "katana/PropertyGraph.h"

#include <sys/mman.h>

#include "katana/ArrowInterchange.h"
#include "katana/Logging.h"
#include "katana/Loops.h"
#include "katana/PerThreadStorage.h"
#include "katana/Platform.h"
#include "katana/Properties.h"
#include "katana/Result.h"
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

[[maybe_unused]] bool
CheckTopology(
    const uint64_t* out_indices, const uint64_t num_nodes,
    const uint32_t* out_dests, const uint64_t num_edges) {
  bool has_bad_adj = false;

  katana::do_all(
      katana::iterate(uint64_t{0}, num_nodes),
      [&](auto n) {
        if (out_indices[n] > num_edges) {
          has_bad_adj = true;
        }
      },
      katana::no_stats());

  bool has_bad_dest = false;
  katana::do_all(
      katana::iterate(uint64_t{0}, num_edges),
      [&](auto e) {
        if (out_dests[e] >= num_nodes) {
          has_bad_dest = true;
        }
      },
      katana::no_stats());

  return !has_bad_adj && !has_bad_dest;
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
/// Since property graphs store their edge data separately, we will
/// ignore the size_of_edge_data (data[1]).
katana::Result<katana::GraphTopology>
MapTopology(const tsuba::FileView& file_view) {
  const auto* data = file_view.ptr<uint64_t>();
  if (file_view.size() < 4) {
    return katana::ErrorCode::InvalidArgument;
  }

  if (data[0] != 1) {
    return katana::ErrorCode::InvalidArgument;
  }

  const uint64_t num_nodes = data[2];
  const uint64_t num_edges = data[3];

  uint64_t expected_size = GetGraphSize(num_nodes, num_edges);

  if (file_view.size() < expected_size) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "file_view size: {} expected {}",
        file_view.size(), expected_size);
  }

  const uint64_t* out_indices = &data[4];
  const uint32_t* out_dests =
      reinterpret_cast<const uint32_t*>(out_indices + num_nodes);

  KATANA_LOG_DEBUG_ASSERT(
      CheckTopology(out_indices, num_nodes, out_dests, num_edges));
  return katana::GraphTopology(out_indices, num_nodes, out_dests, num_edges);
}

katana::Result<std::unique_ptr<tsuba::FileFrame>>
WriteTopology(const katana::GraphTopology& topology) {
  auto ff = std::make_unique<tsuba::FileFrame>();
  if (auto res = ff->Init(); !res) {
    return res.error();
  }
  const uint64_t num_nodes = topology.num_nodes();
  const uint64_t num_edges = topology.num_edges();

  uint64_t data[4] = {1, 0, num_nodes, num_edges};
  arrow::Status aro_sts = ff->Write(&data, 4 * sizeof(uint64_t));
  if (!aro_sts.ok()) {
    return tsuba::ArrowToTsuba(aro_sts.code());
  }

  if (num_nodes) {
    const auto* raw = topology.adj_data();
    static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint64_t>);
    auto buf = arrow::Buffer::Wrap(raw, num_nodes);
    aro_sts = ff->Write(buf);
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }
  }

  if (num_edges) {
    const auto* raw = topology.dest_data();
    static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint32_t>);
    auto buf = arrow::Buffer::Wrap(raw, num_edges);
    aro_sts = ff->Write(buf);
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }
  }
  return std::unique_ptr<tsuba::FileFrame>(std::move(ff));
}

/// Assumes all boolean or uint8 properties are types
katana::Result<katana::NUMAArray<katana::PropertyGraph::TypeSetID>>
GetTypeSetIDsFromProperties(
    const std::shared_ptr<arrow::Table>& properties,
    katana::PropertyGraph::TypeNameToSetOfTypeSetIDsMap*
        type_name_to_type_set_ids,
    katana::PropertyGraph::TypeSetIDToSetOfTypeNamesMap*
        type_set_id_to_type_names) {
  // throw an error if each column/property has more than 1 chunk
  for (int i = 0, n = properties->num_columns(); i < n; i++) {
    std::shared_ptr<arrow::ChunkedArray> property = properties->column(i);
    if (property->num_chunks() != 1) {
      return KATANA_ERROR(
          katana::ErrorCode::NotImplemented,
          "property {} has {} chunks (1 chunk expected)",
          properties->schema()->field(i)->name(), property->num_chunks());
    }
  }

  // collect the list of types
  std::vector<int> type_field_indices;
  const std::shared_ptr<arrow::Schema>& schema = properties->schema();
  KATANA_LOG_DEBUG_ASSERT(schema->num_fields() == properties->num_columns());
  for (int i = 0, n = schema->num_fields(); i < n; i++) {
    const std::shared_ptr<arrow::Field>& current_field = schema->field(i);

    // a bool or uint8 property is (always) considered a type
    // TODO(roshan) make this customizable by the user
    if (current_field->type()->Equals(arrow::boolean()) ||
        current_field->type()->Equals(arrow::uint8())) {
      type_field_indices.push_back(i);
    }
  }

  // assign a new ID to each type
  // NB: cannot use unordered_map without defining a hash function for vectors;
  // performance is not affected here because the map is very small (<=256)
  std::map<katana::gstl::Vector<int>, katana::PropertyGraph::TypeSetID>
      type_field_indices_to_id;
  for (int i : type_field_indices) {
    katana::PropertyGraph::TypeSetID new_type_set_id =
        type_set_id_to_type_names->size();
    katana::gstl::Vector<int> field_indices = {i};
    type_field_indices_to_id.emplace(
        std::make_pair(field_indices, new_type_set_id));

    const std::shared_ptr<arrow::Field>& current_field = schema->field(i);
    const std::string& field_name = current_field->name();
    KATANA_LOG_DEBUG_ASSERT(
        type_name_to_type_set_ids->find(field_name) ==
        type_name_to_type_set_ids->end());

    katana::PropertyGraph::SetOfTypeSetIDs type_set_ids;
    type_set_ids.set(new_type_set_id);
    type_name_to_type_set_ids->emplace(
        std::make_pair(field_name, type_set_ids));
    type_set_id_to_type_names->push_back({field_name});
  }

  // collect the list of unique combination of types
  // NB: cannot use unordered_set without defining a hash function for vectors;
  // performance is not affected here because the set is very small (<=256)
  katana::gstl::Set<katana::gstl::Vector<int>> type_combinations;
  katana::PerThreadStorage<katana::gstl::Set<katana::gstl::Vector<int>>>
      type_combinations_pts;
  katana::do_all(
      katana::iterate(int64_t{0}, properties->num_rows()), [&](int64_t row) {
        katana::gstl::Vector<int> field_indices;
        for (int i : type_field_indices) {
          std::shared_ptr<arrow::Array> property =
              properties->column(i)->chunk(0);
          if (property->type()->Equals(arrow::boolean())) {
            auto bool_property =
                std::static_pointer_cast<arrow::BooleanArray>(property);
            if (bool_property->IsValid(row) && bool_property->Value(row)) {
              field_indices.emplace_back(i);
            }
          } else if (property->type()->Equals(arrow::uint8())) {
            auto uint8_property =
                std::static_pointer_cast<arrow::UInt8Array>(property);
            if (uint8_property->IsValid(row) && uint8_property->Value(row)) {
              field_indices.emplace_back(i);
            }
          }
        }
        if (field_indices.size() > 1) {
          katana::gstl::Set<katana::gstl::Vector<int>>&
              local_type_combinations = *type_combinations_pts.getLocal();
          local_type_combinations.emplace(field_indices);
        }
      });
  for (unsigned t = 0, n = katana::activeThreads; t < n; t++) {
    katana::gstl::Set<katana::gstl::Vector<int>>& remote_type_combinations =
        *type_combinations_pts.getRemote(t);
    for (auto& type_combination : remote_type_combinations) {
      type_combinations.emplace(type_combination);
    }
  }

  // assign a new ID to each unique combination of types
  for (const katana::gstl::Vector<int>& field_indices : type_combinations) {
    katana::PropertyGraph::TypeSetID new_type_set_id =
        type_set_id_to_type_names->size();
    type_field_indices_to_id.emplace(
        std::make_pair(field_indices, new_type_set_id));

    type_set_id_to_type_names->push_back({});
    for (int i : field_indices) {
      const std::shared_ptr<arrow::Field>& current_field = schema->field(i);
      const std::string& field_name = current_field->name();
      KATANA_LOG_DEBUG_ASSERT(
          type_name_to_type_set_ids->find(field_name) !=
          type_name_to_type_set_ids->end());

      type_name_to_type_set_ids->at(field_name).set(new_type_set_id);
      type_set_id_to_type_names->at(new_type_set_id).emplace(field_name);
    }
  }

  // assert that all type IDs and 2 special type IDs (unknown and invalid)
  // can be stored in 8 bits
  if (type_set_id_to_type_names->size() >
      (std::numeric_limits<katana::PropertyGraph::TypeSetID>::max() -
       size_t{2})) {
    return KATANA_ERROR(
        katana::ErrorCode::NotImplemented,
        "number of unique combination of types is {} "
        "but only up to {} is supported currently",
        type_set_id_to_type_names->size(),
        // exclude kUnknownType and kInvalidType
        std::numeric_limits<katana::PropertyGraph::TypeSetID>::max() - 2);
  }

  // allocate type IDs array
  katana::NUMAArray<katana::PropertyGraph::TypeSetID> type_set_ids;
  int64_t num_rows = properties->num_rows();
  type_set_ids.allocateInterleaved(num_rows);

  // assign the type ID for each row
  katana::do_all(katana::iterate(int64_t{0}, num_rows), [&](int64_t row) {
    katana::gstl::Vector<int> field_indices;
    for (int i : type_field_indices) {
      std::shared_ptr<arrow::Array> property = properties->column(i)->chunk(0);

      if (property->type()->Equals(arrow::boolean())) {
        auto bool_property =
            std::static_pointer_cast<arrow::BooleanArray>(property);
        if (bool_property->IsValid(row) && bool_property->Value(row)) {
          field_indices.emplace_back(i);
        }
      } else if (property->type()->Equals(arrow::uint8())) {
        auto uint8_property =
            std::static_pointer_cast<arrow::UInt8Array>(property);
        if (uint8_property->IsValid(row) && uint8_property->Value(row)) {
          field_indices.emplace_back(i);
        }
      }
    }
    if (field_indices.empty()) {
      type_set_ids[row] = katana::PropertyGraph::kUnknownType;
    } else {
      katana::PropertyGraph::TypeSetID type_set_id =
          type_field_indices_to_id.at(field_indices);
      type_set_ids[row] = type_set_id;
    }
  });

  return katana::Result<katana::NUMAArray<katana::PropertyGraph::TypeSetID>>(
      std::move(type_set_ids));
}

katana::NUMAArray<katana::PropertyGraph::TypeSetID>
GetUnknownTypeSetIDs(uint64_t num_rows) {
  katana::NUMAArray<katana::PropertyGraph::TypeSetID> type_set_ids;
  type_set_ids.allocateInterleaved(num_rows);
  katana::do_all(katana::iterate(uint64_t{0}, num_rows), [&](uint64_t row) {
    type_set_ids[row] = katana::PropertyGraph::kUnknownType;
  });
  return type_set_ids;
}

}  // namespace

katana::GraphTopology::GraphTopology(
    const Edge* adj_indices, size_t num_nodes, const Node* dests,
    size_t num_edges) noexcept {
  adj_indices_.allocateInterleaved(num_nodes);
  dests_.allocateInterleaved(num_edges);

  katana::ParallelSTL::copy(
      &adj_indices[0], &adj_indices[num_nodes], adj_indices_.begin());
  katana::ParallelSTL::copy(&dests[0], &dests[num_edges], dests_.begin());
}

katana::GraphTopology
katana::GraphTopology::Copy(const GraphTopology& that) noexcept {
  return katana::GraphTopology(
      that.adj_indices_.data(), that.adj_indices_.size(), that.dests_.data(),
      that.dests_.size());
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    std::unique_ptr<tsuba::RDGFile> rdg_file, tsuba::RDG&& rdg) {
  auto topo_result = MapTopology(rdg.topology_file_storage());
  if (!topo_result) {
    return topo_result.error();
  }

  return std::make_unique<PropertyGraph>(
      std::move(rdg_file), std::move(rdg), std::move(topo_result.value()));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
MakePropertyGraph(
    std::unique_ptr<tsuba::RDGFile> rdg_file,
    const tsuba::RDGLoadOptions& opts) {
  auto rdg_result = tsuba::RDG::Make(*rdg_file, opts);
  if (!rdg_result) {
    return rdg_result.error();
  }

  return katana::PropertyGraph::Make(
      std::move(rdg_file), std::move(rdg_result.value()));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(
    const std::string& rdg_name, const tsuba::RDGLoadOptions& opts) {
  auto handle = tsuba::Open(rdg_name, tsuba::kReadWrite);
  if (!handle) {
    return handle.error();
  }

  return MakePropertyGraph(
      std::make_unique<tsuba::RDGFile>(handle.value()), opts);
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Make(katana::GraphTopology&& topo_to_assign) {
  return std::make_unique<katana::PropertyGraph>(std::move(topo_to_assign));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Copy() const {
  return Copy(node_schema()->field_names(), edge_schema()->field_names());
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::PropertyGraph::Copy(
    const std::vector<std::string>& node_properties,
    const std::vector<std::string>& edge_properties) const {
  // TODO(gill): This should copy the RDG in memory without reloading from storage.
  tsuba::RDGLoadOptions opts;
  opts.partition_id_to_load = partition_id();
  opts.node_properties = node_properties;
  opts.edge_properties = edge_properties;

  return Make(rdg_dir(), opts);
}

katana::Result<void>
katana::PropertyGraph::Validate() {
  // TODO (thunt) check that arrow table sizes match topology
  // if (topology_.out_dests &&
  //    topology_.out_dests->length() != table->num_rows()) {
  //  return ErrorCode::InvalidArgument;
  //}
  // if (topology_.out_indices &&
  //    topology_.out_indices->length() != table->num_rows()) {
  //  return ErrorCode::InvalidArgument;
  //}

  uint64_t num_node_rows = static_cast<uint64_t>(node_properties()->num_rows());
  if (num_node_rows == 0) {
    if ((node_properties()->num_columns() != 0) && (num_nodes() != 0)) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "number of rows in node properties is 0 but "
          "the number of node properties is {} and the number of nodes is {}",
          node_properties()->num_columns(), num_nodes());
    }
  } else if (num_node_rows != num_nodes()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "number of rows in node properties {} differs "
        "from the number of nodes {}",
        node_properties()->num_rows(), num_nodes());
  }

  uint64_t num_edge_rows = static_cast<uint64_t>(edge_properties()->num_rows());
  if (num_edge_rows == 0) {
    if ((edge_properties()->num_columns() != 0) && (num_edges() != 0)) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed,
          "number of rows in edge properties is 0 but "
          "the number of edge properties is {} and the number of edges is {}",
          edge_properties()->num_columns(), num_edges());
    }
  } else if (num_edge_rows != num_edges()) {
    return KATANA_ERROR(
        ErrorCode::AssertionFailed,
        "number of rows in edge properties {} differs "
        "from the number of edges {}",
        edge_properties()->num_rows(), num_edges());
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::ConstructTypeSetIDs() {
  node_type_name_to_type_set_ids_.clear();
  node_type_set_id_to_type_names_.clear();
  edge_type_name_to_type_set_ids_.clear();
  edge_type_set_id_to_type_names_.clear();

  static_assert(kUnknownType == 0);
  node_type_set_id_to_type_names_.push_back(
      {});  // for kUnknownType: assumes it is 0
  uint64_t num_node_rows = static_cast<uint64_t>(node_properties()->num_rows());
  if (num_node_rows == 0) {
    node_type_set_id_ = GetUnknownTypeSetIDs(num_nodes());
  } else {
    auto node_types_res = GetTypeSetIDsFromProperties(
        node_properties(), &node_type_name_to_type_set_ids_,
        &node_type_set_id_to_type_names_);
    if (!node_types_res) {
      return node_types_res.error().WithContext("node properties");
    }
    node_type_set_id_ = std::move(node_types_res.value());
  }

  static_assert(kUnknownType == 0);
  edge_type_set_id_to_type_names_.push_back(
      {});  // for kUnknownType: assumes it is 0
  uint64_t num_edge_rows = static_cast<uint64_t>(edge_properties()->num_rows());
  if (num_edge_rows == 0) {
    edge_type_set_id_ = GetUnknownTypeSetIDs(num_edges());
  } else {
    auto edge_types_res = GetTypeSetIDsFromProperties(
        edge_properties(), &edge_type_name_to_type_set_ids_,
        &edge_type_set_id_to_type_names_);
    if (!edge_types_res) {
      return edge_types_res.error().WithContext("edge properties");
    }
    edge_type_set_id_ = std::move(edge_types_res.value());
  }

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::DoWrite(
    tsuba::RDGHandle handle, const std::string& command_line,
    tsuba::RDG::RDGVersioningPolicy versioning_action) {
  if (!rdg_.topology_file_storage().Valid()) {
    auto result = WriteTopology(topology());
    if (!result) {
      return result.error();
    }
    return rdg_.Store(
        handle, command_line, versioning_action, std::move(result.value()));
  }

  return rdg_.Store(handle, command_line, versioning_action);
}

katana::Result<void>
katana::PropertyGraph::ConductWriteOp(
    const std::string& uri, const std::string& command_line,
    tsuba::RDG::RDGVersioningPolicy versioning_action) {
  auto open_res = tsuba::Open(uri, tsuba::kReadWrite);
  if (!open_res) {
    return open_res.error();
  }
  auto new_file = std::make_unique<tsuba::RDGFile>(open_res.value());

  if (auto res = DoWrite(*new_file, command_line, versioning_action); !res) {
    return res.error();
  }

  file_ = std::move(new_file);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::PropertyGraph::WriteView(
    const std::string& uri, const std::string& command_line) {
  return ConductWriteOp(
      uri, command_line, tsuba::RDG::RDGVersioningPolicy::RetainVersion);
}

katana::Result<void>
katana::PropertyGraph::WriteGraph(
    const std::string& uri, const std::string& command_line) {
  return ConductWriteOp(
      uri, command_line, tsuba::RDG::RDGVersioningPolicy::IncrementVersion);
}

katana::Result<void>
katana::PropertyGraph::Commit(const std::string& command_line) {
  if (file_ == nullptr) {
    if (rdg_.rdg_dir().empty()) {
      return KATANA_ERROR(
          ErrorCode::InvalidArgument, "RDG commit but rdg_dir_ is empty");
    }
    return WriteGraph(rdg_.rdg_dir().string(), command_line);
  }
  return DoWrite(
      *file_, command_line, tsuba::RDG::RDGVersioningPolicy::IncrementVersion);
}

katana::Result<void>
katana::PropertyGraph::WriteView(const std::string& command_line) {
  // WriteView occurs once, and only before any Commit/Write operation
  KATANA_LOG_DEBUG_ASSERT(file_ == nullptr);
  return WriteView(rdg_.rdg_dir().string(), command_line);
}

bool
katana::PropertyGraph::Equals(const PropertyGraph* other) const {
  if (!topology().Equals(other->topology())) {
    return false;
  }
  const auto& node_props = rdg_.node_properties();
  const auto& edge_props = rdg_.edge_properties();
  const auto& other_node_props = other->node_properties();
  const auto& other_edge_props = other->edge_properties();
  if (node_props->num_columns() != other_node_props->num_columns()) {
    return false;
  }
  if (edge_props->num_columns() != other_edge_props->num_columns()) {
    return false;
  }
  for (const auto& prop_name : node_props->ColumnNames()) {
    if (!node_props->GetColumnByName(prop_name)->Equals(
            other_node_props->GetColumnByName(prop_name))) {
      return false;
    }
  }
  for (const auto& prop_name : edge_props->ColumnNames()) {
    if (!edge_props->GetColumnByName(prop_name)->Equals(
            other_edge_props->GetColumnByName(prop_name))) {
      return false;
    }
  }
  return true;
}

std::string
katana::PropertyGraph::ReportDiff(const PropertyGraph* other) const {
  fmt::memory_buffer buf;
  if (!topology().Equals(other->topology())) {
    fmt::format_to(
        std::back_inserter(buf),
        "Topologies differ nodes/edges {}/{} vs. {}/{}\n",
        topology().num_nodes(), topology().num_edges(),
        other->topology().num_nodes(), other->topology().num_edges());
  } else {
    fmt::format_to(std::back_inserter(buf), "Topologies match!\n");
  }
  const auto& node_props = rdg_.node_properties();
  const auto& edge_props = rdg_.edge_properties();
  const auto& other_node_props = other->node_properties();
  const auto& other_edge_props = other->edge_properties();
  if (node_props->num_columns() != other_node_props->num_columns()) {
    fmt::format_to(
        std::back_inserter(buf), "Number of node properties differ {} vs. {}\n",
        node_props->num_columns(), other_node_props->num_columns());
  }
  if (edge_props->num_columns() != other_edge_props->num_columns()) {
    fmt::format_to(
        std::back_inserter(buf), "Number of edge properties differ {} vs. {}\n",
        edge_props->num_columns(), other_edge_props->num_columns());
  }
  for (const auto& prop_name : node_props->ColumnNames()) {
    auto other_col = other_node_props->GetColumnByName(prop_name);
    auto my_col = node_props->GetColumnByName(prop_name);
    if (other_col == nullptr) {
      fmt::format_to(
          std::back_inserter(buf), "Only first has node property {}\n",
          prop_name);
    } else if (!my_col->Equals(other_col)) {
      fmt::format_to(
          std::back_inserter(buf), "Node property {:15} {:12} differs\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
      if (my_col->length() != other_col->length()) {
        fmt::format_to(
            std::back_inserter(buf), " size {}/{}\n", my_col->length(),
            other_col->length());
      } else {
        DiffFormatTo(buf, my_col, other_col);
      }
    } else {
      fmt::format_to(
          std::back_inserter(buf), "Node property {:15} {:12} matches!\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
    }
  }
  for (const auto& prop_name : edge_props->ColumnNames()) {
    auto other_col = other_edge_props->GetColumnByName(prop_name);
    auto my_col = edge_props->GetColumnByName(prop_name);
    if (other_col == nullptr) {
      fmt::format_to(
          std::back_inserter(buf), "Only first has edge property {}\n",
          prop_name);
    } else if (!edge_props->GetColumnByName(prop_name)->Equals(
                   other_edge_props->GetColumnByName(prop_name))) {
      fmt::format_to(
          std::back_inserter(buf), "Edge property {:15} {:12} differs\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
      if (my_col->length() != other_col->length()) {
        fmt::format_to(
            std::back_inserter(buf), " size {}/{}\n", my_col->length(),
            other_col->length());
      } else {
        DiffFormatTo(buf, my_col, other_col);
      }
    } else {
      fmt::format_to(
          std::back_inserter(buf), "Edge property {:15} {:12} matches!\n",
          prop_name, fmt::format("({})", my_col->type()->name()));
    }
  }
  return std::string(buf.begin(), buf.end());
}

katana::Result<void>
katana::PropertyGraph::Write(
    const std::string& rdg_name, const std::string& command_line) {
  if (auto res = tsuba::Create(rdg_name); !res) {
    return res.error();
  }
  return WriteGraph(rdg_name, command_line);
}

katana::Result<void>
katana::PropertyGraph::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& props) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty node prop table");
    return ResultSuccess();
  }
  if (topology().num_nodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_nodes(), props->num_rows());
  }
  return rdg_.AddNodeProperties(props);
}

katana::Result<void>
katana::PropertyGraph::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty node prop table");
    return ResultSuccess();
  }
  if (topology().num_nodes() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_nodes(), props->num_rows());
  }
  return rdg_.UpsertNodeProperties(props);
}

katana::Result<void>
katana::PropertyGraph::RemoveNodeProperty(int i) {
  return rdg_.RemoveNodeProperty(i);
}

katana::Result<void>
katana::PropertyGraph::RemoveNodeProperty(const std::string& prop_name) {
  auto col_names = node_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_.RemoveNodeProperty(std::distance(col_names.cbegin(), pos));
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::UnloadNodeProperty(int i) {
  return rdg_.UnloadNodeProperty(i);
}

katana::Result<void>
katana::PropertyGraph::LoadNodeProperty(const std::string& name, int i) {
  return rdg_.LoadNodeProperty(name, i);
}
/// Load a node property by name if it is absent and append its column to
/// the table do nothing otherwise
katana::Result<void>
katana::PropertyGraph::EnsureNodePropertyLoaded(const std::string& name) {
  if (HasNodeProperty(name)) {
    return katana::ResultSuccess();
  }
  return LoadNodeProperty(name);
}

std::vector<std::string>
katana::PropertyGraph::ListNodeProperties() const {
  return rdg_.ListNodeProperties();
}

std::vector<std::string>
katana::PropertyGraph::ListEdgeProperties() const {
  return rdg_.ListEdgeProperties();
}

katana::Result<void>
katana::PropertyGraph::UnloadNodeProperty(const std::string& prop_name) {
  auto col_names = node_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_.UnloadNodeProperty(std::distance(col_names.cbegin(), pos));
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& props) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("adding empty edge prop table");
    return ResultSuccess();
  }
  if (topology().num_edges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_edges(), props->num_rows());
  }
  return rdg_.AddEdgeProperties(props);
}

katana::Result<void>
katana::PropertyGraph::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props) {
  if (props->num_columns() == 0) {
    KATANA_LOG_DEBUG("upsert empty edge prop table");
    return ResultSuccess();
  }
  if (topology().num_edges() != static_cast<uint64_t>(props->num_rows())) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        topology().num_edges(), props->num_rows());
  }
  return rdg_.UpsertEdgeProperties(props);
}

katana::Result<void>
katana::PropertyGraph::RemoveEdgeProperty(int i) {
  return rdg_.RemoveEdgeProperty(i);
}

katana::Result<void>
katana::PropertyGraph::RemoveEdgeProperty(const std::string& prop_name) {
  auto col_names = edge_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_.RemoveEdgeProperty(std::distance(col_names.cbegin(), pos));
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::UnloadEdgeProperty(int i) {
  return rdg_.UnloadEdgeProperty(i);
}

katana::Result<void>
katana::PropertyGraph::LoadEdgeProperty(const std::string& name, int i) {
  return rdg_.LoadEdgeProperty(name, i);
}

/// Load an edge property by name if it is absent and append its column to
/// the table do nothing otherwise
katana::Result<void>
katana::PropertyGraph::EnsureEdgePropertyLoaded(const std::string& name) {
  if (HasEdgeProperty(name)) {
    return katana::ResultSuccess();
  }
  return LoadEdgeProperty(name);
}

katana::Result<void>
katana::PropertyGraph::UnloadEdgeProperty(const std::string& prop_name) {
  auto col_names = edge_properties()->ColumnNames();
  auto pos = std::find(col_names.cbegin(), col_names.cend(), prop_name);
  if (pos != col_names.cend()) {
    return rdg_.UnloadEdgeProperty(std::distance(col_names.cbegin(), pos));
  }
  return katana::ErrorCode::PropertyNotFound;
}

katana::Result<void>
katana::PropertyGraph::InformPath(const std::string& input_path) {
  if (!rdg_.rdg_dir().empty()) {
    KATANA_LOG_DEBUG("rdg dir from {} to {}", rdg_.rdg_dir(), input_path);
  }
  auto uri_res = katana::Uri::Make(input_path);
  if (!uri_res) {
    return uri_res.error();
  }

  rdg_.set_rdg_dir(uri_res.value());
  return ResultSuccess();
}

katana::Result<std::unique_ptr<katana::NUMAArray<uint64_t>>>
katana::SortAllEdgesByDest(katana::PropertyGraph* pg) {
  // TODO(amber): This function will soon change so that it produces a new sorted
  // topology instead of modifying an existing one. The const_cast will go away
  const auto& topo = pg->topology();

  auto permutation_vec = std::make_unique<katana::NUMAArray<uint64_t>>();
  permutation_vec->allocateInterleaved(topo.num_edges());
  katana::ParallelSTL::iota(
      permutation_vec->begin(), permutation_vec->end(), uint64_t{0});

  auto* out_dests_data = const_cast<GraphTopology::Node*>(topo.dest_data());

  katana::do_all(
      katana::iterate(uint64_t{0}, pg->topology().num_nodes()),
      [&](uint64_t n) {
        const auto e_beg = *pg->topology().edge_begin(n);
        const auto e_end = *pg->topology().edge_end(n);
        // IMPORTANT: Must sort permutation vector before dest array
        std::sort(
            permutation_vec->begin() + e_beg, permutation_vec->begin() + e_end,
            [&](uint64_t a, uint64_t b) {
              return out_dests_data[a] < out_dests_data[b];
            });
        std::sort(
            out_dests_data + e_beg, out_dests_data + e_end,
            std::less<GraphTopology::Node>{});
      },
      katana::steal());

  return std::unique_ptr<katana::NUMAArray<uint64_t>>(
      std::move(permutation_vec));
}

// TODO(amber): make this a method of a sorted topology class in the near future
// TODO(amber): this method should return an edge_iterator
katana::GraphTopology::Edge
katana::FindEdgeSortedByDest(
    const PropertyGraph* graph, const GraphTopology::Node src,
    const GraphTopology::Node dst) {
  using edge_iterator = GraphTopology::edge_iterator;

  const auto& topo = graph->topology();
  const edge_iterator e_beg = topo.edge_begin(src);
  const edge_iterator e_end = topo.edge_end(src);

  constexpr int BINARY_SEARCH_THRESHOLD = 100;

  if (std::distance(e_beg, e_end) < BINARY_SEARCH_THRESHOLD) {
    auto iter = std::find_if(e_beg, e_end, [&](const GraphTopology::Edge& e) {
      return topo.edge_dest(e) == dst;
    });

    return *iter;

  } else {
    auto comparator = [&](const GraphTopology::Edge& e,
                          const GraphTopology::Node& d) {
      return topo.edge_dest(e) < d;
    };

    auto iter = std::lower_bound(e_beg, e_end, dst, comparator);

    return topo.edge_dest(iter) == dst ? *iter : *e_end;
  }
}

// TODO(amber): this method should return a new sorted topology
katana::Result<void>
katana::SortNodesByDegree(katana::PropertyGraph* pg) {
  const auto& topo = pg->topology();

  uint64_t num_nodes = topo.num_nodes();
  uint64_t num_edges = topo.num_edges();

  using DegreeNodePair = std::pair<uint64_t, uint32_t>;
  katana::NUMAArray<DegreeNodePair> dn_pairs;
  dn_pairs.allocateInterleaved(num_nodes);

  katana::do_all(katana::iterate(uint64_t{0}, num_nodes), [&](size_t node) {
    size_t node_degree = pg->edges(node).size();
    dn_pairs[node] = DegreeNodePair(node_degree, node);
  });

  // sort by degree (first item)
  katana::ParallelSTL::sort(
      dn_pairs.begin(), dn_pairs.end(), std::greater<DegreeNodePair>());

  // create mapping, get degrees out to another vector to get prefix sum
  katana::NUMAArray<uint32_t> old_to_new_mapping;
  old_to_new_mapping.allocateInterleaved(num_nodes);

  katana::NUMAArray<uint64_t> new_prefix_sum;
  new_prefix_sum.allocateInterleaved(num_nodes);

  katana::do_all(katana::iterate(uint64_t{0}, num_nodes), [&](uint64_t index) {
    // save degree, which is pair.first
    new_prefix_sum[index] = dn_pairs[index].first;
    // save mapping; original index is in .second, map it to current index
    old_to_new_mapping[dn_pairs[index].second] = index;
  });

  katana::ParallelSTL::partial_sum(
      new_prefix_sum.begin(), new_prefix_sum.end(), new_prefix_sum.begin());

  katana::NUMAArray<uint32_t> new_out_dest;
  new_out_dest.allocateInterleaved(num_edges);

  auto* out_dests_data = const_cast<GraphTopology::Node*>(topo.dest_data());
  auto* out_indices_data = const_cast<GraphTopology::Edge*>(topo.adj_data());

  katana::do_all(
      katana::iterate(uint64_t{0}, num_nodes),
      [&](uint32_t old_node_id) {
        uint32_t new_node_id = old_to_new_mapping[old_node_id];

        // get the start location of this reindex'd nodes edges
        uint64_t new_out_index =
            (new_node_id == 0) ? 0 : new_prefix_sum[new_node_id - 1];

        // construct the graph, reindexing as it goes along
        for (auto e : topo.edges(old_node_id)) {
          // get destination, reindex
          uint32_t old_edge_dest = out_dests_data[e];
          uint32_t new_edge_dest = old_to_new_mapping[old_edge_dest];

          new_out_dest[new_out_index] = new_edge_dest;

          new_out_index++;
        }
        // this assert makes sure reindex was correct + makes sure all edges
        // are accounted for
        KATANA_LOG_DEBUG_ASSERT(new_out_index == new_prefix_sum[new_node_id]);
      },
      katana::steal());

  //Update the underlying PropertyGraph topology
  // TODO(amber): eliminate these copies since we will be returning a new topology
  katana::do_all(
      katana::iterate(uint64_t{0}, num_nodes), [&](uint32_t node_id) {
        out_indices_data[node_id] = new_prefix_sum[node_id];
      });

  katana::do_all(
      katana::iterate(uint64_t{0}, num_edges), [&](uint32_t edge_id) {
        out_dests_data[edge_id] = new_out_dest[edge_id];
      });

  return katana::ResultSuccess();
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::CreateSymmetricGraph(katana::PropertyGraph* pg) {
  const GraphTopology& topology = pg->topology();
  if (topology.num_nodes() == 0) {
    return std::make_unique<PropertyGraph>();
  }

  // New symmetric graph topology
  katana::NUMAArray<uint64_t> out_indices;
  katana::NUMAArray<uint32_t> out_dests;

  out_indices.allocateInterleaved(topology.num_nodes());
  // Store the out-degree of nodes from original graph
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_nodes()), [&](uint64_t n) {
        auto edges = topology.edges(n);
        out_indices[n] = (*edges.end() - *edges.begin());
      });

  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_nodes()),
      [&](uint64_t n) {
        auto edges = topology.edges(n);
        // update the out_indices for the symmetric topology
        for (auto e = edges.begin(); e != edges.end(); ++e) {
          auto dest = topology.edge_dest(*e);
          // Do not add reverse edge for self-loops
          if (n != dest) {
            __sync_fetch_and_add(&(out_indices[dest]), 1);
          }
        }
      },
      katana::steal());

  // Compute prefix sum
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  uint64_t num_nodes_symmetric = topology.num_nodes();
  uint64_t num_edges_symmetric = out_indices[num_nodes_symmetric - 1];

  katana::NUMAArray<uint64_t> out_dests_offset;
  out_dests_offset.allocateInterleaved(topology.num_nodes());
  // Temp NUMAArray for computing new destination positions
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(uint64_t{1}, topology.num_nodes()),
      [&](uint64_t n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  out_dests.allocateInterleaved(num_edges_symmetric);
  // Update graph topology with the original edges + reverse edges
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_nodes()),
      [&](uint64_t src) {
        // get all outgoing edges (excluding self edges) of a particular
        // node and add reverse edges.
        for (GraphTopology::Edge e : topology.edges(src)) {
          // e = start index into edge array for a particular node
          // destination node
          auto dest = topology.edge_dest(e);

          // Add original edge
          auto e_new_src = __sync_fetch_and_add(&(out_dests_offset[src]), 1);
          out_dests[e_new_src] = dest;

          // Do not add reverse edge for self-loops
          if (dest != src) {
            // Add reverse edge
            auto e_new_dst = __sync_fetch_and_add(&(out_dests_offset[dest]), 1);
            out_dests[e_new_dst] = src;
            // TODO(gill) copy edge data to "new" array
          }
        }
      },
      katana::no_stats());

  GraphTopology sym_topo(std::move(out_indices), std::move(out_dests));
  auto symmetric = std::make_unique<katana::PropertyGraph>(std::move(sym_topo));

  return std::unique_ptr<PropertyGraph>(std::move(symmetric));
}

katana::Result<std::unique_ptr<katana::PropertyGraph>>
katana::CreateTransposeGraphTopology(const GraphTopology& topology) {
  if (topology.num_nodes() == 0) {
    return std::make_unique<PropertyGraph>();
  }

  katana::NUMAArray<GraphTopology::Edge> out_indices;
  katana::NUMAArray<GraphTopology::Node> out_dests;

  out_indices.allocateInterleaved(topology.num_nodes());
  out_dests.allocateInterleaved(topology.num_edges());

  // Initialize the new topology indices
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_nodes()),
      [&](uint64_t n) { out_indices[n] = uint64_t{0}; }, katana::no_stats());

  // Keep a copy of old destinaton ids and compute number of
  // in-coming edges for the new prefix sum of out_indices.
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_edges()),
      [&](uint64_t e) {
        // Counting outgoing edges in the tranpose graph by
        // counting incoming edges in the original graph
        auto dest = topology.edge_dest(e);
        __sync_add_and_fetch(&(out_indices[dest]), 1);
      },
      katana::no_stats());

  // Prefix sum calculation of the edge index array
  katana::ParallelSTL::partial_sum(
      out_indices.begin(), out_indices.end(), out_indices.begin());

  katana::NUMAArray<uint64_t> out_dests_offset;
  out_dests_offset.allocateInterleaved(topology.num_nodes());
  // Reuse out_indices_tmp for computing new destination positions
  out_dests_offset[0] = 0;
  katana::do_all(
      katana::iterate(uint64_t{1}, topology.num_nodes()),
      [&](uint64_t n) { out_dests_offset[n] = out_indices[n - 1]; },
      katana::no_stats());

  // Update large_array_out_dests_ with the new destination ids
  // of the transposed graphs
  katana::do_all(
      katana::iterate(uint64_t{0}, topology.num_nodes()),
      [&](uint64_t src) {
        // get all outgoing edges of a particular
        // node and reverse the edges.
        for (GraphTopology::Edge e : topology.edges(src)) {
          // e = start index into edge array for a particular node
          // Destination node
          auto dest = topology.edge_dest(e);
          // Location to save edge
          auto e_new = __sync_fetch_and_add(&(out_dests_offset[dest]), 1);
          // Save src as destination
          out_dests[e_new] = src;
          // TODO (gill) copy edge data to the transposed graph
        }
      },
      katana::no_stats());

  GraphTopology transpose_topo{std::move(out_indices), std::move(out_dests)};
  auto transpose_pg =
      std::make_unique<katana::PropertyGraph>(std::move(transpose_topo));

  return std::unique_ptr<PropertyGraph>(std::move(transpose_pg));
}
