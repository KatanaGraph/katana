#include "tsuba/RDGTopology.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/outcome/detail/value_storage.hpp>
#include <unicode/utypes.h>

#include "PartitionTopologyMetadata.h"
#include "RDGPartHeader.h"
#include "katana/EntityTypeManager.h"
#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"
#include "tsuba/Errors.h"
#include "tsuba/FaultTest.h"
#include "tsuba/FileFrame.h"
#include "tsuba/FileView.h"
#include "tsuba/RDG.h"
#include "tsuba/tsuba.h"

namespace tsuba {

std::string
RDGTopology::path() const {
  if (metadata_entry_valid()) {
    return metadata_entry_->path_;
  }

  KATANA_LOG_WARN(
      "trying to get topology path, but not linked to a metadata entry. "
      "Returning empty string");
  return std::string();
}

void
RDGTopology::set_path(const std::string& path) {
  KATANA_LOG_VASSERT(
      metadata_entry_valid(),
      "metadata_entry_->must be set before we can set the topology path");
  metadata_entry_->path_ = path;
}

void
RDGTopology::set_invalid() {
  invalid_ = true;
  if (metadata_entry_valid()) {
    metadata_entry_->set_invalid();
  }
}

void
RDGTopology::set_metadata_entry(PartitionTopologyMetadataEntry* entry) {
  KATANA_LOG_ASSERT(entry != nullptr);
  metadata_entry_ = entry;
  KATANA_LOG_ASSERT(metadata_entry_valid());
}

bool
RDGTopology::metadata_entry_valid() const {
  return (metadata_entry_ != nullptr);
}

katana::Result<void>
RDGTopology::Bind(const katana::Uri& metadata_dir, bool resolve) {
  if (file_store_bound_) {
    KATANA_LOG_WARN("topology already bound, nothing to do");
    return katana::ResultSuccess();
  }
  if (path().empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "Cannot bind topology with empty path");
  }

  katana::Uri t_path = metadata_dir.Join(path());
  KATANA_LOG_DEBUG(
      "binding to entire topology file at path {}", t_path.string());
  KATANA_CHECKED(file_storage_.Bind(t_path.string(), resolve));

  file_store_bound_ = true;
  storage_valid_ = true;

  return katana::ResultSuccess();
}

katana::Result<void>
RDGTopology::Bind(
    const katana::Uri& metadata_dir, uint64_t begin, uint64_t end,
    bool resolve) {
  if (path().empty()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument, "Cannot bind topology with empty path");
  }
  katana::Uri t_path = metadata_dir.Join(path());
  KATANA_LOG_DEBUG(
      "binding from {} to {} with topology file at path {}", begin, end,
      t_path.string());
  KATANA_CHECKED(file_storage_.Bind(t_path.string(), begin, end, resolve));

  file_store_bound_ = true;
  storage_valid_ = true;

  return katana::ResultSuccess();
}

katana::Result<void>
RDGTopology::Map() {
  if (file_store_mapped_) {
    return katana::ResultSuccess();
  }

  if (!file_store_bound_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "topology must be bound before it is mapped");
  }

  const auto* data = file_storage_.ptr<uint64_t>();

  size_t min_size = 4;
  if (file_storage_.size() < min_size) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "file_storage size {} is less than the minimum size {}",
        file_storage_.size(), min_size);
  }

  if (data[0] != 1) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "first entry in the topology data array must be 1, is {}", data[0]);
  }

  // ensure the data file matches the metadata
  KATANA_LOG_VASSERT(
      num_nodes_ == data[2], "expected {} nodes, found {} nodes", num_nodes_,
      data[2]);
  KATANA_LOG_VASSERT(
      num_edges_ == data[3], "expected {} edges, found {} edges", num_edges_,
      data[3]);

  adj_indices_ = &data[4];

  //TODO(emcginnis): this cursor stuff is gross and easy to mess up.
  // Could introduce a byte iterator, with all usual iterator input stuff.
  // Iterator is initialized with what byte alignment we would like to keep
  // in this case it is sizeof(uint64_t) aligned
  // as well as something like
  // AdvanceBy<T> which advances the cursor in the iterator by sizeof(T)
  // all of the padding math then gets wrapped up in the iterator
  const uint64_t* cursor = adj_indices_;
  const uint64_t magic = (num_nodes_ + num_edges_);

  uint64_t adj_indices_size = num_nodes_;
  // EdgeTypeAwareTopologies have a larger adj_indices array than usual topologies
  if (topology_state_ ==
      tsuba::RDGTopology::TopologyKind::kEdgeTypeAwareTopology) {
    adj_indices_size =
        std::max(num_nodes_, num_nodes_ * edge_condensed_type_id_map_size_);
  }

  cursor += adj_indices_size;

  dests_ = reinterpret_cast<const uint32_t*>(cursor);

  cursor += (num_edges_ / 2 + num_edges_ % 2);

  if (metadata_entry_->edge_index_to_property_index_map_present_) {
    KATANA_LOG_VASSERT(
        *cursor == magic, "expected magic number = {}, found {}", magic,
        *cursor);
    cursor += 1;
    edge_index_to_property_index_map_ = cursor;
    cursor += num_edges_;
  }

  if (metadata_entry_->node_index_to_property_index_map_present_) {
    KATANA_LOG_VASSERT(
        *cursor == magic, "expected magic number = {}, found {}", magic,
        *cursor);
    cursor += 1;
    node_index_to_property_index_map_ = cursor;
    cursor += num_nodes_;
  }

  if (metadata_entry_->edge_condensed_type_id_map_present_) {
    KATANA_LOG_VASSERT(
        *cursor == magic, "expected magic number = {}, found {}", magic,
        *cursor);

    cursor += 1;
    edge_condensed_type_id_map_ =
        reinterpret_cast<const katana::EntityTypeID*>(cursor);
    cursor +=
        ((num_edges_ / sizeof(uint64_t)) +
         FileFrame::calculate_padding_bytes(num_edges_, sizeof(uint64_t)));
  }

  if (metadata_entry_->node_condensed_type_id_map_present_) {
    KATANA_LOG_VASSERT(
        *cursor == magic, "expected magic number = {}, found {}", magic,
        *cursor);

    cursor += 1;
    node_condensed_type_id_map_ =
        reinterpret_cast<const katana::EntityTypeID*>(cursor);
    cursor +=
        ((num_nodes_ / sizeof(uint64_t)) +
         FileFrame::calculate_padding_bytes(num_nodes_, sizeof(uint64_t)));
  }

  size_t expected_size = GetGraphSize();
  if (file_storage_.size() < expected_size) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument,
        "file_view size: {} expected size: {}., num_nodes = {}, num_edges = "
        "{}, "
        "edge_index_to_property_index_map_present_ = {}, "
        "node_index_to_property_index_map_present_ = {}, "
        "edge_condensed_type_id_map_present = {}, "
        "node_condensed_type_id_map_present = {}",
        file_storage_.size(), expected_size, num_nodes_, num_edges_,
        metadata_entry_->edge_index_to_property_index_map_present_,
        metadata_entry_->node_index_to_property_index_map_present_,
        metadata_entry_->edge_condensed_type_id_map_present_,
        metadata_entry_->node_condensed_type_id_map_present_);
  }

  file_store_mapped_ = true;

  return katana::ResultSuccess();
}

katana::Result<void>
RDGTopology::MapMetadataExtract(
    uint64_t num_nodes, uint64_t num_edges, bool storage_valid) {
  if (file_store_mapped_) {
    KATANA_LOG_WARN(
        "Tried to map metadata of the topology file, but topology file is "
        "already mapped");
    return katana::ResultSuccess();
  }

  if (!file_store_bound_) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "topology must be bound before it is mapped");
  }

  const auto* data = file_storage_.ptr<uint64_t>();

  size_t min_size = 4;
  if (file_storage_.size() < min_size) {
    return katana::ErrorCode::InvalidArgument;
  }

  if (data[0] != 1) {
    return katana::ErrorCode::InvalidArgument;
  }

  num_nodes_ = data[2];
  num_edges_ = data[3];

  //TODO(emcginnis): remove the || num_nodes/edges == 0 when the input rdgs are updated
  KATANA_LOG_VASSERT(
      num_nodes_ == num_nodes || num_nodes == 0,
      "Extracted num_nodes = {} does not match the known num_nodes = {}",
      num_nodes_, num_nodes);
  KATANA_LOG_VASSERT(
      num_edges_ == num_edges || num_edges == 0,
      "Extracted num_edges = {} does not match the known num_edges = {}",
      num_edges_, num_edges);

  topology_state_ = TopologyKind::kCSR;
  transpose_state_ = TransposeKind::kNo;
  edge_sort_state_ = EdgeSortKind::kAny;
  node_sort_state_ = NodeSortKind::kAny;

  // update our metadata entry with what we loaded, must do this since the metadata was incomplete before
  metadata_entry_->Update(
      num_edges_, num_nodes_,
      /*edge_index_to_property_index_map_present=*/false,
      /*node_index_to_property_index_map_present=*/false,
      /*edge_condensed_type_id_map_size_=*/0,
      /*edge_condensed_type_id_map_present=*/false,
      /*node_condensed_type_id_map_size_=*/0,
      /*node_condensed_type_id_map_present=*/false, topology_state_,
      transpose_state_, edge_sort_state_, node_sort_state_);

  KATANA_LOG_DEBUG(
      "Extracted Metadata from topology file: num_edges = {}, num_nodes = {}",
      metadata_entry_->num_edges_, metadata_entry_->num_nodes_);

  // since we extracted the metadata, we must write out this topology on Store
  // unless we have RemoteCopied the topology file into place already
  storage_valid_ = storage_valid;

  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::RDGTopology::DoStore(
    RDGHandle handle, const katana::Uri& current_rdg_dir,
    std::unique_ptr<tsuba::WriteGroup>& write_group) {
  KATANA_LOG_VASSERT(!invalid_, "tried to store an invalid RDGTopology");

  if (!storage_valid_) {
    // This RDGTopology is either new, or is an update to a now-invalid RDGTopology

    KATANA_LOG_DEBUG(
        "Storing RDGTopology to file. TopologyKind={}, TransposeKind={}, "
        "EdgeSortKind={}, NodeSortKind={}",
        topology_state_, transpose_state_, edge_sort_state_, node_sort_state_);

    auto ff = std::make_unique<tsuba::FileFrame>();
    KATANA_CHECKED(ff->Init());

    uint64_t data[4] = {1, 0, num_nodes_, num_edges_};
    arrow::Status aro_sts = ff->Write(&data, 4 * sizeof(uint64_t));
    if (!aro_sts.ok()) {
      return tsuba::ArrowToTsuba(aro_sts.code());
    }

    if (num_nodes_) {
      if (edge_condensed_type_id_map_size_ > 0) {
        KATANA_LOG_VASSERT(
            adj_indices_ != nullptr,
            "Cannot store an RDGTopology with edges and null adj_indices");
      }
      const auto* raw = adj_indices_;
      static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint64_t>);
      uint64_t adj_indices_size = num_nodes_;

      // EdgeTypeAwareTopologies have a larger adj_indices array than usual topologies
      if (topology_state_ ==
          tsuba::RDGTopology::TopologyKind::kEdgeTypeAwareTopology) {
        adj_indices_size = num_nodes_ * edge_condensed_type_id_map_size_;
      }

      KATANA_LOG_DEBUG(
          "Storing RDGTopology to file. Writing adj_indices, size = {}",
          adj_indices_size);

      if (adj_indices_size > 0) {
        auto buf = arrow::Buffer::Wrap(raw, adj_indices_size);
        aro_sts = ff->Write(buf);
        if (!aro_sts.ok()) {
          return tsuba::ArrowToTsuba(aro_sts.code());
        }
      }
    }

    if (num_edges_) {
      KATANA_LOG_VASSERT(
          dests_ != nullptr, "Cannot store an RDGTopology with null dests_");
      const auto* raw = dests_;
      static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint32_t>);

      KATANA_LOG_DEBUG(
          "Storing RDGTopology to file. Writing dests, size = {}", num_edges_);

      auto buf = arrow::Buffer::Wrap(raw, num_edges_);
      KATANA_CHECKED_CONTEXT(
          ff->PaddedWrite(buf, sizeof(uint64_t)),
          "Failed to write dests to file frame");
    }

    if (edge_index_to_property_index_map_ != nullptr && num_edges_) {
      KATANA_LOG_DEBUG(
          "Storing RDGTopology to file. Writing "
          "edge_index_to_property_index_map, size = {}",
          num_edges_);

      // first write the magic number
      uint64_t data[1] = {num_nodes_ + num_edges_};
      arrow::Status aro_sts = ff->Write(&data, 1 * sizeof(uint64_t));
      if (!aro_sts.ok()) {
        return tsuba::ArrowToTsuba(aro_sts.code());
      }

      // edge property index map is uint64_t map[num_edges]
      const auto* raw = edge_index_to_property_index_map_;
      static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint64_t>);
      auto buf = arrow::Buffer::Wrap(raw, num_edges_);
      aro_sts = ff->Write(buf);
      if (!aro_sts.ok()) {
        return tsuba::ArrowToTsuba(aro_sts.code());
      }
    }

    if (node_index_to_property_index_map_ != nullptr && num_nodes_) {
      KATANA_LOG_DEBUG(
          "Storing RDGTopology to file. Writing "
          "node_index_to_property_index_map, size = {}",
          num_nodes_);

      // first write the magic number
      uint64_t data[1] = {num_nodes_ + num_edges_};
      arrow::Status aro_sts = ff->Write(&data, 1 * sizeof(uint64_t));
      if (!aro_sts.ok()) {
        return tsuba::ArrowToTsuba(aro_sts.code());
      }

      // node property index map is uint64_t map[num_nodes]
      const auto* raw = node_index_to_property_index_map_;
      static_assert(std::is_same_v<std::decay_t<decltype(*raw)>, uint64_t>);
      auto buf = arrow::Buffer::Wrap(raw, num_nodes_);
      aro_sts = ff->Write(buf);
      if (!aro_sts.ok()) {
        return tsuba::ArrowToTsuba(aro_sts.code());
      }
    }

    if (edge_condensed_type_id_map_ != nullptr && num_edges_) {
      KATANA_LOG_DEBUG(
          "Storing RDGTopology to file. Writing "
          "edge_condensed_type_id_map, size = {}",
          edge_condensed_type_id_map_size_);

      // first write the magic number
      uint64_t data[1] = {num_nodes_ + num_edges_};
      arrow::Status aro_sts = ff->Write(&data, 1 * sizeof(uint64_t));
      if (!aro_sts.ok()) {
        return tsuba::ArrowToTsuba(aro_sts.code());
      }

      // node property index map is uint64_t map[num_nodes]
      const auto* raw = edge_condensed_type_id_map_;
      static_assert(
          std::is_same_v<std::decay_t<decltype(*raw)>, katana::EntityTypeID>);
      auto buf = arrow::Buffer::Wrap(raw, edge_condensed_type_id_map_size_);
      // pad to nearest uint64_t aka 8 byte boundry
      KATANA_CHECKED_CONTEXT(
          ff->PaddedWrite(buf, sizeof(uint64_t)),
          "Failed to write edge_condensed_type_id_map to file frame");
    }

    if (node_condensed_type_id_map_ != nullptr && num_nodes_) {
      KATANA_LOG_DEBUG(
          "Storing RDGTopology to file. Writing "
          "node_condensed_type_id_map, size = {}",
          node_condensed_type_id_map_size_);

      // first write the magic number
      uint64_t data[1] = {num_nodes_ + num_edges_};
      arrow::Status aro_sts = ff->Write(&data, 1 * sizeof(uint64_t));
      if (!aro_sts.ok()) {
        return tsuba::ArrowToTsuba(aro_sts.code());
      }

      // node property index map is uint64_t map[num_nodes]
      const auto* raw = node_condensed_type_id_map_;
      static_assert(
          std::is_same_v<std::decay_t<decltype(*raw)>, katana::EntityTypeID>);
      auto buf = arrow::Buffer::Wrap(raw, node_condensed_type_id_map_size_);
      // pad to nearest uint64_t aka 8 byte boundry
      KATANA_CHECKED_CONTEXT(
          ff->PaddedWrite(buf, sizeof(uint64_t)),
          "Failed to write node_condensed_type_id_map to file frame");
    }

    //TODO: emcginnis need different naming schemes for the optional topologies?
    // add "epi_npi_eti_nti" to name?
    katana::Uri path_uri = MakeTopologyFileName(handle);
    ff->Bind(path_uri.string());
    TSUBA_PTP(internal::FaultSensitivity::Normal);
    write_group->StartStore(std::move(ff));
    TSUBA_PTP(internal::FaultSensitivity::Normal);

    // update the metadata entry

    KATANA_LOG_ASSERT(topology_state_ != TopologyKind::kInvalid);
    KATANA_LOG_ASSERT(transpose_state_ != TransposeKind::kInvalid);
    KATANA_LOG_ASSERT(edge_sort_state_ != EdgeSortKind::kInvalid);
    KATANA_LOG_ASSERT(node_sort_state_ != NodeSortKind::kInvalid);

    metadata_entry_->Update(
        path_uri.BaseName(), num_edges_, num_nodes_,
        (edge_index_to_property_index_map_ != nullptr),
        (node_index_to_property_index_map_ != nullptr),
        edge_condensed_type_id_map_size_,
        (edge_condensed_type_id_map_ != nullptr),
        node_condensed_type_id_map_size_,
        (node_condensed_type_id_map_ != nullptr), topology_state_,
        transpose_state_, edge_sort_state_, node_sort_state_);
  }

  else if (path().empty()) {
    // we don't have an update, but we are persisting in a new location
    // store our in memory state

    KATANA_LOG_DEBUG(
        "Storing RDGTopology to file in new location. TopologyKind={}, "
        "TransposeKind={}, "
        "EdgeSortKind={}, NodeSortKind={}",
        topology_state_, transpose_state_, edge_sort_state_, node_sort_state_);

    //TODO: emcginnis need different naming schemes for the optional topologies
    katana::Uri path_uri = MakeTopologyFileName(handle);

    // file store must be bound to make a copy of it in a new location
    if (!file_store_bound_) {
      // Can't just use RDGTopology::Bind() here since path() is empty
      // Must bind to old_path since we are persisting in a new location
      std::string old_path = metadata_entry_->old_path_;
      if (old_path.empty()) {
        return KATANA_ERROR(
            ErrorCode::InvalidArgument, "Cannot bind topology with empty path");
      }
      katana::Uri t_path = current_rdg_dir.Join(old_path);

      KATANA_LOG_DEBUG(
          "binding to entire topology file at path {} for relocation",
          t_path.string());
      KATANA_CHECKED_CONTEXT(
          file_storage_.Bind(t_path.string(), true),
          "failed binding topology file for copying at path = {}, "
          "TopologyKind={}, "
          "TransposeKind={}, EdgeSortKind={}, NodeSortKind={}",
          t_path.string(), topology_state_, transpose_state_, edge_sort_state_,
          node_sort_state_);

      file_store_bound_ = true;
    }

    TSUBA_PTP(internal::FaultSensitivity::Normal);
    // depends on `topology file_storage_` outliving writes
    // all topology file stores must remain bound until write_group->Finish() completes
    write_group->StartStore(
        path_uri.string(), file_storage_.ptr<uint8_t>(), file_storage_.size());
    TSUBA_PTP(internal::FaultSensitivity::Normal);

    // since nothing has changed besides the storage location, just have to update path
    metadata_entry_->path_ = path_uri.BaseName();
  }
  // else: no update, not persisting in a new location, so nothing for us to do

  return katana::ResultSuccess();
}

bool
tsuba::RDGTopology::Equals(const RDGTopology& other) const {
  return (
      file_storage_.size() == other.file_storage_.size() &&
      !memcmp(
          file_storage_.ptr<uint8_t>(), other.file_storage_.ptr<uint8_t>(),
          file_storage_.size()) &&
      topology_state_ == other.topology_state_ &&
      transpose_state_ == other.transpose_state_ &&
      edge_sort_state_ == other.edge_sort_state_ &&
      node_sort_state_ == other.node_sort_state_);
}

tsuba::RDGTopology
tsuba::RDGTopology::MakeShadow(
    TopologyKind topology_state, TransposeKind transpose_state,
    EdgeSortKind edge_sort_state, NodeSortKind node_sort_state) {
  RDGTopology topo = RDGTopology();

  topo.topology_state_ = topology_state;
  topo.transpose_state_ = transpose_state;
  topo.edge_sort_state_ = edge_sort_state;
  topo.node_sort_state_ = node_sort_state;
  return RDGTopology(std::move(topo));
}

tsuba::RDGTopology
tsuba::RDGTopology::MakeShadowCSR() {
  // Match on a CSR topology transposed or not transposed, and no sorting
  return MakeShadow(
      TopologyKind::kCSR, TransposeKind::kAny, EdgeSortKind::kAny,
      NodeSortKind::kAny);
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::DoMake(
    tsuba::RDGTopology topo, const uint64_t* adj_indices, uint64_t num_nodes,
    const uint32_t* dests, uint64_t num_edges, TopologyKind topology_state,
    TransposeKind transpose_state, EdgeSortKind edge_sort_state,
    NodeSortKind node_sort_state) {
  topo.num_edges_ = num_edges;
  topo.adj_indices_ = std::move(adj_indices);
  topo.num_nodes_ = num_nodes;
  topo.dests_ = std::move(dests);
  topo.topology_state_ = topology_state;
  topo.transpose_state_ = transpose_state;
  topo.edge_sort_state_ = edge_sort_state;
  topo.node_sort_state_ = node_sort_state;

  return RDGTopology(std::move(topo));
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::Make(
    const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
    uint64_t num_edges, TopologyKind topology_state,
    TransposeKind transpose_state, EdgeSortKind edge_sort_state,
    NodeSortKind node_sort_state) {
  RDGTopology topo = RDGTopology();

  // when we make from in memory objects, mark storage as invalid
  topo.storage_valid_ = false;
  return DoMake(
      std::move(topo), adj_indices, num_nodes, dests, num_edges, topology_state,
      transpose_state, edge_sort_state, node_sort_state);
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::Make(
    const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
    uint64_t num_edges, TopologyKind topology_state,
    TransposeKind transpose_state, EdgeSortKind edge_sort_state,
    const uint64_t* edge_index_to_property_index_map) {
  RDGTopology topo = RDGTopology();
  topo.edge_index_to_property_index_map_ =
      std::move(edge_index_to_property_index_map);

  // when we make from in memory objects, mark storage as invalid
  topo.storage_valid_ = false;
  return DoMake(
      std::move(topo), adj_indices, num_nodes, dests, num_edges, topology_state,
      transpose_state, edge_sort_state, NodeSortKind::kAny);
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::Make(
    const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
    uint64_t num_edges, TopologyKind topology_state,
    TransposeKind transpose_state, EdgeSortKind edge_sort_state,
    const uint64_t* edge_index_to_property_index_map,
    uint64_t edge_condensed_type_id_map_size,
    const katana::EntityTypeID* edge_condensed_type_id_map) {
  RDGTopology topo = RDGTopology();
  topo.edge_index_to_property_index_map_ =
      std::move(edge_index_to_property_index_map);
  topo.edge_condensed_type_id_map_size_ = edge_condensed_type_id_map_size;
  topo.edge_condensed_type_id_map_ = std::move(edge_condensed_type_id_map);

  // when we make from in memory objects, mark storage as invalid
  topo.storage_valid_ = false;

  return DoMake(
      std::move(topo), adj_indices, num_nodes, dests, num_edges, topology_state,
      transpose_state, edge_sort_state, NodeSortKind::kAny);
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::Make(
    const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
    uint64_t num_edges, TopologyKind topology_state,
    TransposeKind transpose_state, EdgeSortKind edge_sort_state,
    NodeSortKind node_sort_state,
    const uint64_t* edge_index_to_property_index_map,
    const uint64_t* node_index_to_property_index_map) {
  RDGTopology topo = RDGTopology();
  topo.edge_index_to_property_index_map_ =
      std::move(edge_index_to_property_index_map);
  topo.node_index_to_property_index_map_ =
      std::move(node_index_to_property_index_map);

  // when we make from in memory objects, mark storage as invalid
  topo.storage_valid_ = false;
  return DoMake(
      std::move(topo), adj_indices, num_nodes, dests, num_edges, topology_state,
      transpose_state, edge_sort_state, node_sort_state);
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::Make(
    const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
    uint64_t num_edges, TopologyKind topology_state,
    TransposeKind transpose_state, EdgeSortKind edge_sort_state,
    NodeSortKind node_sort_state,
    const uint64_t* edge_index_to_property_index_map,
    const uint64_t* node_index_to_property_index_map,
    uint64_t edge_condensed_type_id_map_size,
    const katana::EntityTypeID* edge_condensed_type_id_map,
    uint64_t node_condensed_type_id_map_size,
    const katana::EntityTypeID* node_condensed_type_id_map) {
  RDGTopology topo = RDGTopology();
  topo.edge_index_to_property_index_map_ =
      std::move(edge_index_to_property_index_map);
  topo.node_index_to_property_index_map_ =
      std::move(node_index_to_property_index_map);
  topo.edge_condensed_type_id_map_size_ = edge_condensed_type_id_map_size;
  topo.edge_condensed_type_id_map_ = std::move(edge_condensed_type_id_map);
  topo.node_condensed_type_id_map_size_ = node_condensed_type_id_map_size;
  topo.node_condensed_type_id_map_ = std::move(node_condensed_type_id_map);

  // when we make from in memory objects, mark storage as invalid
  topo.storage_valid_ = false;
  return DoMake(
      std::move(topo), adj_indices, num_nodes, dests, num_edges, topology_state,
      transpose_state, edge_sort_state, node_sort_state);
}

katana::Result<tsuba::RDGTopology>
tsuba::RDGTopology::Make(PartitionTopologyMetadataEntry* entry) {
  RDGTopology topo = RDGTopology(entry);
  topo.set_metadata_entry(entry);
  topo.num_edges_ = topo.metadata_entry_->num_edges_;
  topo.num_nodes_ = topo.metadata_entry_->num_nodes_;
  topo.topology_state_ = topo.metadata_entry_->topology_state_;
  topo.transpose_state_ = topo.metadata_entry_->transpose_state_;
  topo.edge_sort_state_ = topo.metadata_entry_->edge_sort_state_;
  topo.node_sort_state_ = topo.metadata_entry_->node_sort_state_;
  topo.edge_condensed_type_id_map_size_ =
      topo.metadata_entry_->edge_condensed_type_id_map_size_;
  topo.node_condensed_type_id_map_size_ =
      topo.metadata_entry_->node_condensed_type_id_map_size_;

  // when we make from storage primitives, we can say the storage is up to date
  topo.storage_valid_ = true;

  return katana::Result<tsuba::RDGTopology>(std::move(topo));
}

size_t
tsuba::RDGTopology::GetGraphSize() const {
  /// version, sizeof_edge_data, num_nodes, num_edges
  constexpr int mandatory_fields = 4;
  size_t graphsize = (mandatory_fields + num_nodes_) * sizeof(uint64_t) +
                     (num_edges_ * sizeof(uint32_t));

  KATANA_LOG_DEBUG("Base graph size = {}", graphsize);

  if (metadata_entry_->edge_index_to_property_index_map_present_) {
    // 1 is for the magic number
    graphsize += (1 * sizeof(uint64_t)) + (num_edges_ * sizeof(uint64_t));
    KATANA_LOG_DEBUG(
        "edge_index_to_property_index_map_present graph size = {}", graphsize);
  }

  if (metadata_entry_->node_index_to_property_index_map_present_) {
    // 1 is for the magic number
    graphsize += (1 * sizeof(uint64_t)) + (num_nodes_ * sizeof(uint64_t));
    KATANA_LOG_DEBUG(
        "node_index_to_property_index_map_present graph size = {}", graphsize);
  }

  if (metadata_entry_->edge_condensed_type_id_map_present_) {
    // 1 is for the magic number
    graphsize +=
        (1 * sizeof(uint64_t)) + (num_edges_ * sizeof(katana::EntityTypeID));
    KATANA_LOG_DEBUG(
        "edge_condensed_type_id_map_present graph size = {}", graphsize);
  }

  if (metadata_entry_->node_condensed_type_id_map_present_) {
    // 1 is for the magic number
    graphsize +=
        (1 * sizeof(uint64_t)) + (num_nodes_ * sizeof(katana::EntityTypeID));
    KATANA_LOG_DEBUG(
        "node_condensed_type_id_map_present graph size = {}", graphsize);
  }

  KATANA_LOG_DEBUG("Total graph size = {}", graphsize);
  return graphsize;
}

RDGTopology::RDGTopology(PartitionTopologyMetadataEntry* metadata_entry)
    : metadata_entry_(metadata_entry) {}

RDGTopology::RDGTopology() = default;

}  // namespace tsuba
