#ifndef KATANA_LIBTSUBA_TSUBA_RDGTOPOLOGY_H_
#define KATANA_LIBTSUBA_TSUBA_RDGTOPOLOGY_H_

#include <array>

#include "katana/EntityTypeManager.h"
#include "katana/ErrorCode.h"
#include "katana/JSON.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/config.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"
#include "tsuba/WriteGroup.h"
#include "tsuba/tsuba.h"

namespace tsuba {

class PartitionTopologyMetadataEntry;

//TODO: emcginnis the import path also defines the csr topology layout, make sure to update that as well.
class KATANA_EXPORT RDGTopology {
public:
  RDGTopology();
  RDGTopology(PartitionTopologyMetadataEntry* metadata_entry);

  //
  // Enums
  // *** Important ***
  // Make sure to update the NLOHMANN_JSON_SERIALIZE_ENUMs
  // in the RDGPartHeader if you add any entries to these enums
  //
  enum class TransposeKind : int { kInvalid = -1, kNo = 0, kYes, kAny };

  enum class EdgeSortKind : int {
    kInvalid = -1,
    kAny = 0,  // don't care. Sorted or Unsorted
    kSortedByDestID,
    kSortedByEdgeType,
    kSortedByNodeType
  };

  enum class NodeSortKind : int {
    kInvalid = -1,
    kAny = 0,
    kSortedByDegree,
    kSortedByNodeType
  };

  enum class TopologyKind : int {
    kInvalid = -1,
    kCSR = 0,
    kEdgeShuffleTopology,
    kShuffleTopology,
    kEdgeTypeAwareTopology
  };

  //
  // File Store Accessors/Mutators
  //

  /// Get the fileview for the topology file on storage
  const FileView& file_storage() const { return file_storage_; }

  /// Get the fileview for the topology file on storage
  FileView& file_storage() { return file_storage_; }

  /// Set the fileview for the topology file to be stored
  void set_file_storage(FileView&& file_storage) {
    file_storage_ = std::move(file_storage);
  }

  /// Invalidate the in memory RDGTopology structures
  void unmap_file_storage() {
    adj_indices_ = nullptr;
    dests_ = nullptr;
    edge_index_to_property_index_map_ = nullptr;
    node_index_to_property_index_map_ = nullptr;
    edge_condensed_type_id_map_ = nullptr;
    node_condensed_type_id_map_ = nullptr;
    file_store_mapped_ = false;
  }

  /// Invalidate the file store
  katana::Result<void> unbind_file_storage() {
    // must unmap the file store if we are unbinding
    if (file_store_mapped_) {
      unmap_file_storage();
    }

    if (file_store_bound_) {
      KATANA_CHECKED(file_storage_.Unbind());
      file_store_bound_ = false;
    }
    return katana::ResultSuccess();
  }

  //
  // Metadata Accessors/Mutators
  //

  uint64_t num_edges() const { return num_edges_; }

  uint64_t num_nodes() const { return num_nodes_; }

  /// Requires backing FileView to be mapped & bound, or the RDGTopology to be filled from memory
  const uint64_t* adj_indices() const {
    KATANA_LOG_VASSERT(
        adj_indices_ != nullptr,
        "RDGTopology must be either bound & mapped, or filled from memory");
    return adj_indices_;
  }

  /// Requires backing FileView to be mapped & bound, or the RDGTopology to be filled from memory
  const uint32_t* dests() const {
    KATANA_LOG_VASSERT(
        dests_ != nullptr,
        "RDGTopology must be either bound & mapped, or filled from memory");
    return dests_;
  }

  /// Optional field, may not be present depending on the kind of topology this is
  /// Requires backing FileView to be mapped & bound, or the RDGTopology to be filled from memory
  const uint64_t* node_index_to_property_index_map() const {
    KATANA_LOG_VASSERT(
        node_index_to_property_index_map_ != nullptr,
        "Either this optional field is not present, or the RDGTopology must be "
        "either bound & mapped, or filled from memory.");
    return node_index_to_property_index_map_;
  }

  /// Optional field, may not be present depending on the kind of topology this is
  /// Requires backing FileView to be mapped & bound, or the RDGTopology to be filled from memory
  const uint64_t* edge_index_to_property_index_map() const {
    KATANA_LOG_VASSERT(
        edge_index_to_property_index_map_ != nullptr,
        "Either this optional field is not present, or the RDGTopology must be "
        "either bound & mapped, or filled from memory.");
    return edge_index_to_property_index_map_;
  }

  /// Optional field, may not be present depending on the kind of topology this is
  /// Requires backing FileView to be mapped & bound, or the RDGTopology to be filled from memory
  const katana::EntityTypeID* edge_condensed_type_id_map() const {
    if (edge_condensed_type_id_map_size() > 0) {
      KATANA_LOG_VASSERT(
          edge_condensed_type_id_map_ != nullptr,
          "Either this optional field is not present, or the RDGTopology must "
          "be "
          "either bound & mapped, or filled from memory.");
    }
    return edge_condensed_type_id_map_;
  }

  /// Optional field, may not be present depending on the kind of topology this is
  /// Requires backing FileView to be mapped & bound, or the RDGTopology to be filled from memory
  const katana::EntityTypeID* node_condensed_type_id_map() const {
    if (node_condensed_type_id_map_size() > 0) {
      KATANA_LOG_VASSERT(
          node_condensed_type_id_map_ != nullptr,
          "Either this optional field is not present, or the RDGTopology must "
          "be "
          "either bound & mapped, or filled from memory.");
    }
    return node_condensed_type_id_map_;
  }

  uint64_t edge_condensed_type_id_map_size() const {
    return edge_condensed_type_id_map_size_;
  }

  uint64_t node_condensed_type_id_map_size() const {
    return node_condensed_type_id_map_size_;
  }

  TopologyKind topology_state() const { return topology_state_; }

  TransposeKind transpose_state() const { return transpose_state_; }

  EdgeSortKind edge_sort_state() const { return edge_sort_state_; }

  NodeSortKind node_sort_state() const { return node_sort_state_; }

  std::string path() const;
  void set_path(const std::string& path);

  bool bound() const { return file_store_bound_; }

  bool mapped() const { return file_store_mapped_; }

  bool invalid() const { return invalid_; }

  void set_invalid();

  void set_metadata_entry(PartitionTopologyMetadataEntry* entry);

  bool metadata_entry_valid() const;

  /// Bind a topology file to the file_storage object, bind entire file
  katana::Result<void> Bind(
      const katana::Uri& metadata_dir, bool resolve = true);

  /// Bind a topology file to the file_storage object, bind specific offset
  katana::Result<void> Bind(
      const katana::Uri& metadata_dir, uint64_t begin, uint64_t end,
      bool resolve);

  /// Map takes the file buffer of a topology file and extracts the
  /// topology elements
  ///
  /// Format of a topology file (extended from the original FileGraph.cpp)
  /// Supports optional topology data structures. Presence of these optional data structures
  /// is defined in the PartitionTopologyMetadataEntry. Any optional topology data structure
  /// may or may not be present in the topology file.
  /// A magic_number is placed before each of these optional data structures as a sanity check.
  ///
  ///   uint64_t version: expected to be 1
  ///   uint64_t sizeof_edge_data: size of edge data element
  ///   uint64_t num_nodes: number of nodes
  ///   uint64_t num_edges: number of edges
  ///   uint64_t[num_nodes] out_indices: start and end of the edges for a node
  ///   uint32_t[num_edges] out_dests: destinations (node indexes) of each edge
  ///   uint32_t padding if num_edges is odd
  ///
  ///   <optional topology data structures follow>
  ///
  ///   uint64_t magic_number: sum of num_edges + num_nodes
  ///   uint64_t[num_edges] edge_index_to_property_index_map: translation map for this topologys edge_indices to the property_index array
  ///   uint64_t magic_number: sum of num_edges + num_nodes
  ///   uint64_t[num_nodes] node_index_to_property_index_map: translation map for this topologys node_indices to the property_index array
  ///   uint64_t magic_number: sum of num_edges + num_nodes
  ///   katana::EntityTypeID edge_condensed_type_id_map: condensed map of the edges EntityTypeIDs
  ///   uint64_t magic_number: sum of num_edges + num_nodes
  ///   katana::EntityTypeID node_condensed_type_id_map: condensed map of the nodes EntityTypeIDs
  ///
  /// Since property graphs store their edge data separately, we
  /// ignore the size_of_edge_data (data[1]) and the
  /// void*[num_edges] edge_data
  /// defined by FileGraph.cpp
  katana::Result<void> Map();

  /// Map a topology file and extract its metadata
  /// this only loads the topology metadata into the PartitionTopologyMetadataEntry
  /// *ONLY USE THIS FOR BACKWARDS COMPATIBILITY*
  /// bool storage_valid: whether this topology should be written out
  /// to a file on Store. Used by graph-convert, where we don't load the
  /// entire topology file into memory.
  katana::Result<void> MapMetadataExtract(
      uint64_t num_nodes, uint64_t num_edges, bool storage_valid = false);

  katana::Result<void> DoStore(
      RDGHandle handle, const katana::Uri& current_rdg_dir,
      std::unique_ptr<tsuba::WriteGroup>& write_group);

  bool Equals(const RDGTopology& other) const;

  /// Create a shadow RDGTopology with parameters
  static tsuba::RDGTopology MakeShadow(
      TopologyKind topology_state, TransposeKind transpose_state,
      EdgeSortKind edge_sort_state, NodeSortKind node_sort_state);

  /// Create a shadow RDGTopology with default CSR state
  static tsuba::RDGTopology MakeShadowCSR();

  /// Make a new basic RDGTopology from in memory structures
  static katana::Result<tsuba::RDGTopology> Make(
      const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
      uint64_t num_edges, TopologyKind topology_state,
      TransposeKind transpose_state, EdgeSortKind edge_sort_state,
      NodeSortKind node_sort_state);

  /// Make an RDGTopology for an EdgeShuffle related Topology from in memory structures
  static katana::Result<tsuba::RDGTopology> Make(
      const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
      uint64_t num_edges, TopologyKind topology_state,
      TransposeKind transpose_state, EdgeSortKind edge_sort_state,
      const uint64_t* edge_index_to_property_index_map);

  /// Make an RDGTopology for an EdgeTypeAware related Topology from in memory structures
  static katana::Result<tsuba::RDGTopology> Make(
      const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
      uint64_t num_edges, TopologyKind topology_state,
      TransposeKind transpose_state, EdgeSortKind edge_sort_state,
      const uint64_t* edge_index_to_property_index_map,
      uint64_t edge_condensed_type_id_map_size,
      const katana::EntityTypeID* edge_condensed_type_id_map_);

  /// Make an RDGTopology for a Shuffle related Topology from in memory structures
  static katana::Result<tsuba::RDGTopology> Make(
      const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
      uint64_t num_edges, TopologyKind topology_state,
      TransposeKind transpose_state, EdgeSortKind edge_sort_state,
      NodeSortKind node_sort_state,
      const uint64_t* edge_index_to_property_index_map,
      const uint64_t* node_index_to_property_index_map);

  /// Make and fully populate an RDGTopology from in memory structures
  static katana::Result<tsuba::RDGTopology> Make(
      const uint64_t* adj_indices, uint64_t num_nodes, const uint32_t* dests,
      uint64_t num_edges, TopologyKind topology_state,
      TransposeKind transpose_state, EdgeSortKind edge_sort_state,
      NodeSortKind node_sort_state,
      const uint64_t* edge_index_to_property_index_map,
      const uint64_t* node_index_to_property_index_map,
      uint64_t edge_condensed_type_id_map_size,
      const katana::EntityTypeID* edge_condensed_type_id_map_,
      uint64_t node_condensed_type_id_map_size,
      const katana::EntityTypeID* node_condensed_type_id_map_);

  // Make an RDGTopology from on storage metadata
  static katana::Result<tsuba::RDGTopology> Make(
      PartitionTopologyMetadataEntry* entry);

private:
  // valid immediately, stored in metadata
  uint64_t num_edges_{0};
  uint64_t num_nodes_{0};
  //TODO: emcginnis PartitionMetadata.h has a transposed flag, see if that is the same thing and replace it?
  TopologyKind topology_state_{-1};
  TransposeKind transpose_state_{-1};
  EdgeSortKind edge_sort_state_{-1};
  NodeSortKind node_sort_state_{-1};
  uint64_t edge_condensed_type_id_map_size_{0};
  uint64_t node_condensed_type_id_map_size_{0};

  // File store state
  /// Flag to show if we have mapped the file store to memory
  bool file_store_mapped_{false};
  /// Flag to show if we have bound the file at PartitionTopologyMetadataEntry.path_ to a file store
  bool file_store_bound_{false};
  /// Flag to show if the file on disk is up to date with our in memory represenation
  bool storage_valid_{false};
  /// Flag to show if this RDGTopology is invalid, and shouldn't be stored or used
  bool invalid_{false};

  // index into the partition_metadata_entries array, also indicates this RDGTopology came from storage
  // rather than purely created in memory
  PartitionTopologyMetadataEntry* metadata_entry_{nullptr};

  // must be loaded from file store or set
  const uint64_t* adj_indices_{nullptr};
  const uint32_t* dests_{nullptr};
  const uint64_t* edge_index_to_property_index_map_{nullptr};
  const uint64_t* node_index_to_property_index_map_{nullptr};
  const katana::EntityTypeID* edge_condensed_type_id_map_{nullptr};
  const katana::EntityTypeID* node_condensed_type_id_map_{nullptr};

  FileView file_storage_;

  static katana::Result<tsuba::RDGTopology> DoMake(
      tsuba::RDGTopology topo, const uint64_t* adj_indices, uint64_t num_nodes,
      const uint32_t* dests, uint64_t num_edges, TopologyKind topology_state,
      TransposeKind transpose_state, EdgeSortKind edge_sort_state,
      NodeSortKind node_sort_state);

  size_t GetGraphSize() const;

  // Topology File Offset Definitions
  static constexpr size_t version_num_offset = 0;
  static constexpr size_t num_nodes_offset_ = 2;
  static constexpr size_t num_edges_offset_ = 3;
  static constexpr size_t adj_indices_offset = 4;
};

// Definitions
using RDGTopologySet = std::array<RDGTopology, kMaxNumTopologies>;

}  // namespace tsuba

#endif
