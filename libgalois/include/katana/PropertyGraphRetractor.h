#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYGRAPHRETRACTOR_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYGRAPHRETRACTOR_H_

#include "katana/PropertyGraph.h"

namespace katana {

/// A PropertyGraphRetractor provides a interfaces to some normally hidden
/// parts of PropertyGraph; similar to the way a surgical retractor holds an
/// incision open to provide access to normally hidden parts of our anatomy.
///
/// This is useful for cases like partitioning where extra
/// metadata must be associated with a PropertyGraph, or where PropertyGraphs
/// need to by dismantled piece-by-piece to save memory.
///
/// N.b.: some of these methods will leave the underlying PropertyGraph in an
/// inconsistent state, always prefer using a PropertyGraph directly unless
/// you're sure you need this.
class KATANA_EXPORT PropertyGraphRetractor {
public:
  PropertyGraphRetractor(std::unique_ptr<PropertyGraph> pg)
      : pg_(std::move(pg)) {}

  const tsuba::PartitionMetadata& partition_metadata() const {
    return pg_->rdg_.part_metadata();
  }
  void set_partition_metadata(const tsuba::PartitionMetadata& meta) {
    pg_->rdg_.set_part_metadata(meta);
  }

  void update_rdg_metadata(const std::string& part_policy, uint32_t num_hosts) {
    pg_->rdg_.set_view_name(
        fmt::format("rdg-{}-part{}", part_policy, num_hosts));
  }

  /// Per-host vector of master nodes
  ///
  /// master_nodes()[this_host].empty() is true
  /// master_nodes()[host_i][x] contains LocalNodeID of masters
  //    for which host_i has a mirror
  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& master_nodes()
      const {
    return pg_->rdg_.master_nodes();
  }
  void set_master_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    pg_->rdg_.set_master_nodes(std::move(a));
  }

  /// Per-host vector of mirror nodes
  ///
  /// mirror_nodes()[this_host].empty() is true
  /// mirror_nodes()[host_i][x] contains LocalNodeID of mirrors
  ///   that have a master on host_i
  const std::vector<std::shared_ptr<arrow::ChunkedArray>>& mirror_nodes()
      const {
    return pg_->rdg_.mirror_nodes();
  }
  void set_mirror_nodes(std::vector<std::shared_ptr<arrow::ChunkedArray>>&& a) {
    pg_->rdg_.set_mirror_nodes(std::move(a));
  }

  /// Return the node property table for local nodes
  const std::shared_ptr<arrow::Table>& node_properties() const {
    return pg_->rdg_.node_properties();
  }

  /// Return the edge property table for local edges
  const std::shared_ptr<arrow::Table>& edge_properties() const {
    return pg_->rdg_.edge_properties();
  }

  /// Return false if type information has been loaded separate from properties.
  /// Return true otherwise.
  bool NeedsEntityTypeIDInference() {
    return !pg_->rdg_.IsEntityTypeIDsOutsideProperties();
  }

  /// This is exposed because type id mappings change sometimes.
  void ReplaceNodeTypeManager(EntityTypeManager&& manager) {
    pg_->node_entity_type_manager_ = std::move(manager);
  }

  /// This is exposed because type id mappings change sometimes.
  void ReplaceEdgeTypeManager(EntityTypeManager&& manager) {
    pg_->edge_entity_type_manager_ = std::move(manager);
  }

  /// Tell the RDG where it's data is coming from
  Result<void> InformPath(const std::string& input_path);

  /// Vector from storage mapping host to global node ID ranges
  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_node_ids()
      const {
    return pg_->rdg_.host_to_owned_global_node_ids();
  }
  void set_host_to_owned_global_node_ids(
      std::shared_ptr<arrow::ChunkedArray>&& a) {
    pg_->rdg_.set_host_to_owned_global_node_ids(std::move(a));
  }

  /// Vector from storage mapping host to global edge ID ranges
  const std::shared_ptr<arrow::ChunkedArray>& host_to_owned_global_edge_ids()
      const {
    return pg_->rdg_.host_to_owned_global_edge_ids();
  }
  void set_host_to_owned_global_edge_ids(
      std::shared_ptr<arrow::ChunkedArray>&& a) {
    pg_->rdg_.set_host_to_owned_global_edge_ids(std::move(a));
  }

  /// Vector from storage mapping local node ID to UserID
  const std::shared_ptr<arrow::ChunkedArray>& local_to_user_id() const {
    return pg_->rdg_.local_to_user_id();
  }
  void set_local_to_user_id(std::shared_ptr<arrow::ChunkedArray>&& a) {
    pg_->rdg_.set_local_to_user_id(std::move(a));
  }

  /// Vector from storage mapping local node ID to global node ID
  const std::shared_ptr<arrow::ChunkedArray>& local_to_global_id() const {
    return pg_->rdg_.local_to_global_id();
  }
  void set_local_to_global_id(std::shared_ptr<arrow::ChunkedArray>&& a) {
    pg_->rdg_.set_local_to_global_id(std::move(a));
  }

  const tsuba::PropertyCache* prop_cache() const {
    return pg_->rdg_.prop_cache();
  }

  tsuba::PropertyCache* prop_cache() { return pg_->rdg_.prop_cache(); }

  void set_prop_cache(tsuba::PropertyCache* prop_cache) {
    pg_->rdg_.set_prop_cache(prop_cache);
  }

  /// Deallocate and forget about all topology information associated with the
  /// managed PropertyGraph
  Result<void> DropTopologies();

  /// access the managed PropertyGraph
  PropertyGraph& property_graph() { return *pg_; }
  const PropertyGraph& property_graph() const { return *pg_; }

private:
  std::unique_ptr<PropertyGraph> pg_;
};

}  // namespace katana

#endif
