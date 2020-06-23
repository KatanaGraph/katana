#ifndef GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTY_FILE_GRAPH_H_
#define GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTY_FILE_GRAPH_H_

#include <arrow/api.h>
#include <boost/outcome/outcome.hpp>

#include <string>
#include <utility>
#include <vector>

#include "galois/FileView.h"

namespace outcome = BOOST_OUTCOME_V2_NAMESPACE;

namespace galois::graphs {

/// A graph topology represents the adjacency information for a graph in CSR
/// format.
struct GraphTopology {
  std::shared_ptr<arrow::UInt64Array> out_indices;
  std::shared_ptr<arrow::UInt32Array> out_dests;

  uint64_t num_nodes() const { return out_indices ? out_indices->length() : 0; }

  uint64_t num_edges() const { return out_dests ? out_dests->length() : 0; }
};

// Private implementation of metadata to avoid exposing implementation details
// while the metadata format is in flux.
struct MetadataImpl;

/// A property graph is a graph that has properties associated with its nodes
/// and edges. A property has a name and value. Its value may be a primitive
/// type, a list of values or a composition of properties.
///
/// A PropertyFileGraph is a representation of a property graph that is backed
/// by persistent storage, and it may be a subgraph of a larger, global property
/// graph. Another way to view a PropertyFileGraph is as a container for node
/// and edge properties that can be serialized.
///
/// A property graph on disk consists of:
///
/// - A topology file: the serialization of the CSR representation of the graph
/// - A Parquet file for each property on an edge
/// - A Parquet file for each property on a node
/// - In addition to user-defined node properties, there are the following
///   reserved, system-defined properties associated with each node:
///   - Global ID: what global node does this node correspond to
///   - Owner Partition: which partition contains the definitive version of
///     this nodes properties
/// - A metadata file: Arrow-serialized buffer that contains an index to the
///   files above. The location of the metadata file is the canonical name of
///   the property graph as a whole.
///
/// Although property graphs are typically partitioned, a non-partitioned
/// property graph can be simply represented as a property graph with one
/// partition.
///
/// Since a PropertyFileGraph supports a superset the capabilities of a
/// FileGraph, the class PropertyFileGraph supersedes FileGraph, and the latter
/// is deprecated.
class PropertyFileGraph {
  static outcome::std_result<std::shared_ptr<PropertyFileGraph>>
  Make(MetadataImpl&& metadata);

  galois::FileView topology_file_storage_;

  std::shared_ptr<arrow::Table> node_table_;
  std::shared_ptr<arrow::Table> edge_table_;

  // The topology is either backed by topology_file_storage_ or shared with the
  // caller of SetTopology.
  GraphTopology topology_;

  // Metadata is the parsed metadata from loading the on-disk representation
  std::unique_ptr<MetadataImpl> metadata_;

public:
  PropertyFileGraph();

  // Declare destructor out-of-line to accomodate incomplete type MetadataImpl.
  ~PropertyFileGraph();

  /// Make a property graph from a metadata file.
  static outcome::std_result<std::shared_ptr<PropertyFileGraph>>
  Make(const std::string& metadata_path);

  /// Make a property graph from a metadata file but only load the
  /// named node and edge properties.
  ///
  /// The order of properties in the resulting graph will match the order of
  /// given in the property arguments.
  ///
  /// \returns invalid_argument if any property is not found or if there
  /// are multiple properties with the same name
  static outcome::std_result<std::shared_ptr<PropertyFileGraph>>
  Make(const std::string& metadata_path,
       const std::vector<std::string>& node_properties,
       const std::vector<std::string>& edge_properties);

  /// Write the property graph to the given path. In addition to the metadata
  /// file that will be written to the metadata path, additional property
  /// files will be written to the same directory.
  ///
  /// \returns io_error if, for instance, a file already exists
  outcome::std_result<void> Write(const std::string& metadata_path);

  std::shared_ptr<arrow::Schema> node_schema() const {
    return node_table_->schema();
  }

  std::shared_ptr<arrow::Schema> edge_schema() const {
    return edge_table_->schema();
  }

  std::shared_ptr<arrow::ChunkedArray> NodeProperty(int i) const {
    return node_table_->column(i);
  }

  std::shared_ptr<arrow::ChunkedArray> EdgeProperty(int i) const {
    return edge_table_->column(i);
  }

  const GraphTopology& topology() const { return topology_; }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> NodeProperties() const {
    return node_table_->columns();
  }

  std::vector<std::shared_ptr<arrow::ChunkedArray>> EdgeProperties() const {
    return edge_table_->columns();
  }

  outcome::std_result<void>
  AddNodeProperties(const std::shared_ptr<arrow::Table>& table);
  outcome::std_result<void>
  AddEdgeProperties(const std::shared_ptr<arrow::Table>& table);

  outcome::std_result<void> RemoveNodeProperty(int i);
  outcome::std_result<void> RemoveEdgeProperty(int i);

  outcome::std_result<void> SetTopology(const GraphTopology& topology);
};

} // namespace galois::graphs

#endif
