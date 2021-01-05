#ifndef GALOIS_LIBGALOIS_GALOIS_OPLOG_H_
#define GALOIS_LIBGALOIS_GALOIS_OPLOG_H_

#include "galois/BuildGraph.h"
#include "galois/Uri.h"

namespace galois {

enum class DataTypes {
  kEmpty,
  kNode,
  kEdge,
};

// https://neo4j.com/docs/cypher-manual/current/clauses/create/
enum class OpTypes {
  kInvalid,
  kOpNodeAdd = 1,
  kOpNodeDel,
  kOpEdgeAdd,
  kOpEdgeDel,
  kOpNodePropDel,
  kOpEdgePropDel,
  kOpNodePropVal,
  kOpEdgePropVal,
};

const char* const optypes_enum2str[] = {
    "Invalid",     /* kInvalid */
    "NodeAdd",     /* kOpNodeAdd */
    "NodeDel",     /* kOpNodeDel */
    "EdgeAdd",     /* kOpEdgeAdd */
    "EdgeDel",     /* kOpEdgeDel */
    "NodePropDel", /* kOpNodePropDel */
    "EdgePropDel", /* kOpEdgePropDel */
    "NodePropVal", /* kOpNodePropVal */
    "EdgePropVal", /* kOpEdgePropVal */
    "Consumed",    /* kConsumed */
};

class GALOIS_EXPORT Operation {
  OpTypes opcode_{0};
  galois::PropertyKey property_key_{
      "", false, false, "", galois::ImportDataType::kUnsupported, false};
  galois::ImportData data_{galois::ImportDataType::kUnsupported, false};

public:
  /// For everything except kOpNodePropVal, kOpEdgePropVal
  Operation(OpTypes opcode, galois::PropertyKey property_key)
      : opcode_(opcode), property_key_(property_key) {}
  /// For kOpNodePropVal, kOpEdgePropVal
  Operation(
      OpTypes opcode, galois::PropertyKey property_key, galois::ImportData data)
      : opcode_(opcode), property_key_(property_key), data_(data) {}

  OpTypes opcode() const { return opcode_; }
  uint64_t id() const {
    if (property_key_.id.empty()) {
      return UINT64_C(0);
    }
    return std::stoul(property_key_.id, nullptr, 0);
  }
  galois::PropertyKey key() const { return property_key_; }
  galois::ImportData data() const { return data_; }
};

class GALOIS_EXPORT OpLog {
  std::vector<Operation> log_;

public:
  OpLog() = default;
  /// Read/write operation log to URI
  OpLog(const galois::Uri& uri);
  /// Read an operation at the given index
  Operation GetOp(uint64_t idx) const;
  /// Write an operation, return the log offset that was written
  uint64_t AppendOp(const Operation& op);
  /// Get the number of log entries
  uint64_t size() const;
  /// Erase log contents
  void Clear();
};

/// A graph update object is constructed from a log to represent the graph state
/// obtained by playing the log.  If the log contains redundant or contradictory
/// operations, these are resolved as the operations are played into the GraphUpdate
/// object (which is mutable, unlike a graph).
///
/// The GraphUpdate object maintains pointers into the log, represented as log indices.
///
/// The ingest process takes a GraphUpdate object and its log and merges it into an existing
/// graph.
class GALOIS_EXPORT GraphUpdate {
  // A vector of node and edge property updates, one per property
  // Each property update has an entry for each local node/edge.
  // Each update is an index into an OpLog
  std::vector<std::vector<uint64_t>> nprop_;
  std::vector<std::string> nprop_names_;
  std::vector<std::vector<uint64_t>> eprop_;
  std::vector<std::string> eprop_names_;
  uint64_t num_nodes_;
  uint64_t num_edges_;

  uint32_t RegisterProp(
      const std::string& name, uint64_t num,
      std::vector<std::vector<uint64_t>>& prop,
      std::vector<std::string>& names) {
    uint32_t index = names.size();
    names.emplace_back(name);
    std::vector<uint64_t> initial(num);
    GALOIS_LOG_ASSERT((uint64_t)index == prop.size());
    prop.emplace_back(initial);
    return index;
  }
  /// Set the value of a property
  void SetProp(
      uint32_t pnum, uint64_t index, uint64_t op_log_index,
      std::vector<std::vector<uint64_t>>& prop) {
    if (pnum >= prop.size()) {
      GALOIS_LOG_DEBUG(
          "Property number {} is out of bounds ({})", pnum, prop.size());
      return;
    }
    if (index >= prop[pnum].size()) {
      GALOIS_LOG_DEBUG(
          "Property index {} is out of bounds ({})", index, prop[pnum].size());
      return;
    }
    prop[pnum][index] = op_log_index;
  }

public:
  GraphUpdate(uint64_t num_nodes, uint64_t num_edges)
      : num_nodes_(num_nodes), num_edges_(num_edges) {}

  uint32_t num_nprop() const { return nprop_.size(); }
  uint32_t num_eprop() const { return eprop_.size(); }

  /// When a new node/edge property is added, call these functions to register it and get back
  /// the property index.
  uint32_t RegisterNodeProp(const std::string& name) {
    return RegisterProp(name, num_nodes_, nprop_, nprop_names_);
  }
  uint32_t RegisterEdgeProp(const std::string& name) {
    return RegisterProp(name, num_edges_, eprop_, eprop_names_);
  }
  std::string GetNName(uint32_t pnum) {
    if (pnum >= nprop_names_.size()) {
      GALOIS_LOG_DEBUG(
          "Property number {} is out of bounds ({})", pnum,
          nprop_names_.size());
      return "";
    }
    return nprop_names_[pnum];
  }
  std::vector<uint64_t> GetNIndices(uint32_t pnum) {
    if (pnum >= nprop_names_.size()) {
      GALOIS_LOG_DEBUG(
          "Property number {} is out of bounds ({})", pnum,
          nprop_names_.size());
      return {};
    }
    return nprop_[pnum];
  }
  std::string GetEName(uint32_t pnum) {
    if (pnum >= eprop_names_.size()) {
      GALOIS_LOG_DEBUG(
          "Property number {} is out of bounds ({})", pnum,
          eprop_names_.size());
      return "";
    }
    return eprop_names_[pnum];
  }
  std::vector<uint64_t> GetEIndices(uint32_t pnum) {
    if (pnum >= eprop_names_.size()) {
      GALOIS_LOG_DEBUG(
          "Property number {} is out of bounds ({})", pnum,
          eprop_names_.size());
      return {};
    }
    return eprop_[pnum];
  }

  void SetNProp(uint32_t pnum, uint64_t index, uint64_t op_log_index) {
    SetProp(pnum, index, op_log_index, nprop_);
  }
  void SetEProp(uint32_t pnum, uint64_t index, uint64_t op_log_index) {
    SetProp(pnum, index, op_log_index, eprop_);
  }
};

}  // namespace galois
#endif
