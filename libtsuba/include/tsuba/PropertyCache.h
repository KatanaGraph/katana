#ifndef KATANA_LIBTSUBA_TSUBA_PROPERTYCACHE_H_
#define KATANA_LIBTSUBA_TSUBA_PROPERTYCACHE_H_

#include <arrow/api.h>

#include "katana/Cache.h"

namespace tsuba {

enum class NodeEdge { kNode = 10, kEdge, kNeitherNodeNorEdge };

class KATANA_EXPORT PropertyCacheKey {
public:
  PropertyCacheKey(
      NodeEdge node_edge, const std::string& rdg_prefix,
      const std::string& prop_name)
      : node_edge_(node_edge), rdg_prefix_(rdg_prefix), prop_name_(prop_name) {}
  bool operator==(const PropertyCacheKey& o) const {
    return node_edge_ == o.node_edge_ && rdg_prefix_ == o.rdg_prefix_ &&
           prop_name_ == o.prop_name_;
  }
  const char* TypeAsConstChar() const {
    return (node_edge_ == NodeEdge::kNode) ? "node" : "edge";
  }
  bool is_node() const { return node_edge_ == NodeEdge::kNode; }
  bool is_edge() const { return node_edge_ == NodeEdge::kEdge; }
  std::string prop_name() const { return prop_name_; }
  std::string rdg_prefix() const { return rdg_prefix_; }

  struct Hash {
    std::size_t operator()(const PropertyCacheKey& k) const {
      using boost::hash_combine;
      using boost::hash_value;

      std::size_t seed = 0;
      hash_combine(seed, hash_value(k.node_edge_));
      hash_combine(seed, hash_value(k.rdg_prefix_));
      hash_combine(seed, hash_value(k.prop_name_));

      // Return the result.
      return seed;
    }
  };

private:
  // Is this a node or edge property name
  NodeEdge node_edge_;
  // What RDG does this property belong to
  std::string rdg_prefix_;
  // Node property names are unique, and we enforce that.
  // Edge property names are unique, and we enforce that.
  std::string prop_name_;
};

class RDG;

using PropertyCache =
    katana::Cache<PropertyCacheKey, std::shared_ptr<arrow::Table>, RDG*>;
}  // namespace tsuba

#endif
