#ifndef KATANA_LIBTSUBA_TSUBA_PROPERTYCACHE_H_
#define KATANA_LIBTSUBA_TSUBA_PROPERTYCACHE_H_

#include "katana/Cache.h"

namespace tsuba {

enum class NodeEdge { kNode = 10, kEdge };

struct KATANA_EXPORT PropertyCacheKey {
  NodeEdge node_edge;
  std::string name;
  PropertyCacheKey(NodeEdge _node_edge) : node_edge(_node_edge) {}
  bool operator==(const PropertyCacheKey& o) const {
    return node_edge == o.node_edge && name == o.name;
  }
  struct Hash {
    std::size_t operator()(const PropertyCacheKey& k) const {
      using boost::hash_combine;
      using boost::hash_value;

      std::size_t seed = 0;
      hash_combine(seed, hash_value(k.node_edge));
      hash_combine(seed, hash_value(k.name));

      // Return the result.
      return seed;
    }
  };
};

// Property names are unique, and we enforce that.
// Each table should only contain a single column.
using PropertyCache =
    katana::Cache<PropertyCacheKey, std::shared_ptr<arrow::Table>>;

}  // namespace tsuba

#endif
