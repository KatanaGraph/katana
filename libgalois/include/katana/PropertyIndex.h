#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYINDEX_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYINDEX_H_

#include <bitset>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/chunked_array.h>
#include <arrow/type_traits.h>
#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include "katana/config.h"
#include "katana/PropertyGraph.h"
#include "katana/Result.h"

namespace katana {

// PropertyIndex provides an interface similar to an ordered container
// over a single property.
class KATANA_EXPORT PropertyIndex {
public:
  // PropertyIndex::iterator returns a sequence of node or edge ids.
  using node_iterator = std::set<GraphTopology::Node>::iterator;
  using edge_iterator = std::set<GraphTopology::Edge>::iterator;

  // Create an index from a column of pg.
  static katana::Result<std::unique_ptr<PropertyIndex>>
    Make(PropertyGraph* pg, std::string column);

  std::string column() { return column_; }

  virtual node_iterator node_begin() = 0;
  virtual node_iterator node_end() = 0;

  virtual edge_iterator edge_begin() = 0;
  virtual edge_iterator edge_end() = 0;

private:
  std::string column_;
};

// BasicPropertyIndex provides a PropertyIndex for anything that can be used
// in a default std::set.
template<typename key_type>
class KATANA_EXPORT BasicPropertyIndex : public PropertyIndex {
public:
  node_iterator node_begin() override { return node_set_.begin(); }
  node_iterator node_end() override { return node_set_.end(); }

  node_iterator NodeFind(key_type key) { return node_set_.find(key); }
  node_iterator NodeLowerBound(key_type key) {
    return node_set_.lower_bound(key);
  }
  node_iterator NodeUpperBound(key_type key) {
    return node_set_.upper_bound(key);
  }

  edge_iterator edge_begin() override { return edge_set_.begin(); }
  edge_iterator edge_end() override { return edge_set_.end(); }

  edge_iterator EdgeFind(key_type key) { return edge_set_.find(key); }
  edge_iterator EdgeLowerBound(key_type key) {
    return edge_set_.lower_bound(key);
  }
  edge_iterator EdgeUpperBound(key_type key) {
    return edge_set_.upper_bound(key);
  }

private:
  struct PropertyCompare {
    bool operator() (GraphTopology::Node a, GraphTopology::Node b) {
      // Compare the value of the indexed property for a and b using std::less.
      (void)a;
      (void)b;
      return true;
    }
  };

  std::set<GraphTopology::Node, PropertyCompare> node_set_;
  std::set<GraphTopology::Edge, PropertyCompare> edge_set_;
};

}  // namespace katana
 
#endif