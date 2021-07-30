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

#include "katana/PropertyGraph.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

// PropertyIndex provides an interface similar to an ordered container
// over a single property.
template <typename node_or_edge>
class KATANA_EXPORT PropertyIndex {
public:
  // PropertyIndex::iterator returns a sequence of node or edge ids.
  using iterator = typename std::set<node_or_edge>::iterator;

  PropertyIndex(std::string column) : column_(std::move(column)) {}

  // Create an index from a column of pg.
  static Result<std::unique_ptr<PropertyIndex<node_or_edge>>> Make(
      PropertyGraph* pg, const std::string& column);

  std::string column() { return column_; }

  virtual iterator begin() = 0;
  virtual iterator end() = 0;

private:
  virtual Result<void> BuildFromProperty(
      std::shared_ptr<arrow::ChunkedArray> property) = 0;

  std::string column_;
};

// BasicPropertyIndex provides a PropertyIndex for arrow types whose C++
// equivalent is supported by std::less.
template <typename node_or_edge, typename key_type>
class KATANA_EXPORT BasicPropertyIndex : public PropertyIndex<node_or_edge> {
public:
  using ArrowArrayType = typename arrow::CTypeTraits<key_type>::ArrayType;
  using iterator = typename PropertyIndex<node_or_edge>::iterator;

  BasicPropertyIndex(
      const std::string& column, const std::shared_ptr<arrow::Array>& property)
      : PropertyIndex<node_or_edge>(column),
        set_(PropertyCompare{
            std::static_pointer_cast<ArrowArrayType>(property)}) {}

  iterator begin() override { return set_.begin(); }
  iterator end() override { return set_.end(); }

  iterator Find(key_type key) { return set_.find(key); }
  iterator LowerBound(key_type key) { return set_.lower_bound(key); }
  iterator UpperBound(key_type key) { return set_.upper_bound(key); }

private:
  struct PropertyCompare {
    bool operator()(node_or_edge a, node_or_edge b) const {
      // Compare the value of the indexed property for a and b using std::less.
      (void)a;
      (void)b;
      return true;
    }
    std::shared_ptr<ArrowArrayType> property;
  };

  virtual Result<void> BuildFromProperty(
      std::shared_ptr<arrow::ChunkedArray> property);

  std::set<node_or_edge, PropertyCompare> set_;
};

}  // namespace katana

#endif