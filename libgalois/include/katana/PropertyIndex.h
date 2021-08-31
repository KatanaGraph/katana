#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYINDEX_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYINDEX_H_

#include <set>
#include <string>
#include <variant>

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/type_traits.h>
#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include "katana/Result.h"
#include "katana/config.h"

namespace katana {

// PropertyIndex provides an interface similar to an ordered container
// over a single property.
template <typename node_or_edge>
class KATANA_EXPORT PropertyIndex {
public:
  // Type-safe container for node and edge ids to avoid overlap with the
  // primitive types we are indexing.
  struct IndexID {
    node_or_edge id;
  };

  // The set key type is either a node/edge id (all the keys in the actual set)
  // or a value representing the search key.
  using set_key_type = typename std::variant<
      IndexID, bool, uint8_t, int64_t, double_t, std::string_view*>;

  // PropertyIndex::iterator returns a sequence of node or edge ids.
  using base_iterator = typename std::multiset<set_key_type>::iterator;
  class iterator : public base_iterator {
  public:
    explicit iterator(const base_iterator& base) : base_iterator(base) {}
    iterator() : base_iterator(0) {}
    const node_or_edge& operator*() const {
      return std::get<IndexID>(base_iterator::operator*()).id;
    }
  };

  PropertyIndex(std::string column_name)
      : column_name_(std::move(column_name)) {}

  PropertyIndex(const PropertyIndex&) = delete;
  PropertyIndex& operator=(const PropertyIndex&) = delete;
  PropertyIndex(const PropertyIndex&&) = delete;
  PropertyIndex& operator=(const PropertyIndex&&) = delete;

  virtual ~PropertyIndex() = default;

  // The name of the indexed property.
  std::string column_name() { return column_name_; }

  virtual iterator begin() = 0;
  virtual iterator end() = 0;

  virtual Result<void> BuildFromProperty() = 0;
  // virtual Result<void> BuildFromFile() = 0;

private:
  std::string column_name_;
};

// PrimitivePropertyIndex provides a PropertyIndex for primitive types.
template <typename node_or_edge, typename c_type>
class KATANA_EXPORT PrimitivePropertyIndex
    : public PropertyIndex<node_or_edge> {
public:
  using ArrowArrayType = typename arrow::CTypeTraits<c_type>::ArrayType;
  using IndexID = typename PropertyIndex<node_or_edge>::IndexID;
  using iterator = typename PropertyIndex<node_or_edge>::iterator;
  using set_key_type = typename PropertyIndex<node_or_edge>::set_key_type;

  PrimitivePropertyIndex(
      const std::string& column, size_t num_entities,
      std::shared_ptr<arrow::Array> property)
      : PropertyIndex<node_or_edge>(column),
        num_entities_(num_entities),
        property_(std::static_pointer_cast<ArrowArrayType>(property)),
        set_(PropertyCompare(property_)) {}

  iterator begin() override { return iterator(set_.begin()); }
  iterator end() override { return iterator(set_.end()); }

  // Returns an iterator to the first element in the set with its property
  // value equal to `key`. The STL does not seem to specify that
  // multiset::find returns the first value, though it appears so in practice.
  iterator Find(c_type key) {
    auto it = set_.lower_bound(key);
    if (it == set_.end() ||
        property_->Value(std::get<IndexID>(*it).id) != key) {
      return end();
    }
    return iterator(it);
  }

  // Returns an iterator to the first element in the set that is greater than or
  // equal to `key`.
  iterator LowerBound(c_type key) { return iterator(set_.lower_bound(key)); }

  // Returns an iterator to the first element in the set that is greater than
  // `key`.
  iterator UpperBound(c_type key) { return iterator(set_.upper_bound(key)); }

private:
  class PropertyCompare {
  public:
    PropertyCompare(std::shared_ptr<ArrowArrayType> property)
        : property_(std::move(property)) {}

    bool operator()(const set_key_type& a, const set_key_type& b) const {
      // Each operand is either a literal key_type or a node/edge.
      c_type val_a = GetValue(a);
      c_type val_b = GetValue(b);
      // Compare the value of the indexed property for a and b using std::less.
      return std::less<c_type>{}(val_a, val_b);
    }

  private:
    c_type GetValue(const set_key_type& a) const {
      if (std::holds_alternative<IndexID>(a)) {
        node_or_edge id = std::get<IndexID>(a).id;
        c_type val = property_->Value(id);
        return val;
      }
      return std::get<c_type>(a);
    }

    std::shared_ptr<ArrowArrayType> property_;
  };

  Result<void> BuildFromProperty() override;
  // Result<void> BuildFromFile(...) override;

  size_t num_entities_;
  std::shared_ptr<ArrowArrayType> property_;
  std::multiset<set_key_type, PropertyCompare> set_;
};

// StringPropertyIndex provides a PropertyIndex for strings.
template <typename node_or_edge>
class KATANA_EXPORT StringPropertyIndex : public PropertyIndex<node_or_edge> {
public:
  using ArrowArrayType =
      typename arrow::TypeTraits<arrow::LargeStringType>::ArrayType;
  using IndexID = typename PropertyIndex<node_or_edge>::IndexID;
  using iterator = typename PropertyIndex<node_or_edge>::iterator;
  using set_key_type = typename PropertyIndex<node_or_edge>::set_key_type;

  StringPropertyIndex(
      const std::string& column_name, size_t num_entities,
      const std::shared_ptr<arrow::Array>& property)
      : PropertyIndex<node_or_edge>(column_name),
        num_entities_(num_entities),
        property_(std::static_pointer_cast<arrow::LargeStringArray>(property)),
        set_(StringCompare(property_)) {}

  iterator begin() override { return iterator(set_.begin()); }
  iterator end() override { return iterator(set_.end()); }

  // Returns an iterator to the first element in the set with its property
  // value equal to `key`. The STL does not appear to specify that
  // multiset::find returns the first value, though it appears so in practice.
  iterator Find(std::string_view key) {
    auto it = set_.lower_bound(&key);
    if (it != set_.end()) {
      arrow::util::string_view arrow_view =
          property_->GetView(std::get<IndexID>(*it).id);
      if (std::string_view(arrow_view.data(), arrow_view.length()) != key) {
        return end();
      }
    }
    return iterator(it);
  }

  // Returns an iterator to the first element in the set that is greater than or
  // equal to `key`.
  iterator LowerBound(std::string_view key) {
    return iterator(set_.lower_bound(&key));
  }

  // Returns an iterator to the first element in the set that is greater than
  // `key`.
  iterator UpperBound(std::string_view key) {
    return iterator(set_.upper_bound(&key));
  }

private:
  class StringCompare {
  public:
    StringCompare(std::shared_ptr<arrow::LargeStringArray> property)
        : property_(std::move(property)) {}

    bool operator()(const set_key_type& a, const set_key_type& b) const {
      // Each operand is either a pointer to a string_view or a node/edge.
      std::string_view val_a = GetValue(a);
      std::string_view val_b = GetValue(b);
      // Compare the value of the indexed property for a and b using std::less.
      return std::less<std::string_view>{}(val_a, val_b);
    }

  private:
    std::string_view GetValue(const set_key_type& a) const {
      if (std::holds_alternative<IndexID>(a)) {
        arrow::util::string_view arrow_view =
            property_->GetView(std::get<IndexID>(a).id);
        return std::string_view(arrow_view.data(), arrow_view.length());
      }
      return *std::get<std::string_view*>(a);
    }

    std::shared_ptr<arrow::LargeStringArray> property_;
  };

  Result<void> BuildFromProperty() override;
  // virtual Result<void> BuildFromFile(...) override;

  size_t num_entities_;
  std::shared_ptr<arrow::LargeStringArray> property_;
  std::multiset<set_key_type, StringCompare> set_;
};  // namespace katana

// Create a PropertyIndex with the apropriate type for 'property'. Does not
// build the index.
template <typename node_or_edge>
Result<std::unique_ptr<PropertyIndex<node_or_edge>>> MakeTypedIndex(
    const std::string& column_name, size_t num_entities,
    std::shared_ptr<arrow::Array> property);

}  // namespace katana

#endif
