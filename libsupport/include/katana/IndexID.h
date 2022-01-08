#ifndef KATANA_LIBSUPPORT_KATANA_INDEXID_H_
#define KATANA_LIBSUPPORT_KATANA_INDEXID_H_

#include "katana/OpaqueID.h"

namespace katana {

/// A numeric ID type suitable for indexing. IndexIDs support:
///
/// - Addition and subtraction of other IDs (the result of which is an ID)
/// - Increment and decrement
/// - Dereference
///
/// NB: IndexID is less safe in general than `OpaqueID` and `OpaqueLinearID`.
///     Use those stricter variants whenever possible.
template <typename _IDType, typename _Value>
struct IndexID : public OpaqueIDOrderedWithValue<_IDType, _Value> {
public:
  using OpaqueIDOrderedWithValue<_IDType, _Value>::OpaqueIDOrderedWithValue;

  // iterator traits
  using difference_type = _IDType;
  using value_type = _IDType;
  using pointer = _IDType*;
  using reference = _IDType&;
  using iterator_category = std::random_access_iterator_tag;

  // must provide a dereference to be an iterator
  reference operator*() { return *static_cast<_IDType*>(this); }

  _IDType& operator++() {
    ++this->value_;
    return *static_cast<_IDType*>(this);
  }

  _IDType& operator--() {
    --this->value_;
    return *static_cast<_IDType*>(this);
  }

  _IDType operator++(int) {
    _IDType ret{*static_cast<_IDType*>(this)};
    this->value_++;
    return ret;
  }

  _IDType operator--(int) {
    _IDType ret{*static_cast<_IDType*>(this)};
    this->value_--;
    return ret;
  }

  _IDType& operator+=(const _IDType& rhs) { return *this += rhs.value(); }

  _IDType& operator+=(_Value val) {
    this->value_ += val;
    return *static_cast<_IDType*>(this);
  }

  _IDType& operator-=(const _IDType& rhs) { return *this -= rhs.value(); }

  _IDType& operator-=(_Value val) {
    this->value_ -= val;
    return *static_cast<_IDType*>(this);
  }

  _IDType operator+(const _IDType& rhs) const { return *this + rhs.value(); }

  _IDType operator+(_Value val) const { return _IDType(this->value() + val); }

  _IDType operator-(const _IDType& rhs) const { return *this - rhs.value(); }

  _IDType operator-(_Value val) const { return _IDType(this->value() - val); }
};

}  // namespace katana

#endif
