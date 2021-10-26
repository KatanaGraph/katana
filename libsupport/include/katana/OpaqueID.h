#pragma once

#include <cstdint>
#include <iostream>

#include <boost/math/tools/precision.hpp>
#include <fmt/format.h>

namespace katana {

// The count_traits are here to support the Count type within OpaqueIDs
// Default count type is size_t
template <class T, class = void>
struct count_traits {
  using Count = size_t;
};

// If value type is integral, use the unsigned version of it for Count
template <class T>
struct count_traits<T, typename std::enable_if_t<std::is_integral_v<T>>> {
  using Count = std::make_unsigned_t<T>;
};

/// Base class for opaque ID types.
///
/// Opaque ID types are:
///
/// - copyable, assignable, swappable, movable
/// - explicitly convertible to and from their value type
/// - insertable into ostream (using the behavior of the value type.
/// - equality comparable
/// - hashable and implement std::less to allow use as keys in maps.
///
/// Subclasses should be used as IDs for objects such as nodes and edges. This
/// avoids the potential accidentally use a node ID as an edge ID for instance.
///
/// For example,
///
/// <code>
/// struct T : public katana::OpaqueID<T, int> {
///   using OpaqueID::OpaqueID;
/// };
///
/// template <> struct std::hash<T> : public katana::OpaqueIDHashable<T> {};
/// template <> struct std::less<T> : public katana::OpaqueIDLess<T> {};
/// </code>
///
/// This defines a new type T which is stored as an int, but is distinct from
/// integers and does not allow arithmetic, or any other operations not listed
/// above. The next two lines provide hash and less specializations to enable T
/// to be used as key in unordered and order maps respectively. If ad-hoc order
/// comparison of T (e.g., a < b) is meaningful use OpaqueIDOrdered instead of
/// OpaqueID.
///
/// \tparam _IDType The OpaqueID subclass (or another type to use a tag)
/// \tparam _Value The underlying value type to store the ID
template <typename _IDType, typename _Value>
struct OpaqueID {
  // TODO(amp): We need some check that _IDType is a subtype of
  //  OpaqueID<_IDType, _Value>. There doesn't seem to be any way to do that.
  using ValueType = _Value;
  using IDType = _IDType;

  using Count = typename count_traits<_Value>::Count;

protected:
  ValueType value_;

public:
  OpaqueID() noexcept = default;
  OpaqueID(const OpaqueID& other) noexcept = default;
  OpaqueID(OpaqueID&& other) noexcept = default;

  ~OpaqueID() noexcept = default;

  OpaqueID& operator=(const OpaqueID& other) noexcept = default;
  OpaqueID& operator=(OpaqueID&& other) noexcept = default;

  constexpr bool operator==(const _IDType& other) const noexcept {
    return value_ == other.value_;
  }
  constexpr bool operator!=(const _IDType& other) const noexcept {
    return value_ != other.value_;
  }

  constexpr ValueType value() const noexcept { return value_; }

  constexpr explicit OpaqueID(ValueType value) : value_(value) {}

  explicit operator ValueType&() noexcept { return value_; }
  explicit operator const ValueType&() const noexcept { return value_; }

  friend void swap(_IDType& a, _IDType& b) noexcept {
    std::swap(a.value_, b.value_);
  }

  friend std::ostream& operator<<(std::ostream& os, const _IDType& self) {
    return os << self.value();
  }

  friend std::size_t hash_value(const _IDType& self) {
    return std::hash<ValueType>{}(self.value());
  }
};

// This function has a non-standard name type to act as a better error message
// in case of template match failure.
template <typename T, typename _Value>
_Value
must_be_OpaqueID_subclass(const OpaqueID<T, _Value>&) {}

/// Return the value type of an OpaqueID subclass.
///
/// \see OpaqueID
template <typename OpaqueIDType>
using opaque_id_value_type =
    decltype(must_be_OpaqueID_subclass(std::declval<OpaqueIDType>()));

/// An ordered ID.
///
/// As OpaqueID, except that it also implements order comparison operators:
/// <=, <, >=, >.
///
/// \see OpaqueID
template <typename _IDType, typename _Value>
struct OpaqueIDOrdered : public OpaqueID<_IDType, _Value> {
  using OpaqueID<_IDType, _Value>::OpaqueID;

  constexpr bool operator<=(const _IDType& other) const {
    return this->value() <= other.value();
  }
  constexpr bool operator<(const _IDType& other) const {
    return this->value() < other.value();
  }
  constexpr bool operator>=(const _IDType& other) const {
    return this->value() >= other.value();
  }
  constexpr bool operator>(const _IDType& other) const {
    return this->value() > other.value();
  }
};

/// Allow order comparison of IDs with their value type.
template <typename _IDType, typename _Value>
struct OpaqueIDOrderedWithValue : public OpaqueIDOrdered<_IDType, _Value> {
  using OpaqueIDOrdered<_IDType, _Value>::OpaqueIDOrdered;

  using OpaqueIDOrdered<_IDType, _Value>::operator==;
  using OpaqueIDOrdered<_IDType, _Value>::operator!=;

  constexpr bool operator==(const _Value& other) const {
    return this->value() == other;
  }
  constexpr bool operator!=(const _Value& other) const {
    return this->value() != other;
  }

  using OpaqueIDOrdered<_IDType, _Value>::operator<=;
  using OpaqueIDOrdered<_IDType, _Value>::operator<;
  using OpaqueIDOrdered<_IDType, _Value>::operator>=;
  using OpaqueIDOrdered<_IDType, _Value>::operator>;

  constexpr bool operator<=(const _Value& other) const {
    return this->value() <= other;
  }
  constexpr bool operator<(const _Value& other) const {
    return this->value() < other;
  }
  constexpr bool operator>=(const _Value& other) const {
    return this->value() >= other;
  }
  constexpr bool operator>(const _Value& other) const {
    return this->value() > other;
  }

  friend constexpr bool operator<=(const _Value& other, const _IDType& self) {
    return other <= self.value();
  }
  friend constexpr bool operator<(const _Value& other, const _IDType& self) {
    return other < self.value();
  }
  friend constexpr bool operator>=(const _Value& other, const _IDType& self) {
    return other >= self.value();
  }
  friend constexpr bool operator>(const _Value& other, const _IDType& self) {
    return other > self.value();
  }
};

/// A linear ID. Linear IDs support:
///
/// - Addition and subtraction of DifferenceType (which is a signed version of
///   ValueType)
/// - Increment and decrement
/// - Subtraction of two IDs to get a DifferenceType
/// - Dereference
template <typename _IDType, typename _Value>
struct OpaqueIDLinear : public OpaqueIDOrderedWithValue<_IDType, _Value> {
public:
  using OpaqueIDOrderedWithValue<_IDType, _Value>::OpaqueIDOrderedWithValue;

  static_assert(
      sizeof(_Value) <= sizeof(std::ptrdiff_t),
      "OpaqueIDLinear only supports values up to the size of ptrdiff_t.");

  using DifferenceType = std::ptrdiff_t;

  // iterator traits
  using difference_type = DifferenceType;
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

  _IDType& operator+=(const DifferenceType& rhs) {
    this->value_ += rhs;
    return *static_cast<_IDType*>(this);
  }

  _IDType& operator-=(const DifferenceType& rhs) {
    this->value_ -= rhs;
    return *static_cast<_IDType*>(this);
  }

  _IDType operator+(DifferenceType v) const {
    return _IDType(this->value() + v);
  }

  _IDType operator-(DifferenceType v) const {
    return _IDType(this->value() - v);
  }

  /// The difference between two IDs represented as the signed variant of
  /// ValueType.
  DifferenceType operator-(_IDType v) const {
    return DifferenceType(this->value()) - DifferenceType(v.value());
  }

  /// If your _Value is not a language-provided numeric type
  /// you should specialize std::numeric_limits<_Value>, specialize this
  /// sentinel function, or specialize boost::math::tools::max_value.
  /// Otherwise this won't compile.
  static constexpr _IDType sentinel() {
    return _IDType(boost::math::tools::max_value<_Value>());
  }
};

/// Provide hashing for IDs to allow them to be used in unordered maps.
///
/// \see OpaqueID
template <typename T>
struct OpaqueIDHashable : private std::hash<opaque_id_value_type<T>> {
  size_t operator()(const T& v) const noexcept {
    return std::hash<opaque_id_value_type<T>>::operator()(v.value());
  }
};

/// Provide ordering for "unordered" IDs to allow them to be used in
/// ordered maps. These types do not support comparison operators, to prevent
/// users from comparing them without thinking about it.
///
/// \see OpaqueID
template <typename T>
struct OpaqueIDLess : private std::less<opaque_id_value_type<T>> {
  bool operator()(const T& x, const T& y) const noexcept {
    return std::less<opaque_id_value_type<T>>::operator()(x.value(), y.value());
  }
};

}  // namespace katana

template <typename T, typename Char>
struct fmt::formatter<
    T, Char,
    std::enable_if_t<std::is_convertible<
        T*,
        katana::OpaqueID<typename T::IDType, typename T::ValueType>*>::value>>
    : formatter<std::string> {
  template <typename FormatContext>
  auto format(const T& id, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", id.value());
  }
};
