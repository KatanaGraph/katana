#pragma once

#include <bitset>
#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/array/array_primitive.h>
#include <arrow/table.h>

#include "katana/DynamicBitsetSlow.h"
#include "katana/Logging.h"
#include "katana/Result.h"

namespace katana {

/// EntityTypeID uniquely identifies an entity (node or edge) type
/// EntityTypeID for nodes is distinct from EntityTypeID for edges
/// This type may either be an atomic type or an intersection of atomic types
/// EntityTypeID is represented using 8 bits
using EntityTypeID = uint16_t;
static constexpr EntityTypeID kUnknownEntityType = EntityTypeID{0};
static constexpr EntityTypeID kInvalidEntityType =
    std::numeric_limits<EntityTypeID>::max();

/// The minimum size of the dynamically sized SetOfEntityTypeIDs
static constexpr size_t kDefaultSetOfEntityTypeIDsSize = 256;
/// The maximum size of the dynamically sized SetOfEntityTypeIDs
static constexpr size_t kMaxSetOfEntityTypeIDsSize = kInvalidEntityType + 1;

/// A dynamically sized set of EntityTypeIDs
using SetOfEntityTypeIDs = DynamicBitsetSlow;
//TODO(emcginnis): use DynamicBitset when it is available to libsupport
/// A map from EntityTypeID to a set of EntityTypeIDs
using EntityTypeIDToSetOfEntityTypeIDsMap = std::vector<SetOfEntityTypeIDs>;
/// A map from the atomic type name to its EntityTypeID
/// (that does not intersect any other atomic type)
using AtomicTypeNameToEntityTypeIDMap =
    std::unordered_map<std::string, EntityTypeID>;
/// A map from the atomic type's EntityTypeID to its name
/// (that does not intersect any other atomic type)
using EntityTypeIDToAtomicTypeNameMap =
    std::unordered_map<EntityTypeID, std::string>;
using TypeNameSet = std::set<std::string>;

class KATANA_EXPORT EntityTypeManager {
  // TODO (scober): add iterator over all types
  // TODO (scober): add iterator over all atomic types
  // TODO (scober): add convenient iteration over SetOfEntityTypeIDs
public:
  EntityTypeManager() { Init(); }

  EntityTypeManager(
      EntityTypeIDToAtomicTypeNameMap&& atomic_entity_type_id_to_type_name,
      EntityTypeIDToSetOfEntityTypeIDsMap&&
          entity_type_id_to_atomic_entity_type_ids)
      : atomic_entity_type_id_to_type_name_(
            std::move(atomic_entity_type_id_to_type_name)),
        entity_type_id_to_atomic_entity_type_ids_(
            std::move(entity_type_id_to_atomic_entity_type_ids)) {
    // Ensure kUnknownEntityType is not considered an AtomicEntityType
    KATANA_LOG_ASSERT(
        !entity_type_id_to_atomic_entity_type_ids_.at(kUnknownEntityType)
             .test(kUnknownEntityType));
    KATANA_LOG_ASSERT(
        atomic_entity_type_id_to_type_name_.find(kUnknownEntityType) ==
        atomic_entity_type_id_to_type_name_.end());

    for (auto& type_id_name_pair : atomic_entity_type_id_to_type_name_) {
      atomic_type_name_to_entity_type_id_.emplace(
          type_id_name_pair.second, type_id_name_pair.first);
    }

    // construct the atomic_entity_type_id_to_entity_type_ids map
    size_t num_entity_types = entity_type_id_to_atomic_entity_type_ids_.size();
    atomic_entity_type_id_to_entity_type_ids_.resize(num_entity_types);

    // Max EntityTypeID is 1 less than the number of entity type ids
    size_t set_size = CalculateSetOfEntityTypeIDsSize(num_entity_types - 1);

    for (size_t i = 0; i < num_entity_types; i++) {
      atomic_entity_type_id_to_entity_type_ids_.at(i).resize(set_size);
    }

    for (size_t i = 0, ni = num_entity_types; i < ni; ++i) {
      for (size_t j = 0, nj = num_entity_types; j < nj; ++j) {
        if (entity_type_id_to_atomic_entity_type_ids_.at(i).test(j)) {
          atomic_entity_type_id_to_entity_type_ids_.at(j).set(i);
        }
      }
    }

    // ensure the passed in sets are the correct size
    for (size_t i = 0; i < num_entity_types; i++) {
      entity_type_id_to_atomic_entity_type_ids_.at(i).resize(set_size);
    }
  }

  EntityTypeManager(
      EntityTypeIDToAtomicTypeNameMap&& atomic_entity_type_id_to_type_name,
      AtomicTypeNameToEntityTypeIDMap&& atomic_type_name_to_entity_type_id,
      EntityTypeIDToSetOfEntityTypeIDsMap&&
          entity_type_id_to_atomic_entity_type_ids,
      EntityTypeIDToSetOfEntityTypeIDsMap&&
          atomic_entity_type_id_to_entity_type_ids)
      : atomic_entity_type_id_to_type_name_(
            std::move(atomic_entity_type_id_to_type_name)),
        atomic_type_name_to_entity_type_id_(
            std::move(atomic_type_name_to_entity_type_id)),
        entity_type_id_to_atomic_entity_type_ids_(
            std::move(entity_type_id_to_atomic_entity_type_ids)),
        atomic_entity_type_id_to_entity_type_ids_(
            std::move(atomic_entity_type_id_to_entity_type_ids)) {
    // Ensure kUnknownEntityType is not considered an AtomicEntityType
    KATANA_LOG_ASSERT(
        !entity_type_id_to_atomic_entity_type_ids_.at(kUnknownEntityType)
             .test(kUnknownEntityType));
    KATANA_LOG_ASSERT(
        atomic_entity_type_id_to_type_name_.find(kUnknownEntityType) ==
        atomic_entity_type_id_to_type_name_.end());

    //Must ensure all sets are at least big enough to fit all EntityTypeIDs
    size_t num_entity_types = entity_type_id_to_atomic_entity_type_ids_.size();
    ResizeSetOfEntityTypeIDsMaps(num_entity_types - 1);
  }

  /// This function can be used to convert "old style" graphs (storage format 1,
  /// where types are represented by uint8 properties) and "new style"
  /// graphs (version > 2, where types are represented in our native type
  /// represenation). This function is serial but it likely iterates over
  /// O(nodes) and O(edges) vectors, so it is very slow. It should only be used
  /// for updating old graphs.
  ///
  /// The length of entity_type_ids should be equal to topo_size.
  /// properties->num_rows() should be equal to the length of entity_type_ids or
  /// 0.
  ///
  /// It returns a list of the properties used for types so that they can be
  /// removed as properties.
  template <typename EntityTypeArray>
  static Result<std::vector<std::string>> AssignEntityTypeIDsFromProperties(
      const size_t topo_size,  // == either num_nodes() or  num_edges()
      const std::shared_ptr<arrow::Table>& properties,
      katana::EntityTypeManager* entity_type_manager,
      EntityTypeArray* entity_type_ids) {
    KATANA_LOG_WARN(
        "assigning entity type ids from properties with {} properties loaded",
        properties->num_columns());
    KATANA_LOG_WARN(
        "store the RDG to avoid overhead from assigning entity type ids from "
        "properties in the future");
    static_assert(
        std::is_same_v<typename EntityTypeArray::value_type, EntityTypeID>);
    if (entity_type_ids->size() != topo_size) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "size of topology array ({}) doesn't match size of type array ({})",
          topo_size, entity_type_ids->size());
    }
    int64_t num_rows = properties->num_rows();
    if (num_rows == 0) {
      std::fill(
          entity_type_ids->begin(), entity_type_ids->end(), kUnknownEntityType);
      return std::vector<std::string>();
    }
    auto num_rows_for_comparison = static_cast<size_t>(num_rows);
    if (entity_type_ids->size() != num_rows_for_comparison) {
      return KATANA_ERROR(
          katana::ErrorCode::InvalidArgument,
          "size of property table ({}) doesn't match size of type array ({})",
          num_rows, entity_type_ids->size());
    }

    // We cannot use KATANA_CHECKED here because nvcc cannot handle it.
    auto res =
        DoAssignEntityTypeIDsFromProperties(properties, entity_type_manager);
    if (!res) {
      return katana::Result<std::vector<std::string>>(std::move(res.error()));
    }
    TypeProperties type_properties = std::move(res.value());

    // assign the type ID for each row
    for (int64_t row = 0; row < num_rows; ++row) {
      TypeProperties::FieldEntity field_indices;
      for (auto& uint8_property : type_properties.uint8_properties) {
        if (uint8_property.array->IsValid(row) &&
            uint8_property.array->Value(row)) {
          field_indices.emplace_back(uint8_property.field_index);
        }
      }
      if (field_indices.empty()) {
        entity_type_ids->at(row) = katana::kUnknownEntityType;
      } else {
        katana::EntityTypeID entity_type_id =
            type_properties.type_field_indices_to_id.at(field_indices);
        entity_type_ids->at(row) = entity_type_id;
      }
    }

    std::vector<std::string> properties_used;
    for (const auto& prop_col : type_properties.uint8_properties) {
      properties_used.emplace_back(
          properties->field(prop_col.field_index)->name());
    }

    return properties_used;
  }

  /// adds a new entity type for the atomic type with name \p name
  ///
  /// this function is required to be deterministic because it adds new entity
  /// type ids
  ///
  /// \returns the EntityTypeID for the new type
  Result<EntityTypeID> AddAtomicEntityType(const std::string& name);

  /// Get the intersection of the types named in \p names; or add the type if
  /// it does not already exist. If any types named in \p names do not exist,
  /// create them.
  ///
  /// this function is required to be deterministic because it adds new entity
  /// type ids
  ///
  /// \returns the EntityTypeID of the intersection type.
  ///
  /// \see GetOrAddNonAtomicEntityType(const SetOfEntityTypeIDs&)
  template <typename Container>
  Result<EntityTypeID> GetOrAddNonAtomicEntityTypeFromStrings(
      const Container& names) {
    // We cannot use KATANA_CHECKED here because nvcc cannot handle it.
    auto res = GetOrAddEntityTypeIDs(names);
    if (!res) {
      return res.error();
    }
    return GetOrAddNonAtomicEntityType(res.assume_value());
  }

  /// Get the intersection of the types named in \p names
  ///
  /// \returns the EntityTypeID of the intersection type.
  template <typename Container>
  Result<EntityTypeID> GetNonAtomicEntityTypeFromStrings(
      const Container& names) const {
    // We cannot use KATANA_CHECKED here because nvcc cannot handle it.
    auto res = GetEntityTypeIDs(names);
    if (!res) {
      return res.error();
    }
    return GetNonAtomicEntityType(res.assume_value());
  }

  /// Get the intersection of the types passed in; or add the type if it does
  /// not already exist.
  ///
  /// this function is required to be deterministic because it adds new entity
  /// type ids
  ///
  /// \warning This operation is currently `O(number of types)` due to a linear
  ///     search. This can be fixed with a space--time trade-off if needed.
  ///
  /// \returns the EntityTypeID of the intersection type.
  Result<EntityTypeID> GetOrAddNonAtomicEntityType(
      const SetOfEntityTypeIDs& type_id_set);

  /// Get the intersection of the types passed in.
  ///
  /// \warning This operation is currently `O(number of types)` due to a linear
  ///     search. This can be fixed with a space--time trade-off if needed.
  ///
  /// \returns the EntityTypeID of the intersection type.
  Result<EntityTypeID> GetNonAtomicEntityType(
      const SetOfEntityTypeIDs& type_id_set) const;

  /// Get the intersection of the types passed in.
  ///
  /// this function is required to be deterministic because it adds new entity
  /// type ids
  ///
  /// \warning This function does not do proper error checking. Only use if you
  ///     can prove the intersection type does not already exist. Otherwise, use
  ///     GetOrAddNonAtomicEntityType(const SetOfEntityTypeIDs& type_id_set).
  ///
  /// \returns the EntityTypeID of the intersection type.
  Result<EntityTypeID> AddNonAtomicEntityType(
      const SetOfEntityTypeIDs& type_id_set);

  /// \returns the number of atomic types
  size_t GetNumAtomicTypes() const {
    return atomic_entity_type_id_to_type_name_.size();
  }

  /// \returns the number of entity types (including kUnknownEntityType)
  size_t GetNumEntityTypes() const {
    return entity_type_id_to_atomic_entity_type_ids_.size();
  }

  /// \returns true iff an atomic type \p name exists
  bool HasAtomicType(const std::string& name) const {
    return atomic_type_name_to_entity_type_id_.count(name) == 1;
  }

  std::vector<std::string> ListAtomicTypes() const {
    std::vector<std::string> types;
    // TODO(aneesh) define an iterator-type alias and return an iterator over
    // the names instead of constructing a vector.
    for (const auto& kv : atomic_type_name_to_entity_type_id_) {
      types.push_back(kv.first);
    }
    return types;
  }

  /// \returns true iff an entity type \p entity_type_id exists
  /// (returns true for kUnknownEntityType)
  bool HasEntityType(EntityTypeID entity_type_id) const {
    return entity_type_id < entity_type_id_to_atomic_entity_type_ids_.size();
  }

  /// \returns the EntityTypeID for an atomic type with name \p name
  /// (assumes that the type exists)
  EntityTypeID GetEntityTypeID(const std::string& name) const {
    return atomic_type_name_to_entity_type_id_.at(name);
  }

  /// this function is required to be deterministic because it adds new entity
  /// type ids
  ///
  /// \returns the EntityTypeID for an atomic type with name \p name, adding it
  /// if it doesn't exist.
  Result<EntityTypeID> GetOrAddEntityTypeID(const std::string& name);

  Result<katana::TypeNameSet> EntityTypeToTypeNameSet(
      katana::EntityTypeID type_id) const;

  /// \returns the EntityTypeIDs for atomic types with \p names, or an error if
  /// any does not exist.
  template <typename Container>
  Result<SetOfEntityTypeIDs> GetEntityTypeIDs(const Container& names) const {
    SetOfEntityTypeIDs res;
    res.resize(SetOfEntityTypeIDsSize_);
    for (const auto& name : names) {
      if (HasAtomicType(name)) {
        EntityTypeID id = GetEntityTypeID(name);
        if (res.test(id)) {
          return KATANA_ERROR(
              ErrorCode::InvalidArgument, "duplicate name: {}", name);
        }
        res.set(id);
      } else {
        return KATANA_ERROR(
            ErrorCode::NotFound, "type {} does not exist", name);
      }
    }
    return MakeResult(std::move(res));
  }

  /// this function is required to be deterministic because it adds new entity
  /// type ids
  ///
  /// \returns the EntityTypeIDs for atomic types with \p names, adding them if
  /// needed.
  template <typename Container>
  Result<SetOfEntityTypeIDs> GetOrAddEntityTypeIDs(const Container& names) {
    SetOfEntityTypeIDs res;
    res.resize(SetOfEntityTypeIDsSize_);

    for (const auto& name : names) {
      auto id_res = GetOrAddEntityTypeID(name);
      // We cannot use KATANA_CHECKED here because nvcc cannot handle it.
      if (!id_res) {
        return id_res.error();
      }
      auto id = id_res.value();

      // Ensure our return set has enough room if we did add a new EntityTypeID
      // if there already is, this is quick
      res.resize(SetOfEntityTypeIDsSize_);

      if (res.test(id)) {
        return KATANA_ERROR(
            ErrorCode::InvalidArgument, "duplicate name: {}, id = {}", name,
            id);
      }
      res.set(id);
    }

    return MakeResult(std::move(res));
  }

  /// \returns the name of the atomic type if the EntityTypeID
  /// \p entity_type_id is an atomic type, nullopt otherwise
  std::optional<std::string> GetAtomicTypeName(
      EntityTypeID entity_type_id) const {
    auto found = atomic_entity_type_id_to_type_name_.find(entity_type_id);
    if (found != atomic_entity_type_id_to_type_name_.cend()) {
      return found->second;
    }
    return std::nullopt;
  }

  /// \returns a vector containing all atomic type names
  std::vector<EntityTypeID> GetAtomicEntityTypeIDs() const {
    std::vector<EntityTypeID> type_vec;
    type_vec.reserve(atomic_type_name_to_entity_type_id_.size());
    for (const auto& entry : atomic_type_name_to_entity_type_id_) {
      type_vec.push_back(entry.second);
    }
    return type_vec;
  }

  /// \returns the set of entity types that intersect
  /// the atomic type \p entity_type_id
  /// (assumes that the atomic type exists)
  const SetOfEntityTypeIDs& GetSupertypes(EntityTypeID entity_type_id) const {
    return atomic_entity_type_id_to_entity_type_ids_.at(entity_type_id);
  }

  /// \returns the set of atomic types that are intersected
  /// by the entity type \p entity_type_id
  /// (assumes that the entity type exists)
  const SetOfEntityTypeIDs& GetAtomicSubtypes(
      EntityTypeID entity_type_id) const {
    return entity_type_id_to_atomic_entity_type_ids_.at(entity_type_id);
  }

  /// \returns true iff the type \p sub_type is a
  /// sub-type of the type \p super_type
  /// (assumes that the sub_type and super_type EntityTypeIDs exists)
  bool IsSubtypeOf(EntityTypeID sub_type, EntityTypeID super_type) const {
    const auto& super_atomic_types = GetAtomicSubtypes(super_type);
    const auto& sub_atomic_types = GetAtomicSubtypes(sub_type);
    // return true if sub_atomic_types is a subset of super_atomic_types
    SetOfEntityTypeIDs res;
    res.resize(SetOfEntityTypeIDsSize_);
    res.bitwise_and(sub_atomic_types, super_atomic_types);
    return (res == sub_atomic_types);
  }

  const EntityTypeIDToSetOfEntityTypeIDsMap&
  GetEntityTypeIDToAtomicEntityTypeIDs() const {
    return entity_type_id_to_atomic_entity_type_ids_;
  }

  const EntityTypeIDToAtomicTypeNameMap& GetEntityTypeIDToAtomicTypeNameMap()
      const {
    return atomic_entity_type_id_to_type_name_;
  }

  /// Returns the current size of the SetOFEntityTypeIDs bitsets
  size_t SetOfEntityTypeIDsSize() const { return SetOfEntityTypeIDsSize_; }

  /// Calculate the SetOfEntityTypeIDs size required to fix max_id number of EntityTypeIDs
  /// Optimally, we would only ever resize to exactly max_id
  /// but this would be extremely inefficient in cases where we have thousands of EntityTypeIDs
  /// and are still adding more as we would have to resize every bitset for each new EntityTypeID
  /// Must keep resizing infrequent and deterministic;
  static size_t CalculateSetOfEntityTypeIDsSize(EntityTypeID max_id);

  /// bool Equals() IS A TESTING ONLY FUNCTION, DO NOT EXPOSE THIS TO THE USER
  bool Equals(const EntityTypeManager& other) const;

  /// std::string ReportDiff() IS A TESTING ONLY FUNCTION, DO NOT EXPOSE THIS TO THE USER
  std::string ReportDiff(const EntityTypeManager& other) const;

  /// ToString() IS A TESTING ONLY FUNCTION, DO NOT EXPOSE THIS TO THE USER
  std::string ToString() const;

  /// std::string PrintTypes() IS A TESTING ONLY FUNCTION, DO NOT EXPOSE THIS TO THE USER
  std::string PrintEntityTypes() const;

private:
  // Used by AssignEntityTypeIDsFromProperties()
  template <typename ArrowType>
  struct PropertyColumn {
    int field_index;
    std::shared_ptr<ArrowType> array;

    PropertyColumn(int i, std::shared_ptr<ArrowType>& a)
        : field_index(i), array(a) {}
  };

  // Used by AssignEntityTypeIDsFromProperties()
  struct TypeProperties {
    std::vector<PropertyColumn<arrow::UInt8Array>> uint8_properties;
    using FieldEntity = std::vector<int>;
    std::map<FieldEntity, katana::EntityTypeID> type_field_indices_to_id;
  };

  static Result<TypeProperties> DoAssignEntityTypeIDsFromProperties(
      const std::shared_ptr<arrow::Table>& properties,
      EntityTypeManager* entity_type_manager);

  void Init() {
    // assume kUnknownEntityType is 0
    static_assert(kUnknownEntityType == 0);
    // add kUnknownEntityType: do not treat it as an atomic type;
    // treat it as an entity type that does not have any atomic subtypes
    SetOfEntityTypeIDs empty_type_id_set;
    empty_type_id_set.resize(kDefaultSetOfEntityTypeIDsSize);
    auto id = AddNonAtomicEntityType(empty_type_id_set);
    KATANA_LOG_ASSERT(id.value() == kUnknownEntityType);
  }

  /// The current size of the SetEntityTypeIDs bitsets
  size_t SetOfEntityTypeIDsSize_ = kDefaultSetOfEntityTypeIDsSize;

  /// Resize the SetEntityTypeIDs bitmaps to fit the new_entity_type_id
  void ResizeSetOfEntityTypeIDsMaps(EntityTypeID new_entity_type_id);

  /// A map from the EntityTypeID to its type name if it is an atomic type
  /// (that does not intersect any other atomic type)
  EntityTypeIDToAtomicTypeNameMap atomic_entity_type_id_to_type_name_;

  /// A map from the atomic type name to its EntityTypeID
  /// (that does not intersect any other atomic type):
  /// derived from atomic_entity_type_id_to_type_name_
  AtomicTypeNameToEntityTypeIDMap atomic_type_name_to_entity_type_id_;

  /// A map from the EntityTypeID to its sub-atomic-types
  /// (the set of atomic entity type IDs it intersects)
  EntityTypeIDToSetOfEntityTypeIDsMap entity_type_id_to_atomic_entity_type_ids_;

  /// A map from the atomic EntityTypeID to its super-types
  /// (to the set of the EntityTypeIDs that intersect it):
  /// derived from entity_type_id_to_atomic_entity_type_ids_
  /// By definition, an atomic EntityTypeID intersects with itself
  /// So the intersection set of an atomic EntityTypeID will contain itself
  /// The intersection set of a non-atomic EntityTypeID will *not* contain itself
  /// ex: atomic_entity_type_id_to_entity_type_ids_[atomic_id][atomic_id] == 1
  /// but atomic_entity_type_id_to_entity_type_ids_[non_atomic_id][non_atomic_id] == 0
  EntityTypeIDToSetOfEntityTypeIDsMap atomic_entity_type_id_to_entity_type_ids_;
};

}  // namespace katana

// Allow TypeNameSet foo; fmt::print("{}\n", foo);
template <>
struct KATANA_EXPORT fmt::formatter<katana::TypeNameSet>
    : formatter<std::string> {
  template <typename FormatContext>
  auto format(const katana::TypeNameSet& tns, FormatContext& ctx) {
    // Use Cypher syntax
    return format_to(ctx.out(), "{}", fmt::join(tns, ":"));
  }
};
