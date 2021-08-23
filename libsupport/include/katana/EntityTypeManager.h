#pragma once

#include <bitset>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <arrow/array/array_primitive.h>
#include <arrow/table.h>

#include "katana/Logging.h"
#include "katana/Result.h"

namespace katana {

/// EntityTypeID uniquely identifies an entity (node or edge) type
/// EntityTypeID for nodes is distinct from EntityTypeID for edges
/// This type may either be an atomic type or an intersection of atomic types
/// EntityTypeID is represented using 8 bits
using EntityTypeID = uint8_t;
static constexpr EntityTypeID kUnknownEntityType = EntityTypeID{0};
static constexpr std::string_view kUnknownEntityTypeName = "kUnknownName";
static constexpr EntityTypeID kInvalidEntityType =
    std::numeric_limits<EntityTypeID>::max();
/// A set of EntityTypeIDs
using SetOfEntityTypeIDs =
    std::bitset<std::numeric_limits<EntityTypeID>::max() + 1>;
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

class EntityTypeManager {
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
    for (auto& type_id_name_pair : atomic_entity_type_id_to_type_name_) {
      atomic_type_name_to_entity_type_id_.emplace(
          std::make_pair(type_id_name_pair.second, type_id_name_pair.first));
    }

    size_t num_entity_types = entity_type_id_to_atomic_entity_type_ids_.size();
    atomic_entity_type_id_to_entity_type_ids_.resize(num_entity_types);
    for (size_t i = 0, ni = num_entity_types; i < ni; ++i) {
      for (size_t j = 0, nj = num_entity_types; j < nj; ++j) {
        if (entity_type_id_to_atomic_entity_type_ids_[i].test(j)) {
          atomic_entity_type_id_to_entity_type_ids_[j].set(i);
        }
      }
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
            std::move(atomic_entity_type_id_to_entity_type_ids)) {}

  template <typename ArrowType>
  struct PropertyColumn {
    int field_index;
    std::shared_ptr<ArrowType> array;

    PropertyColumn(int i, std::shared_ptr<ArrowType>& a)
        : field_index(i), array(a) {}
  };

  /// This function can be used to convert "old style" graphs (storage format 1,
  /// where types are represented by bool or uint8 properties) and "new style"
  /// graphs (version > 2, where types are represented in our native type
  /// represenation). This function is serial but it likely iterates over
  /// O(nodes) and O(edges) vectors, so it is very slow. It should only be used
  /// for updating old graphs.
  ///
  /// The number of entries in entity_type_ids should be equal to topo_size and
  /// properties->num_rows().
  template <template <typename> class Vector>
  static Result<void> AssignEntityTypeIDsFromProperties(
      const size_t topo_size,  // == either num_nodes() or  num_edges()
      const std::shared_ptr<arrow::Table>& properties,
      katana::EntityTypeManager* entity_type_manager,
      Vector<EntityTypeID>* entity_type_ids) {
    KATANA_LOG_ASSERT(entity_type_ids->size() == topo_size);
    int64_t num_rows = properties->num_rows();
    if (num_rows == 0) {
      std::fill(
          entity_type_ids->begin(), entity_type_ids->end(), kUnknownEntityType);
      return ResultSuccess();
    }
    auto num_rows_for_comparison = static_cast<size_t>(num_rows);
    KATANA_LOG_VASSERT(
        entity_type_ids->size() == num_rows_for_comparison,
        "size: {}, expected: {}, signed expected: {}", entity_type_ids->size(),
        num_rows_for_comparison, num_rows);

    // throw an error if each column/property has more than 1 chunk
    for (int i = 0, n = properties->num_columns(); i < n; i++) {
      std::shared_ptr<arrow::ChunkedArray> property = properties->column(i);
      if (property->num_chunks() != 1) {
        return KATANA_ERROR(
            katana::ErrorCode::NotImplemented,
            "property {} has {} chunks (1 chunk expected)",
            properties->schema()->field(i)->name(), property->num_chunks());
      }
    }

    // collect the list of types
    std::vector<int> type_field_indices;
    using BoolPropertyColumn = PropertyColumn<arrow::BooleanArray>;
    std::vector<BoolPropertyColumn> bool_properties;
    using UInt8PropertyColumn = PropertyColumn<arrow::UInt8Array>;
    std::vector<UInt8PropertyColumn> uint8_properties;
    const std::shared_ptr<arrow::Schema>& schema = properties->schema();
    KATANA_LOG_DEBUG_ASSERT(schema->num_fields() == properties->num_columns());
    for (int i = 0, n = schema->num_fields(); i < n; i++) {
      const std::shared_ptr<arrow::Field>& current_field = schema->field(i);

      // a bool or uint8 property is (always) considered a type
      // TODO(roshan) make this customizable by the user
      if (current_field->type()->Equals(arrow::boolean())) {
        type_field_indices.push_back(i);
        std::shared_ptr<arrow::Array> property =
            properties->column(i)->chunk(0);
        auto bool_property =
            std::static_pointer_cast<arrow::BooleanArray>(property);
        bool_properties.emplace_back(i, bool_property);
      } else if (current_field->type()->Equals(arrow::uint8())) {
        type_field_indices.push_back(i);
        std::shared_ptr<arrow::Array> property =
            properties->column(i)->chunk(0);
        auto uint8_property =
            std::static_pointer_cast<arrow::UInt8Array>(property);
        uint8_properties.emplace_back(i, uint8_property);
      }
    }

    // assign a new ID to each type
    // NB: cannot use unordered_map without defining a hash function for vectors;
    // performance is not affected here because the map is very small (<=256)
    std::map<std::vector<int>, katana::EntityTypeID> type_field_indices_to_id;
    for (int i : type_field_indices) {
      const std::shared_ptr<arrow::Field>& current_field = schema->field(i);
      const std::string& field_name = current_field->name();
      katana::EntityTypeID new_entity_type_id =
          entity_type_manager->AddAtomicEntityType(field_name);

      std::vector<int> field_indices = {i};
      type_field_indices_to_id.emplace(
          std::make_pair(field_indices, new_entity_type_id));
    }

    // collect the list of unique combination of types
    using FieldEntityType = std::vector<int>;
    // NB: cannot use unordered_set without defining a hash function for vectors;
    // performance is not affected here because the set is very small (<=256)
    using FieldEntityTypeSet = std::set<FieldEntityType>;
    FieldEntityTypeSet type_combinations;
    for (int64_t row = 0, num_rows = properties->num_rows(); row < num_rows;
         ++row) {
      FieldEntityType field_indices;
      for (auto bool_property : bool_properties) {
        if (bool_property.array->IsValid(row) &&
            bool_property.array->Value(row)) {
          field_indices.emplace_back(bool_property.field_index);
        }
      }
      for (auto uint8_property : uint8_properties) {
        if (uint8_property.array->IsValid(row) &&
            uint8_property.array->Value(row)) {
          field_indices.emplace_back(uint8_property.field_index);
        }
      }
      if (field_indices.size() > 1) {
        type_combinations.emplace(field_indices);
      }
    }

    // assign a new ID to each unique combination of types
    for (const FieldEntityType& field_indices : type_combinations) {
      std::vector<std::string> field_names;
      for (int i : field_indices) {
        const std::shared_ptr<arrow::Field>& current_field = schema->field(i);
        const std::string& field_name = current_field->name();
        field_names.emplace_back(field_name);
      }
      katana::EntityTypeID new_entity_type_id =
          entity_type_manager->AddNonAtomicEntityType(field_names);
      type_field_indices_to_id.emplace(
          std::make_pair(field_indices, new_entity_type_id));
    }

    // assert that all type IDs (including kUnknownEntityType) and
    // 1 special type ID (kInvalidEntityType)
    // can be stored in 8 bits
    if (entity_type_manager->GetNumEntityTypes() >
        (std::numeric_limits<katana::EntityTypeID>::max() - size_t{1})) {
      return KATANA_ERROR(
          katana::ErrorCode::NotImplemented,
          "number of unique combination of types is {} "
          "but only up to {} is supported currently",
          // exclude kUnknownEntityType
          entity_type_manager->GetNumEntityTypes() - 1,
          // exclude kUnknownEntityType and kInvalidEntityType
          std::numeric_limits<katana::EntityTypeID>::max() - 2);
    }

    // assign the type ID for each row
    for (int64_t row = 0; row < num_rows; ++row) {
      FieldEntityType field_indices;
      for (auto bool_property : bool_properties) {
        if (bool_property.array->IsValid(row) &&
            bool_property.array->Value(row)) {
          field_indices.emplace_back(bool_property.field_index);
        }
      }
      for (auto uint8_property : uint8_properties) {
        if (uint8_property.array->IsValid(row) &&
            uint8_property.array->Value(row)) {
          field_indices.emplace_back(uint8_property.field_index);
        }
      }
      if (field_indices.empty()) {
        entity_type_ids->at(row) = katana::kUnknownEntityType;
      } else {
        katana::EntityTypeID entity_type_id =
            type_field_indices_to_id.at(field_indices);
        entity_type_ids->at(row) = entity_type_id;
      }
    }

    return ResultSuccess();
  }

  // TODO(amber): delete this method. It's risky
  void Reset() {
    atomic_entity_type_id_to_type_name_.clear();
    atomic_type_name_to_entity_type_id_.clear();
    entity_type_id_to_atomic_entity_type_ids_.clear();
    atomic_entity_type_id_to_entity_type_ids_.clear();
    Init();
  }

  /// adds a new entity type for the atomic type with name \p name
  /// \returns the EntityTypeID for the new type
  EntityTypeID AddAtomicEntityType(const std::string& name) {
    KATANA_LOG_DEBUG_ASSERT(!HasAtomicType(name));

    // TODO(roshan) limit the new entity type id to the max for 8 bits
    // and return an error
    EntityTypeID new_entity_type_id = GetNumEntityTypes();
    atomic_entity_type_id_to_type_name_.emplace(
        std::make_pair(new_entity_type_id, name));
    atomic_type_name_to_entity_type_id_.emplace(
        std::make_pair(name, new_entity_type_id));

    SetOfEntityTypeIDs entity_type_ids;
    entity_type_ids.set(new_entity_type_id);
    entity_type_id_to_atomic_entity_type_ids_.emplace_back(entity_type_ids);
    atomic_entity_type_id_to_entity_type_ids_.emplace_back(entity_type_ids);

    return new_entity_type_id;
  }

  /// adds a new entity type for the set of atomic types with names \p names
  /// \returns the EntityTypeID for the new type
  EntityTypeID AddNonAtomicEntityType(const std::vector<std::string>& names) {
    for (auto& name : names) {
      if (!HasAtomicType(name)) {
        AddAtomicEntityType(name);
      }
    }

    // TODO(roshan) limit the new entity type id to the max for 8 bits
    // and return an error
    EntityTypeID new_entity_type_id = GetNumEntityTypes();
    entity_type_id_to_atomic_entity_type_ids_.emplace_back(
        SetOfEntityTypeIDs());
    atomic_entity_type_id_to_entity_type_ids_.emplace_back(
        SetOfEntityTypeIDs());
    for (auto& name : names) {
      EntityTypeID atomic_entity_type_id = GetEntityTypeID(name);
      entity_type_id_to_atomic_entity_type_ids_.at(new_entity_type_id)
          .set(atomic_entity_type_id);
      atomic_entity_type_id_to_entity_type_ids_.at(atomic_entity_type_id)
          .set(new_entity_type_id);
    }

    return new_entity_type_id;
  }

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
    return (sub_atomic_types & super_atomic_types) == sub_atomic_types;
  }

  const EntityTypeIDToSetOfEntityTypeIDsMap&
  GetEntityTypeIDToAtomicEntityTypeIDs() const {
    return entity_type_id_to_atomic_entity_type_ids_;
  }

  const EntityTypeIDToAtomicTypeNameMap& GetEntityTypeIDToAtomicTypeNameMap()
      const {
    return atomic_entity_type_id_to_type_name_;
  }

  /// bool Equals() IS A TESTING ONLY FUNCTION, DO NOT EXPOSE THIS TO THE USER
  bool Equals(const EntityTypeManager& other) const {
    if (entity_type_id_to_atomic_entity_type_ids_ !=
        other.entity_type_id_to_atomic_entity_type_ids_) {
      return false;
    }
    if (atomic_entity_type_id_to_type_name_ !=
        other.atomic_entity_type_id_to_type_name_) {
      return false;
    }

    if (atomic_type_name_to_entity_type_id_ !=
        other.atomic_type_name_to_entity_type_id_) {
      return false;
    }

    if (atomic_entity_type_id_to_entity_type_ids_ !=
        other.atomic_entity_type_id_to_entity_type_ids_) {
      return false;
    }
    return true;
  }

  /// std::string ReportDiff() IS A TESTING ONLY FUNCTION, DO NOT EXPOSE THIS TO THE USER
  std::string ReportDiff(const EntityTypeManager& other) const {
    fmt::memory_buffer buf;
    if (entity_type_id_to_atomic_entity_type_ids_ !=
        other.entity_type_id_to_atomic_entity_type_ids_) {
      fmt::format_to(
          std::back_inserter(buf),
          "entity_type_id_to_atomic_entity_type_ids_ differ. size {}"
          "vs. {}\n",
          entity_type_id_to_atomic_entity_type_ids_.size(),
          other.entity_type_id_to_atomic_entity_type_ids_.size());
    } else {
      fmt::format_to(
          std::back_inserter(buf),
          "entity_type_id_to_atomic_entity_type_ids_ match!\n");
    }
    if (atomic_entity_type_id_to_type_name_ !=
        other.atomic_entity_type_id_to_type_name_) {
      fmt::format_to(
          std::back_inserter(buf),
          "atomic_entity_type_id_to_type_name_ differ. size {}"
          "vs. {}\n",
          atomic_entity_type_id_to_type_name_.size(),
          other.atomic_entity_type_id_to_type_name_.size());
    } else {
      fmt::format_to(
          std::back_inserter(buf),
          "atomic_entity_type_id_to_type_name_ match!\n");
    }
    if (atomic_type_name_to_entity_type_id_ !=
        other.atomic_type_name_to_entity_type_id_) {
      fmt::format_to(
          std::back_inserter(buf),
          "atomic_type_name_to_entity_type_id_ differ. size {}"
          "vs. {}\n",
          atomic_type_name_to_entity_type_id_.size(),
          other.atomic_type_name_to_entity_type_id_.size());
    } else {
      fmt::format_to(
          std::back_inserter(buf),
          "atomic_type_name_to_entity_type_id_ match!\n");
    }

    if (atomic_entity_type_id_to_entity_type_ids_ !=
        other.atomic_entity_type_id_to_entity_type_ids_) {
      fmt::format_to(
          std::back_inserter(buf),
          "atomic_entity_type_id_to_entity_type_ids_ differ. size {}"
          "vs. {}\n",
          atomic_entity_type_id_to_entity_type_ids_.size(),
          other.atomic_entity_type_id_to_entity_type_ids_.size());
    } else {
      fmt::format_to(
          std::back_inserter(buf),
          "atomic_entity_type_id_to_entity_type_ids_ match!\n");
    }
    return std::string(buf.begin(), buf.end());
  }

private:
  void Init() {
    // assume kUnknownEntityType is 0
    static_assert(kUnknownEntityType == 0);
    static_assert(kUnknownEntityTypeName == std::string_view("kUnknownName"));
    // add kUnknownEntityType
    auto id = AddAtomicEntityType(std::string(kUnknownEntityTypeName));
    KATANA_LOG_ASSERT(id == kUnknownEntityType);
  }

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
