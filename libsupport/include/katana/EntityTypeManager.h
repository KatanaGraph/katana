#pragma once

#include <bitset>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "katana/Logging.h"

namespace katana {

/// EntityTypeID uniquely identifies an entity (node or edge) type
/// EntityTypeID for nodes is distinct from EntityTypeID for edges
/// This type may either be an atomic type or an intersection of atomic types
/// EntityTypeID is represented using 8 bits
using EntityTypeID = uint8_t;
static constexpr EntityTypeID kUnknownEntityType = EntityTypeID{0};
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
    // TODO(amber): add a sentinel name for kUnknownEntityType to the maps that hold atomic
    // ID names
    // assume kUnknownEntityType is 0
    static_assert(kUnknownEntityType == 0);
    // add kUnknownEntityType
    entity_type_id_to_atomic_entity_type_ids_.emplace_back(
        SetOfEntityTypeIDs());  // for kUnknownEntityType
    atomic_entity_type_id_to_entity_type_ids_.emplace_back(
        SetOfEntityTypeIDs());  // for kUnknownEntityType
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
  EntityTypeIDToSetOfEntityTypeIDsMap atomic_entity_type_id_to_entity_type_ids_;
};

}  // namespace katana
