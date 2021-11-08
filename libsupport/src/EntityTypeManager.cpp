#include "katana/EntityTypeManager.h"

#include "katana/Logging.h"
#include "katana/Result.h"

//TODO(emcginnis): while this logic works and technically saves cycles by avoiding
// looping through all of the sets too frequently, its cumbersome and likely not worth it
// simplify to just resizing as we need to, and let the reserve functionality in the backend
// keep calls to resize() fast
size_t
katana::EntityTypeManager::CalculateSetOfEntityTypeIDsSize(
    EntityTypeID max_id) {
  // min number of bits to fit bitset[max_id] is max_id +1
  size_t min_size = max_id + 1;

  KATANA_LOG_VASSERT(
      max_id < kInvalidEntityType, "Katana only supports {} entity types",
      kInvalidEntityType);

  size_t new_size = kDefaultSetOfEntityTypeIDsSize;
  // Double the default size of the set until it is bigger than min_size.
  // There are faster, more clever ways to do this, like using a DeBruijn sequence but
  // 1) we don't use this function too often
  // 2) this is easier to understand
  while (new_size < min_size) {
    new_size = new_size * 2;
  }

  // catch overflow wraparound
  if (new_size < min_size) {
    new_size = kMaxSetOfEntityTypeIDsSize;
  }

  // must not allow the set size to excede the max number of EntityTypeIDs
  if (new_size > kMaxSetOfEntityTypeIDsSize) {
    new_size = kMaxSetOfEntityTypeIDsSize;
  }

  return new_size;
}

katana::Result<katana::EntityTypeManager::TypeProperties>
katana::EntityTypeManager::DoAssignEntityTypeIDsFromProperties(
    const std::shared_ptr<arrow::Table>& properties,
    EntityTypeManager* entity_type_manager) {
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
  TypeProperties type_properties;
  TypeProperties::FieldEntity type_field_indices;
  const std::shared_ptr<arrow::Schema>& schema = properties->schema();

  KATANA_LOG_DEBUG_ASSERT(schema->num_fields() == properties->num_columns());
  for (int i = 0, n = schema->num_fields(); i < n; i++) {
    const std::shared_ptr<arrow::Field>& current_field = schema->field(i);

    // a uint8 property is (always) considered a type
    if (current_field->type()->Equals(arrow::uint8())) {
      type_field_indices.push_back(i);
      KATANA_LOG_DEBUG_ASSERT(properties->column(i)->num_chunks() == 1);
      std::shared_ptr<arrow::Array> property = properties->column(i)->chunk(0);
      auto uint8_property =
          std::static_pointer_cast<arrow::UInt8Array>(property);
      type_properties.uint8_properties.emplace_back(i, uint8_property);
    }
  }

  // assign a new ID to each type
  // NB: cannot use unordered_map without defining a hash function for vectors;
  // performance is not affected here because the map is very small (<=256)
  for (int i : type_field_indices) {
    const std::shared_ptr<arrow::Field>& current_field = schema->field(i);
    const std::string& field_name = current_field->name();
    katana::EntityTypeID new_entity_type_id =
        KATANA_CHECKED(entity_type_manager->AddAtomicEntityType(field_name));

    std::vector<int> field_indices = {i};
    type_properties.type_field_indices_to_id.emplace(
        field_indices, new_entity_type_id);
  }

  // NB: cannot use unordered_set without defining a hash function for vectors;
  // performance is not affected here because the set is very small (<=256)
  using FieldEntityTypeSet = std::set<TypeProperties::FieldEntity>;
  FieldEntityTypeSet type_combinations;
  for (int64_t row = 0, num_rows = properties->num_rows(); row < num_rows;
       ++row) {
    TypeProperties::FieldEntity field_indices;
    for (auto uint8_property : type_properties.uint8_properties) {
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
  for (const TypeProperties::FieldEntity& field_indices : type_combinations) {
    std::vector<std::string> field_names;
    for (int i : field_indices) {
      const std::shared_ptr<arrow::Field>& current_field = schema->field(i);
      const std::string& field_name = current_field->name();
      field_names.emplace_back(field_name);
    }
    katana::EntityTypeID new_entity_type_id = KATANA_CHECKED(
        entity_type_manager->AddNonAtomicEntityType(KATANA_CHECKED(
            entity_type_manager->template GetOrAddEntityTypeIDs(field_names))));
    type_properties.type_field_indices_to_id.emplace(
        field_indices, new_entity_type_id);
  }

  // assert that all type IDs (including kUnknownEntityType) and
  // 1 special type ID (kInvalidEntityType)
  // can be stored in kMaxSetOfEntityTypeIDsSize bits
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

  return type_properties;
}

katana::Result<katana::EntityTypeID>
katana::EntityTypeManager::AddNonAtomicEntityType(
    const katana::SetOfEntityTypeIDs& type_id_set) {
  // Add the element since it doesn't exist.
  EntityTypeID new_entity_type_id = GetNumEntityTypes();
  if (new_entity_type_id >= kInvalidEntityType) {
    return KATANA_ERROR(
        ErrorCode::NotImplemented, "Katana only supports {} entity types",
        kInvalidEntityType);
  }

  // Ensure the bitmaps can fit the new entity_type_id
  ResizeSetOfEntityTypeIDsMaps(new_entity_type_id);
  SetOfEntityTypeIDs type_id_set_resized = type_id_set;
  type_id_set_resized.resize(SetOfEntityTypeIDsSize_);
  entity_type_id_to_atomic_entity_type_ids_.emplace_back(type_id_set_resized);

  SetOfEntityTypeIDs empty_set;
  empty_set.resize(SetOfEntityTypeIDsSize_);
  atomic_entity_type_id_to_entity_type_ids_.emplace_back(empty_set);

  for (size_t atomic_entity_type_id = 0;
       atomic_entity_type_id < type_id_set.size(); ++atomic_entity_type_id) {
    if (type_id_set.test(atomic_entity_type_id)) {
      atomic_entity_type_id_to_entity_type_ids_.at(atomic_entity_type_id)
          .set(new_entity_type_id);
    }
  }

  // Ideally this would return an error instead of failing. But checking is
  // probably too slow. Remember kids, fast is more important than correct.
  // Exclude this check if our EntityTypeID == 0, as we expect it to have the empty set
  KATANA_LOG_DEBUG_VASSERT(
      new_entity_type_id == 0 ||
          std::count(
              entity_type_id_to_atomic_entity_type_ids_.begin(),
              entity_type_id_to_atomic_entity_type_ids_.end(),
              type_id_set_resized) == 1,
      "AddNonAtomicEntityType called with type_id_set that is already "
      "present.");

  return Result<EntityTypeID>(new_entity_type_id);
}

katana::Result<katana::EntityTypeID>
katana::EntityTypeManager::GetOrAddNonAtomicEntityType(
    const katana::SetOfEntityTypeIDs& type_id_set) {
  // Find a previous type ID for this set of types. This is a linear search
  // and O(n types). However, n types is expected to stay small so this isn't
  // a big issue. If this does turn out to be a performance problem we could
  // keep around an extra map from type ID sets to type IDs.

  katana::SetOfEntityTypeIDs type_id_set_resized = type_id_set;
  type_id_set_resized.resize(SetOfEntityTypeIDsSize_);

  auto existing_id = std::find(
      entity_type_id_to_atomic_entity_type_ids_.cbegin(),
      entity_type_id_to_atomic_entity_type_ids_.cend(), type_id_set_resized);
  if (existing_id != entity_type_id_to_atomic_entity_type_ids_.cend()) {
    // We rely on the fact that entity_type_id_to_atomic_entity_type_ids_ is a
    // vector here so that distances are the same as type IDs.
    return Result<EntityTypeID>(std::distance(
        entity_type_id_to_atomic_entity_type_ids_.cbegin(), existing_id));
  }

  return AddNonAtomicEntityType(type_id_set);
}

katana::Result<katana::EntityTypeID>
katana::EntityTypeManager::GetNonAtomicEntityType(
    const katana::SetOfEntityTypeIDs& type_id_set) const {
  // Find a previous type ID for this set of types. This is a linear search
  // and O(n types). However, n types is expected to stay small so this isn't
  // a big issue. If this does turn out to be a performance problem we could
  // keep around an extra map from type ID sets to type IDs.

  katana::SetOfEntityTypeIDs type_id_set_resized = type_id_set;
  type_id_set_resized.resize(SetOfEntityTypeIDsSize_);

  auto existing_id = std::find(
      entity_type_id_to_atomic_entity_type_ids_.cbegin(),
      entity_type_id_to_atomic_entity_type_ids_.cend(), type_id_set_resized);
  if (existing_id != entity_type_id_to_atomic_entity_type_ids_.cend()) {
    // We rely on the fact that entity_type_id_to_atomic_entity_type_ids_ is a
    // vector here so that distances are the same as type IDs.
    return Result<EntityTypeID>(std::distance(
        entity_type_id_to_atomic_entity_type_ids_.cbegin(), existing_id));
  }

  return KATANA_ERROR(
      katana::ErrorCode::NotFound,
      "no compound type found for given set of atomic types");
}

katana::Result<katana::EntityTypeID>
katana::EntityTypeManager::AddAtomicEntityType(const std::string& name) {
  // This is a hash lookup, so this should be fast enough for production code.
  if (HasAtomicType(name)) {
    return KATANA_ERROR(
        ErrorCode::AlreadyExists, "Type {} already exists", name);
  }

  EntityTypeID new_entity_type_id = GetNumEntityTypes();
  if (new_entity_type_id >= kInvalidEntityType) {
    return KATANA_ERROR(
        ErrorCode::NotImplemented, "Katana only supports {} entity types",
        kInvalidEntityType);
  }

  // Ensure the bitmaps can fit the new entity_type_id
  ResizeSetOfEntityTypeIDsMaps(new_entity_type_id);

  atomic_entity_type_id_to_type_name_.emplace(new_entity_type_id, name);
  atomic_type_name_to_entity_type_id_.emplace(name, new_entity_type_id);

  SetOfEntityTypeIDs entity_type_ids;
  entity_type_ids.resize(SetOfEntityTypeIDsSize_);

  entity_type_ids.set(new_entity_type_id);
  entity_type_id_to_atomic_entity_type_ids_.emplace_back(entity_type_ids);
  atomic_entity_type_id_to_entity_type_ids_.emplace_back(entity_type_ids);

  return Result<EntityTypeID>(new_entity_type_id);
}

void
katana::EntityTypeManager::ResizeSetOfEntityTypeIDsMaps(
    katana::EntityTypeID new_entity_type_id) {
  // if entity_type_id_to_atomic_entity_type_ids has bitset entries, then so will atomic_entity_type_id_to_entity_type_ids
  if (entity_type_id_to_atomic_entity_type_ids_.empty()) {
    // no bitsets, no work to do
    return;
  }

  KATANA_LOG_ASSERT(
      !entity_type_id_to_atomic_entity_type_ids_.empty() &&
      !atomic_entity_type_id_to_entity_type_ids_.empty());

  // Assume that all of the bitsets are the same size, since we always resize them at the same time
  // must resize if the two are equal, otherwise our bitset will be one bit too small
  if (SetOfEntityTypeIDsSize_ <= new_entity_type_id) {
    size_t new_size = CalculateSetOfEntityTypeIDsSize(new_entity_type_id);
    KATANA_LOG_WARN(
        "Resizing SetOfEntityTypeIDs Maps. Current Size = {}, New EntityTypeID "
        "= {}, New Size = {}",
        SetOfEntityTypeIDsSize_, new_entity_type_id, new_size);
    for (size_t i = 0; i < entity_type_id_to_atomic_entity_type_ids_.size();
         i++) {
      KATANA_LOG_DEBUG_VASSERT(
          entity_type_id_to_atomic_entity_type_ids_[i].size() ==
              SetOfEntityTypeIDsSize_,
          "entity_type_id_to_atomic_entity_type_ids_ bitsets must all be the "
          "same size. Expected size = {}, observed size = {}, i = {}",
          SetOfEntityTypeIDsSize_,
          entity_type_id_to_atomic_entity_type_ids_[i].size(), i);
      entity_type_id_to_atomic_entity_type_ids_[i].resize(new_size);
    }
    for (size_t i = 0; i < atomic_entity_type_id_to_entity_type_ids_.size();
         i++) {
      KATANA_LOG_DEBUG_VASSERT(
          atomic_entity_type_id_to_entity_type_ids_[i].size() ==
              SetOfEntityTypeIDsSize_,
          "atomic_entity_type_id_to_entity_type_ids_ bitsets must all be the "
          "same size. Expected size = {}, observed size = {}, i = {} ",
          SetOfEntityTypeIDsSize_,
          atomic_entity_type_id_to_entity_type_ids_[i].size(), i);
      atomic_entity_type_id_to_entity_type_ids_[i].resize(new_size);
    }

    SetOfEntityTypeIDsSize_ = new_size;
  }
}

// helper function for ToString
// Converts a SetOfEntityTypeIDs to its integer represenation
size_t
to_int(const katana::SetOfEntityTypeIDs& set) {
  size_t ret = 0;
  for (size_t i = 0; i < set.size(); i++) {
    if (set.test(i)) {
      ret += pow(2, i);
    }
  }
  return ret;
}

std::string
katana::EntityTypeManager::ToString() const {
  fmt::memory_buffer buf;

  fmt::format_to(
      std::back_inserter(buf),
      "entity_type_id_to_atomic_entity_type_ids_ size {} \n",
      entity_type_id_to_atomic_entity_type_ids_.size());

  for (size_t i = 0; i < entity_type_id_to_atomic_entity_type_ids_.size();
       i++) {
    fmt::format_to(
        std::back_inserter(buf),
        "SetOfEntityTypeIDs for EntityTypeID = {} size = {}, int =  {}\n", i,
        entity_type_id_to_atomic_entity_type_ids_.at(i).size(),
        to_int(entity_type_id_to_atomic_entity_type_ids_.at(i)));
  }

  fmt::format_to(
      std::back_inserter(buf),
      "atomic_entity_type_id_to_entity_type_ids_ size {}\n",
      atomic_entity_type_id_to_entity_type_ids_.size());

  for (size_t i = 0; i < entity_type_id_to_atomic_entity_type_ids_.size();
       i++) {
    fmt::format_to(
        std::back_inserter(buf),
        "SetOfEntityTypeIDs for EntityTypeID = {} size = {}, int =  {}\n", i,
        atomic_entity_type_id_to_entity_type_ids_.at(i).size(),
        to_int(atomic_entity_type_id_to_entity_type_ids_.at(i)));
  }
  return std::string(buf.begin(), buf.end());
}

std::string
katana::EntityTypeManager::ReportDiff(
    const katana::EntityTypeManager& other) const {
  fmt::memory_buffer buf;
  if (entity_type_id_to_atomic_entity_type_ids_ !=
      other.entity_type_id_to_atomic_entity_type_ids_) {
    fmt::format_to(
        std::back_inserter(buf),
        "entity_type_id_to_atomic_entity_type_ids_ differ. size {} "
        "vs. {}\n",
        entity_type_id_to_atomic_entity_type_ids_.size(),
        other.entity_type_id_to_atomic_entity_type_ids_.size());
    for (size_t i = 0; i < entity_type_id_to_atomic_entity_type_ids_.size();
         i++) {
      if (entity_type_id_to_atomic_entity_type_ids_.at(i) ==
          other.entity_type_id_to_atomic_entity_type_ids_.at(i)) {
        fmt::format_to(
            std::back_inserter(buf),
            "SetOfEntityTypeIDs for EntityTypeID = {} matches\n", i);
      } else {
        fmt::format_to(
            std::back_inserter(buf),
            "SetOfEntityTypeIDs for EntityTypeID = {} does not match. "
            "This.size = {}, This = {}, "
            "Other.size() = {}, Other = {}\n",
            i, entity_type_id_to_atomic_entity_type_ids_.at(i).size(),
            to_int(entity_type_id_to_atomic_entity_type_ids_.at(i)),
            other.entity_type_id_to_atomic_entity_type_ids_.at(i).size(),
            to_int(other.entity_type_id_to_atomic_entity_type_ids_.at(i)));
      }
    }
  } else {
    fmt::format_to(
        std::back_inserter(buf),
        "entity_type_id_to_atomic_entity_type_ids_ match!\n");
  }
  if (atomic_entity_type_id_to_type_name_ !=
      other.atomic_entity_type_id_to_type_name_) {
    fmt::format_to(
        std::back_inserter(buf),
        "atomic_entity_type_id_to_type_name_ differ. size {} "
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
        "atomic_type_name_to_entity_type_id_ differ. size {} "
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
        "atomic_entity_type_id_to_entity_type_ids_ differ. size {} "
        "vs. {}\n",
        atomic_entity_type_id_to_entity_type_ids_.size(),
        other.atomic_entity_type_id_to_entity_type_ids_.size());
    for (size_t i = 0; i < entity_type_id_to_atomic_entity_type_ids_.size();
         i++) {
      if (atomic_entity_type_id_to_entity_type_ids_.at(i) ==
          other.atomic_entity_type_id_to_entity_type_ids_.at(i)) {
        fmt::format_to(
            std::back_inserter(buf),
            "SetOfEntityTypeIDs for EntityTypeID = {} matches\n", i);
      } else {
        fmt::format_to(
            std::back_inserter(buf),
            "SetOfEntityTypeIDs for EntityTypeID = {} does not match. "
            "This.size = {}, This = {}, "
            "Other.size()  = {}, Other = {}\n",
            i, atomic_entity_type_id_to_entity_type_ids_.at(i).size(),
            to_int(atomic_entity_type_id_to_entity_type_ids_.at(i)),
            other.atomic_entity_type_id_to_entity_type_ids_.at(i).size(),
            to_int(other.atomic_entity_type_id_to_entity_type_ids_.at(i)));
      }
    }
  } else {
    fmt::format_to(
        std::back_inserter(buf),
        "atomic_entity_type_id_to_entity_type_ids_ match!\n");
  }
  return std::string(buf.begin(), buf.end());
}

std::string
katana::EntityTypeManager::PrintEntityTypes() const {
  fmt::memory_buffer buf;
  auto end = GetNumEntityTypes();
  for (size_t i = 0; i < end; ++i) {
    auto res = EntityTypeToTypeNameSet(i);
    if (res) {
      fmt::format_to(std::back_inserter(buf), "{:2} {}\n", i, res.value());
    } else {
      fmt::format_to(
          std::back_inserter(buf), "{:2} **error**: {}\n", i, res.error());
    }
  }
  return std::string(buf.begin(), buf.end());
}

bool
katana::EntityTypeManager::Equals(
    const katana::EntityTypeManager& other) const {
  if (entity_type_id_to_atomic_entity_type_ids_ !=
      other.entity_type_id_to_atomic_entity_type_ids_) {
    KATANA_LOG_DEBUG(
        "this.entity_type_id_to_atomic_entity_type_ids_.size() = {}, "
        "other.size() = {}. SetOfEntityTypeIDsSize = {}, "
        "other.SetOfEntityTypeIDsSize = {}",
        entity_type_id_to_atomic_entity_type_ids_.size(),
        other.entity_type_id_to_atomic_entity_type_ids_.size(),
        SetOfEntityTypeIDsSize_, other.SetOfEntityTypeIDsSize_);

    KATANA_LOG_DEBUG(
        "this.entity_type_id_to_atomic_entity_type_ids_.at(0).size = {}, "
        "other.size = "
        "{}",
        entity_type_id_to_atomic_entity_type_ids_.at(0).size(),
        other.entity_type_id_to_atomic_entity_type_ids_.at(0).size());
    return false;
  }
  if (atomic_entity_type_id_to_type_name_ !=
      other.atomic_entity_type_id_to_type_name_) {
    KATANA_LOG_DEBUG(
        "this.atomic_entity_type_id_to_type_name_.size() = {}, other.size() = "
        "{}",
        atomic_entity_type_id_to_type_name_.size(),
        other.atomic_entity_type_id_to_type_name_.size());
    return false;
  }

  if (atomic_type_name_to_entity_type_id_ !=
      other.atomic_type_name_to_entity_type_id_) {
    KATANA_LOG_DEBUG(
        "this.atomic_type_name_to_entity_type_id_.size() = {}, other.size() = "
        "{}",
        atomic_type_name_to_entity_type_id_.size(),
        other.atomic_type_name_to_entity_type_id_.size());
    return false;
  }

  if (atomic_entity_type_id_to_entity_type_ids_ !=
      other.atomic_entity_type_id_to_entity_type_ids_) {
    KATANA_LOG_DEBUG(
        "this.atomic_entity_type_id_to_entity_type_ids_.size() = {}, "
        "other.size() = {}. SetOfEntityTypeIDsSize = {}, "
        "other.SetOfEntityTypeIDsSize = {}",
        atomic_entity_type_id_to_entity_type_ids_.size(),
        other.atomic_entity_type_id_to_entity_type_ids_.size(),
        SetOfEntityTypeIDsSize_, other.SetOfEntityTypeIDsSize_);
    return false;
  }
  return true;
}

katana::Result<katana::EntityTypeID>
katana::EntityTypeManager::GetOrAddEntityTypeID(const std::string& name) {
  if (HasAtomicType(name)) {
    return GetEntityTypeID(name);
  } else {
    return AddAtomicEntityType(name);
  }
}

katana::Result<katana::TypeNameSet>
katana::EntityTypeManager::EntityTypeToTypeNameSet(
    katana::EntityTypeID type_id) const {
  katana::TypeNameSet type_name_set;
  if (type_id == katana::kInvalidEntityType || type_id >= GetNumEntityTypes()) {
    return KATANA_ERROR(
        ErrorCode::InvalidArgument,
        "no string representation for invalid type");
  }
  auto type_set = GetAtomicSubtypes(type_id);
  for (size_t idx = 0; idx < type_set.size(); ++idx) {
    if (type_set.test(idx)) {
      auto name = GetAtomicTypeName(idx);
      KATANA_LOG_ASSERT(name.has_value());
      type_name_set.insert(name.value());
    }
  }
  return type_name_set;
}
