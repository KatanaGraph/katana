#include "katana/EntityTypeManager.h"

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
    // TODO(roshan) make this customizable by the user
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

  entity_type_id_to_atomic_entity_type_ids_.emplace_back(type_id_set);
  atomic_entity_type_id_to_entity_type_ids_.emplace_back(SetOfEntityTypeIDs());
  for (size_t atomic_entity_type_id = 0;
       atomic_entity_type_id < type_id_set.size(); ++atomic_entity_type_id) {
    if (type_id_set[atomic_entity_type_id]) {
      atomic_entity_type_id_to_entity_type_ids_.at(atomic_entity_type_id)
          .set(new_entity_type_id);
    }
  }

  // Ideally this would return an error instead of failing. But checking is
  // probably too slow. Remember kids, fast is more important than correct.
  KATANA_LOG_DEBUG_VASSERT(
      std::count(
          entity_type_id_to_atomic_entity_type_ids_.begin(),
          entity_type_id_to_atomic_entity_type_ids_.end(), type_id_set) == 1,
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
  auto existing_id = std::find(
      entity_type_id_to_atomic_entity_type_ids_.cbegin(),
      entity_type_id_to_atomic_entity_type_ids_.cend(), type_id_set);
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
  auto existing_id = std::find(
      entity_type_id_to_atomic_entity_type_ids_.cbegin(),
      entity_type_id_to_atomic_entity_type_ids_.cend(), type_id_set);
  if (existing_id != entity_type_id_to_atomic_entity_type_ids_.cend()) {
    // We rely on the fact that entity_type_id_to_atomic_entity_type_ids_ is a
    // vector here so that distances are the same as type IDs.
    return Result<EntityTypeID>(std::distance(
        entity_type_id_to_atomic_entity_type_ids_.cbegin(), existing_id));
  }

  return KATANA_ERROR(
      katana::ErrorCode::NotFound,
      "no compound type found for given set of atomic types: {}", type_id_set);
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

  atomic_entity_type_id_to_type_name_.emplace(new_entity_type_id, name);
  atomic_type_name_to_entity_type_id_.emplace(name, new_entity_type_id);

  SetOfEntityTypeIDs entity_type_ids;
  entity_type_ids.set(new_entity_type_id);
  entity_type_id_to_atomic_entity_type_ids_.emplace_back(entity_type_ids);
  atomic_entity_type_id_to_entity_type_ids_.emplace_back(entity_type_ids);

  return Result<EntityTypeID>(new_entity_type_id);
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
  } else {
    fmt::format_to(
        std::back_inserter(buf),
        "atomic_entity_type_id_to_entity_type_ids_ match!\n");
  }
  return std::string(buf.begin(), buf.end());
}

bool
katana::EntityTypeManager::Equals(
    const katana::EntityTypeManager& other) const {
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
    if (type_set[idx]) {
      auto name = GetAtomicTypeName(idx);
      KATANA_LOG_ASSERT(name.has_value());
      type_name_set.insert(name.value());
    }
  }
  return type_name_set;
}
