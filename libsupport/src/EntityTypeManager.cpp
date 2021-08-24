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

    // a bool or uint8 property is (always) considered a type
    // TODO(roshan) make this customizable by the user
    if (current_field->type()->Equals(arrow::boolean())) {
      type_field_indices.push_back(i);
      std::shared_ptr<arrow::Array> property = properties->column(i)->chunk(0);
      auto bool_property =
          std::static_pointer_cast<arrow::BooleanArray>(property);
      type_properties.bool_properties.emplace_back(i, bool_property);
    } else if (current_field->type()->Equals(arrow::uint8())) {
      type_field_indices.push_back(i);
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
        entity_type_manager->AddAtomicEntityType(field_name);

    std::vector<int> field_indices = {i};
    type_properties.type_field_indices_to_id.emplace(
        std::make_pair(field_indices, new_entity_type_id));
  }

  // NB: cannot use unordered_set without defining a hash function for vectors;
  // performance is not affected here because the set is very small (<=256)
  using FieldEntityTypeSet = std::set<TypeProperties::FieldEntity>;
  FieldEntityTypeSet type_combinations;
  for (int64_t row = 0, num_rows = properties->num_rows(); row < num_rows;
       ++row) {
    TypeProperties::FieldEntity field_indices;
    for (auto bool_property : type_properties.bool_properties) {
      if (bool_property.array->IsValid(row) &&
          bool_property.array->Value(row)) {
        field_indices.emplace_back(bool_property.field_index);
      }
    }
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
    katana::EntityTypeID new_entity_type_id =
        entity_type_manager->AddNonAtomicEntityType(field_names);
    type_properties.type_field_indices_to_id.emplace(
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

  return type_properties;
}
