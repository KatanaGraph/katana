#include "RDGCore.h"

#include "RDGPartHeader.h"
#include "RDGTopologyManager.h"
#include "katana/ArrowInterchange.h"
#include "katana/ErrorCode.h"
#include "katana/ParquetReader.h"
#include "katana/RDGPrefix.h"
#include "katana/Result.h"

namespace {

katana::Result<std::set<std::string>>
UpsertProperties(
    const std::shared_ptr<arrow::Table>& props,
    std::shared_ptr<arrow::Table>* to_update,
    std::vector<katana::PropStorageInfo>* prop_state) {
  std::set<std::string> written_prop_names;
  if (!props->schema()->HasDistinctFieldNames()) {
    return KATANA_ERROR(
        katana::ErrorCode::AlreadyExists, "column names must be distinct: {}",
        fmt::join(props->schema()->field_names(), ", "));
  }

  if (prop_state->empty()) {
    KATANA_LOG_ASSERT((*to_update)->num_columns() == 0);
    for (const auto& field : props->fields()) {
      prop_state->emplace_back(field->name(), field->type());
      written_prop_names.insert(field->name());
    }
    *to_update = props;
    return written_prop_names;
  }

  std::shared_ptr<arrow::Table> next = *to_update;

  if (next->num_columns() > 0 && next->num_rows() != props->num_rows()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        next->num_rows(), props->num_rows());
  }

  int last = next->num_columns();

  for (int i = 0, n = props->num_columns(); i < n; i++) {
    std::shared_ptr<arrow::Field> field = props->field(i);
    int current_col = -1;

    auto prop_info_it = std::find_if(
        prop_state->begin(), prop_state->end(),
        [&](const katana::PropStorageInfo& psi) {
          return field->name() == psi.name();
        });
    if (prop_info_it == prop_state->end()) {
      prop_info_it = prop_state->insert(
          prop_info_it, katana::PropStorageInfo(field->name(), field->type()));
    } else if (!prop_info_it->IsAbsent()) {
      current_col = next->schema()->GetFieldIndex(field->name());
    }

    if (current_col < 0) {
      if (next->num_columns() == 0) {
        next = arrow::Table::Make(arrow::schema({field}), {props->column(i)});
      } else {
        next = KATANA_CHECKED_CONTEXT(
            next->AddColumn(last++, field, props->column(i)),
            "insert; column {}", i);
      }
    } else {
      next = KATANA_CHECKED_CONTEXT(
          next->SetColumn(current_col, field, props->column(i)),
          "update; column {}", i);
    }
    prop_info_it->WasModified(field->type());
    written_prop_names.insert(field->name());
  }

  if (!next->schema()->HasDistinctFieldNames()) {
    return KATANA_ERROR(
        katana::ErrorCode::AlreadyExists, "column names are not distinct: {}",
        fmt::join(next->schema()->field_names(), ", "));
  }

  *to_update = next;

  return written_prop_names;
}

katana::Result<std::set<std::string>>
AddProperties(
    const std::shared_ptr<arrow::Table>& props,
    std::shared_ptr<arrow::Table>* to_update,
    std::vector<katana::PropStorageInfo>* prop_state) {
  for (const auto& field : props->fields()) {
    // Column names are not sorted, but assumed to be less than 100s
    auto prop_info_it = std::find_if(
        prop_state->begin(), prop_state->end(),
        [&](const katana::PropStorageInfo& psi) {
          return field->name() == psi.name();
        });

    if (prop_info_it != prop_state->end()) {
      return KATANA_ERROR(
          katana::ErrorCode::AlreadyExists, "column names are not distinct");
    }
  }

  return UpsertProperties(props, to_update, prop_state);
}

katana::Result<void>
EnsureTypeLoaded(const katana::URI& rdg_dir, katana::PropStorageInfo* psi) {
  if (!psi->type()) {
    auto reader = KATANA_CHECKED(katana::ParquetReader::Make());
    KATANA_LOG_ASSERT(psi->IsAbsent());
    std::shared_ptr<arrow::Schema> schema =
        KATANA_CHECKED(reader->GetSchema(rdg_dir.Join(psi->path())));
    psi->set_type(schema->field(0)->type());
  }
  return katana::ResultSuccess();
}

std::shared_ptr<arrow::Schema>
schemify(const std::vector<katana::PropStorageInfo>& prop_info_list) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (const auto& prop : prop_info_list) {
    KATANA_LOG_VASSERT(
        prop.type(), "should be impossible for type of {} to be null here",
        prop.name());
    fields.emplace_back(
        std::make_shared<arrow::Field>(prop.name(), prop.type()));
  }
  return arrow::schema(fields);
}

katana::Result<katana::NUMAArray<katana::EntityTypeID>>
LoadIDArray(
    size_t begin, size_t end, const katana::URI& types_path,
    const katana::RDGPartHeader& part_header) {
  katana::NUMAArray<katana::EntityTypeID> types;

  // NB: we add sizeof(EntityTypeIDArrayHeader) to every range element because
  // the structure of this file is
  // [header, value, value, value, ...]
  // Recent storage formats remove this header
  size_t header_size = part_header.IsHeaderlessEntityTypeIDArray()
                           ? 0
                           : sizeof(katana::EntityTypeIDArrayHeader);
  size_t storage_begin = begin * sizeof(katana::EntityTypeID) + header_size;
  size_t storage_end = end * sizeof(katana::EntityTypeID) + header_size;

  katana::FileView fv;

  KATANA_CHECKED_CONTEXT(
      fv.Bind(types_path.string(), storage_begin, storage_end, true),
      "loading node type id array, begin: {}, end: {}", begin, end);

  types.allocateInterleaved(end - begin);

  const auto* storage_types = fv.template valid_ptr<katana::EntityTypeID>();
  katana::do_all(
      katana::iterate(size_t{0}, end - begin),
      [&types, &storage_types](size_t i) { types[i] = storage_types[i]; });

  return types;
}

}  // namespace

std::shared_ptr<arrow::Schema>
katana::RDGCore::full_node_schema() const {
  return schemify(part_header().node_prop_info_list());
}

std::shared_ptr<arrow::Schema>
katana::RDGCore::full_edge_schema() const {
  return schemify(part_header().edge_prop_info_list());
}

katana::Result<void>
katana::RDGCore::AddPartitionMetadataArray(
    const std::shared_ptr<arrow::Table>& props) {
  auto field = props->schema()->field(0);
  const std::string& name = field->name();
  std::shared_ptr<arrow::ChunkedArray> col = props->column(0);

  if (name.find(kMirrorNodesPropName) == 0) {
    AddMirrorNodes(std::move(col));
  } else if (name.find(kMasterNodesPropName) == 0) {
    AddMasterNodes(std::move(col));
  } else if (name == kHostToOwnedGlobalNodeIDsPropName) {
    set_host_to_owned_global_node_ids(std::move(col));
  } else if (name == kHostToOwnedGlobalEdgeIDsPropName) {
    set_host_to_owned_global_edge_ids(std::move(col));
  } else if (name == kLocalToUserIDPropName) {
    set_local_to_user_id(std::move(col));
  } else if (name == kLocalToGlobalIDPropName) {
    set_local_to_global_id(std::move(col));
  } else if (name == kDeprecatedLocalToGlobalIDPropName) {
    KATANA_LOG_WARN(
        "deprecated graph format; replace the existing graph by storing the "
        "current graph");
    set_local_to_global_id(std::move(col));
  } else if (name == kDeprecatedHostToOwnedGlobalNodeIDsPropName) {
    KATANA_LOG_WARN(
        "deprecated graph format; replace the existing graph by storing the "
        "current graph");
    set_host_to_owned_global_node_ids(std::move(col));
  } else {
    return KATANA_ERROR(ErrorCode::InvalidArgument, "checking metadata name");
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGCore::AddNodeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG_ASSERT(txn_ctx != nullptr);
  auto written_prop_names = KATANA_CHECKED(AddProperties(
      props, &node_properties_, &part_header_.node_prop_info_list()));
  // store write properties into transaction context
  txn_ctx->InsertNodePropertyWrite<std::set<std::string>>(
      rdg_dir_, written_prop_names);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGCore::AddEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG_ASSERT(txn_ctx != nullptr);
  auto written_prop_names = KATANA_CHECKED(AddProperties(
      props, &edge_properties_, &part_header_.edge_prop_info_list()));
  // store write properties into transaction context
  txn_ctx->InsertEdgePropertyWrite<std::set<std::string>>(
      rdg_dir_, written_prop_names);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGCore::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG_ASSERT(txn_ctx != nullptr);
  auto written_prop_names = KATANA_CHECKED(UpsertProperties(
      props, &node_properties_, &part_header_.node_prop_info_list()));
  // store write properties into transaction context
  txn_ctx->InsertNodePropertyWrite<std::set<std::string>>(
      rdg_dir_, written_prop_names);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGCore::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, katana::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG_ASSERT(txn_ctx != nullptr);
  auto written_prop_names = KATANA_CHECKED(UpsertProperties(
      props, &edge_properties_, &part_header_.edge_prop_info_list()));
  // store write properties into transaction context
  txn_ctx->InsertEdgePropertyWrite<std::set<std::string>>(
      rdg_dir_, written_prop_names);

  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGCore::EnsureNodeTypesLoaded() {
  if (rdg_dir_.empty()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument,
        "no rdg_dir set, cannot ensure node types are loaded");
  }
  for (auto& prop : part_header_.node_prop_info_list()) {
    KATANA_CHECKED_CONTEXT(
        EnsureTypeLoaded(rdg_dir_, &prop), "property {}",
        std::quoted(prop.name()));
  }
  return katana::ResultSuccess();
}

katana::Result<void>
katana::RDGCore::EnsureEdgeTypesLoaded() {
  if (rdg_dir_.empty()) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument,
        "no rdg_dir set, cannot ensure edge types are loaded");
  }
  for (auto& prop : part_header_.edge_prop_info_list()) {
    KATANA_CHECKED_CONTEXT(
        EnsureTypeLoaded(rdg_dir_, &prop), "property {}",
        std::quoted(prop.name()));
  }
  return katana::ResultSuccess();
}

void
katana::RDGCore::InitEmptyProperties() {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
}

bool
katana::RDGCore::Equals(const katana::RDGCore& other) const {
  // Assumption: all RDGTopology objeccts in topology_manger_ and other.topology_manager_
  // are bound to their files
  return topology_manager_.Equals(other.topology_manager_) &&
         node_properties_->Equals(*other.node_properties_, true) &&
         edge_properties_->Equals(*other.edge_properties_, true);

  return true;
}

katana::Result<void>
katana::RDGCore::RemoveNodeProperty(int i, katana::TxnContext* txn_ctx) {
  auto field = node_properties_->field(i);
  node_properties_ = KATANA_CHECKED(node_properties_->RemoveColumn(i));
  // store write properties into transaction context
  txn_ctx->InsertNodePropertyWrite(rdg_dir_, field->name());

  return part_header_.RemoveNodeProperty(field->name());
}

katana::Result<void>
katana::RDGCore::RemoveEdgeProperty(int i, katana::TxnContext* txn_ctx) {
  auto field = edge_properties_->field(i);
  edge_properties_ = KATANA_CHECKED(edge_properties_->RemoveColumn(i));
  // store write properties into transaction context
  txn_ctx->InsertEdgePropertyWrite(rdg_dir_, field->name());

  return part_header_.RemoveEdgeProperty(field->name());
}

katana::Result<katana::NUMAArray<katana::EntityTypeID>>
katana::RDGCore::node_entity_type_id_array(size_t begin, size_t end) const {
  if (!part_header().IsUint16tEntityTypeIDs()) {
    NUMAArray<EntityTypeID> types;
    return types;
  }
  end = std::min(end, size_t{part_header().metadata().num_nodes_});

  katana::URI node_types_path =
      rdg_dir_.Join(part_header().node_entity_type_id_array_path());

  return KATANA_CHECKED(
      LoadIDArray(begin, end, node_types_path, part_header()));
}
katana::Result<katana::NUMAArray<katana::EntityTypeID>>
katana::RDGCore::edge_entity_type_id_array(size_t begin, size_t end) const {
  if (!part_header().IsUint16tEntityTypeIDs()) {
    NUMAArray<EntityTypeID> types;
    return types;
  }
  end = std::min(end, size_t{part_header().metadata().num_edges_});

  katana::URI edge_types_path =
      rdg_dir_.Join(part_header().edge_entity_type_id_array_path());

  return KATANA_CHECKED(
      LoadIDArray(begin, end, edge_types_path, part_header()));
}
