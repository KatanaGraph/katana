#include "RDGCore.h"

#include "RDGPartHeader.h"
#include "RDGTopologyManager.h"
#include "katana/ArrowInterchange.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/ParquetReader.h"

namespace {

katana::Result<std::unordered_set<std::string>>
UpsertProperties(
    const std::shared_ptr<arrow::Table>& props,
    std::shared_ptr<arrow::Table>* to_update,
    std::vector<tsuba::PropStorageInfo>* prop_state) {
  std::unordered_set<std::string> written_prop_names;
  if (!props->schema()->HasDistinctFieldNames()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::Exists, "column names must be distinct: {}",
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
        tsuba::ErrorCode::InvalidArgument, "expected {} rows found {} instead",
        next->num_rows(), props->num_rows());
  }

  int last = next->num_columns();

  for (int i = 0, n = props->num_columns(); i < n; i++) {
    std::shared_ptr<arrow::Field> field = props->field(i);
    int current_col = -1;

    auto prop_info_it = std::find_if(
        prop_state->begin(), prop_state->end(),
        [&](const tsuba::PropStorageInfo& psi) {
          return field->name() == psi.name();
        });
    if (prop_info_it == prop_state->end()) {
      prop_info_it = prop_state->insert(
          prop_info_it, tsuba::PropStorageInfo(field->name(), field->type()));
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
        tsuba::ErrorCode::Exists, "column names are not distinct: {}",
        fmt::join(next->schema()->field_names(), ", "));
  }

  *to_update = next;

  return written_prop_names;
}

katana::Result<void>
AddProperties(
    const std::shared_ptr<arrow::Table>& props,
    std::shared_ptr<arrow::Table>* to_update,
    std::vector<tsuba::PropStorageInfo>* prop_state) {
  for (const auto& field : props->fields()) {
    // Column names are not sorted, but assumed to be less than 100s
    auto prop_info_it = std::find_if(
        prop_state->begin(), prop_state->end(),
        [&](const tsuba::PropStorageInfo& psi) {
          return field->name() == psi.name();
        });

    if (prop_info_it != prop_state->end()) {
      return KATANA_ERROR(
          tsuba::ErrorCode::Exists, "column names are not distinct");
    }
  }
  auto written_prop_names =
      KATANA_CHECKED(UpsertProperties(props, to_update, prop_state));

  // TODO (yige): return write set to TxnContext
  return katana::ResultSuccess();
}

katana::Result<void>
EnsureTypeLoaded(const katana::Uri& rdg_dir, tsuba::PropStorageInfo* psi) {
  if (!psi->type()) {
    auto reader = KATANA_CHECKED(tsuba::ParquetReader::Make());
    KATANA_LOG_ASSERT(psi->IsAbsent());
    std::shared_ptr<arrow::Schema> schema =
        KATANA_CHECKED(reader->GetSchema(rdg_dir.Join(psi->path())));
    psi->set_type(schema->field(0)->type());
  }
  return katana::ResultSuccess();
}

tsuba::PropStorageInfo*
find_prop_info(
    const std::string& name, std::vector<tsuba::PropStorageInfo>* prop_infos) {
  auto it = std::find_if(
      prop_infos->begin(), prop_infos->end(),
      [&name](tsuba::PropStorageInfo& psi) { return psi.name() == name; });
  if (it == prop_infos->end()) {
    return nullptr;
  }

  return &(*it);
}
}  // namespace

namespace tsuba {

katana::Result<void>
RDGCore::AddPartitionMetadataArray(const std::shared_ptr<arrow::Table>& props) {
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
RDGCore::AddNodeProperties(const std::shared_ptr<arrow::Table>& props) {
  return AddProperties(
      props, &node_properties_, &part_header_.node_prop_info_list());
}

katana::Result<void>
RDGCore::AddEdgeProperties(const std::shared_ptr<arrow::Table>& props) {
  return AddProperties(
      props, &edge_properties_, &part_header_.edge_prop_info_list());
}

katana::Result<void>
RDGCore::UpsertNodeProperties(
    const std::shared_ptr<arrow::Table>& props, tsuba::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG_ASSERT(txn_ctx != nullptr);
  auto written_prop_names = KATANA_CHECKED(UpsertProperties(
      props, &node_properties_, &part_header_.node_prop_info_list()));
  // store write properties into transaction context
  txn_ctx->InsertNodePropertyWrite(std::move(written_prop_names));

  return katana::ResultSuccess();
}

katana::Result<void>
RDGCore::UpsertEdgeProperties(
    const std::shared_ptr<arrow::Table>& props, tsuba::TxnContext* txn_ctx) {
  KATANA_LOG_DEBUG_ASSERT(txn_ctx != nullptr);
  auto written_prop_names = KATANA_CHECKED(UpsertProperties(
      props, &edge_properties_, &part_header_.edge_prop_info_list()));
  // store write properties into transaction context
  txn_ctx->InsertEdgePropertyWrite(std::move(written_prop_names));

  return katana::ResultSuccess();
}

katana::Result<void>
RDGCore::EnsureNodeTypesLoaded() {
  if (rdg_dir_.empty()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument,
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
RDGCore::EnsureEdgeTypesLoaded() {
  if (rdg_dir_.empty()) {
    return KATANA_ERROR(
        tsuba::ErrorCode::InvalidArgument,
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
RDGCore::InitEmptyProperties() {
  std::vector<std::shared_ptr<arrow::Array>> empty;
  node_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
  edge_properties_ = arrow::Table::Make(arrow::schema({}), empty, 0);
}

bool
RDGCore::Equals(const RDGCore& other) const {
  // Assumption: all RDGTopology objeccts in topology_manger_ and other.topology_manager_
  // are bound to their files
  return topology_manager_.Equals(other.topology_manager_) &&
         node_properties_->Equals(*other.node_properties_, true) &&
         edge_properties_->Equals(*other.edge_properties_, true);

  return true;
}

katana::Result<void>
RDGCore::RemoveNodeProperty(int i) {
  auto field = node_properties_->field(i);
  node_properties_ = KATANA_CHECKED(node_properties_->RemoveColumn(i));

  return part_header_.RemoveNodeProperty(field->name());
}

katana::Result<void>
RDGCore::RemoveEdgeProperty(int i) {
  auto field = edge_properties_->field(i);
  edge_properties_ = KATANA_CHECKED(edge_properties_->RemoveColumn(i));

  return part_header_.RemoveEdgeProperty(field->name());
}

PropStorageInfo*
RDGCore::find_node_prop_info(const std::string& name) {
  return find_prop_info(name, &part_header().node_prop_info_list());
}
PropStorageInfo*
RDGCore::find_edge_prop_info(const std::string& name) {
  return find_prop_info(name, &part_header().edge_prop_info_list());
}
PropStorageInfo*
RDGCore::find_part_prop_info(const std::string& name) {
  return find_prop_info(name, &part_header().part_prop_info_list());
}

}  // namespace tsuba
