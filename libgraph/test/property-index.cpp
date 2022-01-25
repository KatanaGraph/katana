#include <arrow/api.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include "TestTypedPropertyGraph.h"
#include "katana/EntityIndex.h"
#include "katana/Logging.h"
#include "katana/Properties.h"
#include "katana/SharedMemSys.h"

template <typename node_or_edge>
struct NodeOrEdge {
  static katana::Result<katana::EntityIndex<node_or_edge>*> MakeIndex(
      katana::PropertyGraph* pg, const std::string& column_name);
  static katana::Result<void> AddProperties(
      katana::PropertyGraph* pg, std::shared_ptr<arrow::Table> properties,
      katana::TxnContext* txn_ctx);
  static size_t num_entities(katana::PropertyGraph* pg);
};

using Node = NodeOrEdge<katana::GraphTopology::Node>;
using Edge = NodeOrEdge<katana::GraphTopology::Edge>;

template <>
katana::Result<katana::EntityIndex<katana::GraphTopology::Node>*>
Node::MakeIndex(katana::PropertyGraph* pg, const std::string& column_name) {
  auto result = pg->MakeNodeIndex(column_name);
  if (!result) {
    return result.error();
  }

  for (const auto& index : pg->node_indexes()) {
    if (index->column_name() == column_name) {
      return index.get();
    }
  }

  return KATANA_ERROR(katana::ErrorCode::NotFound, "Created index not found");
}

template <>
katana::Result<katana::EntityIndex<katana::GraphTopology::Edge>*>
Edge::MakeIndex(katana::PropertyGraph* pg, const std::string& column_name) {
  auto result = pg->MakeEdgeIndex(column_name);
  if (!result) {
    return result.error();
  }

  for (const auto& index : pg->edge_indexes()) {
    if (index->column_name() == column_name) {
      return index.get();
    }
  }

  return KATANA_ERROR(katana::ErrorCode::NotFound, "Created index not found");
}

template <>
size_t
Node::num_entities(katana::PropertyGraph* pg) {
  return pg->NumNodes();
}

template <>
size_t
Edge::num_entities(katana::PropertyGraph* pg) {
  return pg->NumEdges();
}

template <>
katana::Result<void>
Node::AddProperties(
    katana::PropertyGraph* pg, std::shared_ptr<arrow::Table> properties,
    katana::TxnContext* txn_ctx) {
  return pg->AddNodeProperties(properties, txn_ctx);
}

template <>
katana::Result<void>
Edge::AddProperties(
    katana::PropertyGraph* pg, std::shared_ptr<arrow::Table> properties,
    katana::TxnContext* txn_ctx) {
  return pg->AddEdgeProperties(properties, txn_ctx);
}

template <typename c_type>
std::shared_ptr<arrow::Table>
CreatePrimitiveProperty(
    const std::string& name, bool uniform, size_t num_rows) {
  using BuilderType = typename arrow::CTypeTraits<c_type>::BuilderType;
  using ArrowType = typename arrow::CTypeTraits<c_type>::ArrowType;

  BuilderType builder;
  for (size_t i = 0; i < num_rows; ++i) {
    if (uniform) {
      KATANA_LOG_ASSERT(builder.Append(42).ok());
    } else {
      KATANA_LOG_ASSERT(builder.Append(i * 2 + 42).ok());
    }
  }
  std::vector<std::shared_ptr<arrow::Array>> chunks(1);
  KATANA_LOG_ASSERT(builder.Finish(&chunks[0]).ok());
  return arrow::Table::Make(
      arrow::schema({arrow::field(name, std::make_shared<ArrowType>())}),
      {std::make_shared<arrow::ChunkedArray>(chunks)});
}

std::shared_ptr<arrow::Table>
CreateStringProperty(const std::string& name, bool uniform, size_t num_rows) {
  using BuilderType = arrow::LargeStringBuilder;
  using ArrowType = arrow::LargeStringType;

  BuilderType builder;
  char str[] = "aaaa";
  for (size_t i = 0; i < num_rows; ++i) {
    KATANA_LOG_ASSERT(
        builder.Append(std::string(static_cast<char*>(str))).ok());
    if (!uniform) {
      for (int i = 3; i >= 0; --i) {
        str[i] += 2;
        if (str[i] <= 'z') {
          break;
        }
        str[i] = 'a';
      }
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> chunks(1);
  KATANA_LOG_ASSERT(builder.Finish(&chunks[0]).ok());
  return arrow::Table::Make(
      arrow::schema({arrow::field(name, std::make_shared<ArrowType>())}),
      {std::make_shared<arrow::ChunkedArray>(chunks)});
}

template <typename node_or_edge, typename DataType>
void
TestPrimitiveIndex(size_t num_nodes, size_t line_width) {
  using IndexType = katana::PrimitiveEntityIndex<node_or_edge, DataType>;
  using ArrayType = typename arrow::CTypeTraits<DataType>::ArrayType;

  LinePolicy policy{line_width};

  katana::TxnContext txn_ctx;

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<DataType>(num_nodes, 0, &policy, &txn_ctx);

  std::shared_ptr<arrow::Table> uniform_prop =
      CreatePrimitiveProperty<DataType>(
          "uniform", true, NodeOrEdge<node_or_edge>::num_entities(g.get()));
  std::shared_ptr<arrow::Table> nonuniform_prop =
      CreatePrimitiveProperty<DataType>(
          "nonuniform", false, NodeOrEdge<node_or_edge>::num_entities(g.get()));
  KATANA_LOG_ASSERT(
      NodeOrEdge<node_or_edge>::AddProperties(g.get(), uniform_prop, &txn_ctx));
  KATANA_LOG_ASSERT(NodeOrEdge<node_or_edge>::AddProperties(
      g.get(), nonuniform_prop, &txn_ctx));

  auto uniform_index_result =
      NodeOrEdge<node_or_edge>::MakeIndex(g.get(), "uniform");
  KATANA_LOG_VASSERT(
      uniform_index_result, "Could not create index: {}",
      uniform_index_result.error());
  auto nonuniform_index_result =
      NodeOrEdge<node_or_edge>::MakeIndex(g.get(), "nonuniform");
  KATANA_LOG_VASSERT(
      nonuniform_index_result, "Could not create index: {}",
      nonuniform_index_result.error());

  auto* uniform_index = static_cast<IndexType*>(uniform_index_result.value());
  auto* nonuniform_index =
      static_cast<IndexType*>(nonuniform_index_result.value());
  (void)nonuniform_index;

  // The uniform index has every value == (c_type)42.
  auto it = uniform_index->Find(0);
  KATANA_LOG_VASSERT(
      it == uniform_index->end(), "Found expected-missing value {}: {}", 0,
      *it);

  // Searching for '42' should get every item.
  size_t num_entities = NodeOrEdge<node_or_edge>::num_entities(g.get());
  std::vector<bool> found(num_entities, false);
  for (it = uniform_index->Find(42); it != uniform_index->end(); ++it) {
    node_or_edge id = *it;
    KATANA_LOG_VASSERT(id < num_entities, "Invalid id: {}", id);
    KATANA_LOG_VASSERT(!found[id], "Duplicate id: {}", id);
    found[id] = true;
  }
  for (node_or_edge id = 0; id < num_entities; ++id) {
    KATANA_LOG_VASSERT(found[id], "Not in index: {}", id);
  }

  // The non-uniform index starts at 42 and increases by 2.
  auto typed_prop =
      std::static_pointer_cast<ArrayType>(nonuniform_prop->column(0)->chunk(0));
  it = nonuniform_index->Find(43);
  KATANA_LOG_ASSERT(it == nonuniform_index->end());
  it = nonuniform_index->LowerBound(43);
  KATANA_LOG_ASSERT(it != nonuniform_index->end());
  KATANA_LOG_ASSERT(typed_prop->Value(*it) == 44);
  it = nonuniform_index->UpperBound(44);
  KATANA_LOG_ASSERT(it != nonuniform_index->end());
  KATANA_LOG_ASSERT(typed_prop->Value(*it) == 46);
}

template <typename node_or_edge>
void
TestStringIndex(size_t num_nodes, size_t line_width) {
  using IndexType = katana::StringEntityIndex<node_or_edge>;
  using ArrayType = arrow::LargeStringArray;

  LinePolicy policy{line_width};

  katana::TxnContext txn_ctx;

  std::unique_ptr<katana::PropertyGraph> g =
      MakeFileGraph<int>(num_nodes, 0, &policy, &txn_ctx);

  std::shared_ptr<arrow::Table> uniform_prop = CreateStringProperty(
      "uniform", true, NodeOrEdge<node_or_edge>::num_entities(g.get()));
  std::shared_ptr<arrow::Table> nonuniform_prop = CreateStringProperty(
      "nonuniform", false, NodeOrEdge<node_or_edge>::num_entities(g.get()));
  KATANA_LOG_ASSERT(
      NodeOrEdge<node_or_edge>::AddProperties(g.get(), uniform_prop, &txn_ctx));
  KATANA_LOG_ASSERT(NodeOrEdge<node_or_edge>::AddProperties(
      g.get(), nonuniform_prop, &txn_ctx));

  auto uniform_index_result =
      NodeOrEdge<node_or_edge>::MakeIndex(g.get(), "uniform");
  KATANA_LOG_VASSERT(
      uniform_index_result, "Could not create index: {}",
      uniform_index_result.error());
  auto nonuniform_index_result =
      NodeOrEdge<node_or_edge>::MakeIndex(g.get(), "nonuniform");
  KATANA_LOG_VASSERT(
      nonuniform_index_result, "Could not create index: {}",
      nonuniform_index_result.error());

  auto* uniform_index = static_cast<IndexType*>(uniform_index_result.value());
  auto* nonuniform_index =
      static_cast<IndexType*>(nonuniform_index_result.value());

  // The uniform index has every value == "aaaa".
  auto it = uniform_index->Find("aaaq");
  KATANA_LOG_VASSERT(
      it == uniform_index->end(), "Found expected-missing value aaaq: {}", *it);

  // Searching for "aaaa" should get every item.
  size_t num_entities = NodeOrEdge<node_or_edge>::num_entities(g.get());
  std::vector<bool> found(num_entities, false);
  for (it = uniform_index->Find("aaaa"); it != uniform_index->end(); ++it) {
    node_or_edge id = *it;
    KATANA_LOG_VASSERT(id < num_entities, "Invalid id: {}", id);
    KATANA_LOG_VASSERT(!found[id], "Duplicate id: {}", id);
    found[id] = true;
  }
  for (node_or_edge id = 0; id < num_entities; ++id) {
    KATANA_LOG_VASSERT(found[id], "Not in index: {}", id);
  }

  // The non-uniform index starts at "aaaa" and increases by 2.
  auto typed_prop =
      std::static_pointer_cast<ArrayType>(nonuniform_prop->column(0)->chunk(0));
  it = nonuniform_index->Find("aaaj");
  KATANA_LOG_ASSERT(it == nonuniform_index->end());
  it = nonuniform_index->LowerBound("aaaj");
  KATANA_LOG_ASSERT(it != nonuniform_index->end());
  KATANA_LOG_ASSERT(typed_prop->GetView(*it) == "aaak");
  it = nonuniform_index->UpperBound("aaak");
  KATANA_LOG_ASSERT(it != nonuniform_index->end());
  KATANA_LOG_ASSERT(typed_prop->GetView(*it) == "aaam");
}

int
main() {
  katana::SharedMemSys S;

  TestPrimitiveIndex<katana::GraphTopology::Node, int64_t>(10, 3);
  TestPrimitiveIndex<katana::GraphTopology::Edge, int64_t>(10, 3);
  TestPrimitiveIndex<katana::GraphTopology::Node, double_t>(10, 3);
  TestPrimitiveIndex<katana::GraphTopology::Edge, double_t>(10, 3);

  TestStringIndex<katana::GraphTopology::Node>(10, 3);
  TestStringIndex<katana::GraphTopology::Edge>(10, 3);

  return 0;
}
