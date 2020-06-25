#include "galois/Logging.h"
#include "galois/graphs/PropertyFileGraph.h"

#include <cstdlib> // mkdtemp

#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

std::shared_ptr<arrow::Table> MakeTable(const std::string& name,
                                        const std::vector<int32_t>& data) {
  arrow::NumericBuilder<arrow::Int32Type> builder;

  auto append_status = builder.AppendValues(data.begin(), data.end());
  GALOIS_LOG_ASSERT(append_status.ok());

  std::shared_ptr<arrow::Array> array;

  auto finish_status = builder.Finish(&array);
  GALOIS_LOG_ASSERT(finish_status.ok());

  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field(name, arrow::int32())});

  return arrow::Table::Make(schema, {array});
}

void TestRoundTrip() {
  constexpr int test_length = 10;

  auto g = std::make_unique<galois::graphs::PropertyFileGraph>();

  std::vector<int32_t> data;
  {
    int32_t value = 0;
    for (int i = 0; i < test_length; ++i) {
      data.emplace_back(value++);
    }
  }

  std::shared_ptr<arrow::Table> node_table = MakeTable("node-name", data);

  auto add_node_result = g->AddNodeProperties(node_table);
  GALOIS_LOG_ASSERT(add_node_result);

  std::shared_ptr<arrow::Table> edge_table = MakeTable("edge-name", data);

  auto add_edge_result = g->AddEdgeProperties(edge_table);
  GALOIS_LOG_ASSERT(add_edge_result);

  const std::string tmpl = "/tmp/propertyfilegraph-XXXXXX";
  std::vector<char> my_tmpl(tmpl.begin(), tmpl.end());
  my_tmpl.emplace_back('\0');
  char* temp_dir = mkdtemp(my_tmpl.data());
  if (temp_dir == nullptr) {
    perror("mkdtemp");
  }
  GALOIS_LOG_ASSERT(temp_dir != nullptr);

  std::string meta_file{temp_dir};
  meta_file += "/meta";

  auto write_result = g->Write(meta_file);

  GALOIS_LOG_WARN("creating temp file {}", meta_file);

  if (!write_result) {
    fs::remove_all(temp_dir);
    GALOIS_LOG_FATAL("writing result: {}", write_result.error());
  }

  outcome::std_result<std::shared_ptr<galois::graphs::PropertyFileGraph>>
      make_result = galois::graphs::PropertyFileGraph::Make(meta_file);
  fs::remove_all(temp_dir);
  if (!make_result) {
    GALOIS_LOG_FATAL("making result: {}", make_result.error());
  }

  std::shared_ptr<galois::graphs::PropertyFileGraph> g2 = make_result.value();

  std::vector<std::shared_ptr<arrow::ChunkedArray>> node_properties =
      g2->NodeProperties();
  std::vector<std::shared_ptr<arrow::ChunkedArray>> edge_properties =
      g2->EdgeProperties();

  GALOIS_LOG_ASSERT(node_properties.size() == 1);
  GALOIS_LOG_ASSERT(edge_properties.size() == 1);

  std::shared_ptr<arrow::ChunkedArray> node_property = node_properties[0];
  std::shared_ptr<arrow::ChunkedArray> edge_property = edge_properties[0];

  GALOIS_LOG_ASSERT(node_property->length() == test_length);
  GALOIS_LOG_ASSERT(node_property->num_chunks() == 1);
  GALOIS_LOG_ASSERT(edge_property->length() == test_length);
  GALOIS_LOG_ASSERT(edge_property->num_chunks() == 1);

  {
    std::shared_ptr<arrow::Array> node_chunk = node_property->chunk(0);
    std::shared_ptr<arrow::Array> edge_chunk = edge_property->chunk(0);
    auto node_data = std::static_pointer_cast<arrow::Int32Array>(node_chunk);
    auto edge_data = std::static_pointer_cast<arrow::Int32Array>(edge_chunk);

    int32_t value = 0;
    for (int i = 0; i < test_length; ++i) {
      GALOIS_LOG_ASSERT(!node_data->IsNull(i) && node_data->Value(i) == value);
      GALOIS_LOG_ASSERT(!edge_data->IsNull(i) && edge_data->Value(i) == value);
      ++value;
    }
  }
}

int main() {
  TestRoundTrip();

  return 0;
}
