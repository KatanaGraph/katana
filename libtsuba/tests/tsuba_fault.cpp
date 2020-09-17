// During a graph mutation, crash horribly
#include <ctime>

#include "bench_utils.h"
#include "galois/Logging.h"
#include "galois/Random.h"
#include "tsuba/FaultTest.h"
#include "tsuba/FileView.h"
#include "tsuba/RDG.h"
#include "tsuba/file.h"
#include "tsuba/tsuba.h"

std::string src_uri{};
bool opt_print{false};
bool opt_validate{false};
int32_t count{1};              // By default do 1 thing
int32_t node_property_num{0};  // Which node property
float independent_failure_probability{0.0f};
uint64_t run_length{UINT64_C(0)};
std::string prog_name = "tsuba_fault";
std::string usage_msg =
    "Usage: {} <RDG URI>\n"
    "  [-c] count (default=1)\n"
    "  [-n] node property number (default=0)\n"
    "  [-i] Independent failure probability (default=0.0, max=0.5)\n"
    "  [-r] Execute this many PtPs, then die (starts at 1)\n"
    "  [-v] validate graph\n"
    "  [-p] print graph\n"
    "  [-h] usage message\n"
    "  when run with just -c, it will mutate & store the graph count times "
    "with no errors\n";

void
parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "c:n:i:r:vph")) != -1) {
    switch (c) {
    case 'c':
      count = std::atoi(optarg);
      break;
    case 'n':
      node_property_num = std::atoi(optarg);
      break;
    case 'i':
      independent_failure_probability = std::atof(optarg);
      break;
    case 'r': {
      char* p_end{nullptr};
      run_length = std::strtoul(optarg, &p_end, 10);
      if (optarg == p_end) {
        fmt::print(stderr, "Can't parse -r argument (run length)\n");
        fmt::print(stderr, usage_msg, prog_name);
        exit(EXIT_FAILURE);
      }
    } break;
    case 'v':
      opt_validate = true;
      break;
    case 'p':
      opt_print = true;
      break;
    case 'h':
      fmt::print(stderr, usage_msg, prog_name);
      exit(0);
      break;
    default:
      fmt::print(stderr, "Bad option {}\n", (char)c);
      fmt::print(stderr, usage_msg, prog_name);
      exit(EXIT_FAILURE);
    }
  }

  // TODO: Validate paths
  auto index = optind;
  if (index >= argc) {
    fmt::print(stderr, "{} requires property graph URI argument\n", prog_name);
    exit(EXIT_FAILURE);
  }
  src_uri = argv[index++];
}

/******************************************************************************/
// Utility functions to print tables
static void
PrintInts(std::shared_ptr<arrow::ChunkedArray> arr) {
  for (auto chunk = 0; chunk < arr->num_chunks(); ++chunk) {
    auto int_arr =
        std::static_pointer_cast<arrow::Int64Array>(arr->chunk(chunk));
    for (auto i = 0; i < int_arr->length(); ++i) {
      fmt::print("  {:d}: {:d}\n", i, int_arr->Value(i));
    }
  }
}
static void
PrintStrings(std::shared_ptr<arrow::ChunkedArray> arr) {
  for (auto chunk = 0; chunk < arr->num_chunks(); ++chunk) {
    auto str_arr =
        std::static_pointer_cast<arrow::StringArray>(arr->chunk(chunk));
    for (auto i = 0; i < arr->length(); ++i) {
      fmt::print("  {:d}: {}\n", i, str_arr->GetString(i));
    }
  }
}

// TODO(witchel) Should use table->ToString();
static void
PrintTable(std::shared_ptr<arrow::Table> table) {
  const auto& schema = table->schema();
  for (int i = 0, n = schema->num_fields(); i < n; i++) {
    fmt::print("Schema {:d} {}\n", i, schema->field(i)->name());
  }
  fmt::print(
      "Table num col {:d} num row {:d} col 0 chunks {:d}\n",
      table->num_columns(), table->num_rows(), table->column(0)->num_chunks());
}

/******************************************************************************/
// Construct arrow tables, which are node & edge properties
// Schemas
std::shared_ptr<arrow::Schema>
int64_schema(const std::string& prop_name) {
  auto field = std::make_shared<arrow::Field>(
      prop_name.c_str(), std::make_shared<arrow::Int64Type>());
  auto schema =
      std::make_shared<arrow::Schema>(arrow::Schema({field}, nullptr));
  return schema;
}

std::shared_ptr<arrow::Schema>
string_schema() {
  auto field = std::make_shared<arrow::Field>(
      "str", std::make_shared<arrow::StringType>());
  auto schema =
      std::make_shared<arrow::Schema>(arrow::Schema({field}, nullptr));
  return schema;
}

// Tables
std::shared_ptr<arrow::Table>
MakeNodePropTable(
    std::vector<int64_t> node_props, const std::string& node_prop_name) {
  arrow::Int64Builder builder;
  arrow::Status status;

  for (const auto& node_prop : node_props) {
    status = builder.Append(node_prop);
    GALOIS_LOG_ASSERT(status.ok());
  }

  std::shared_ptr<arrow::Int64Array> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab =
      arrow::Table::Make(int64_schema(node_prop_name), {arr});
  return tab;
}

std::shared_ptr<arrow::Table>
MakeStrTable(std::vector<int64_t> edge_lens) {
  arrow::StringBuilder builder;
  arrow::Status status;

  for (const auto& edge_len : edge_lens) {
    if (edge_len == 0L) {
      // Encode 0 with an empty string
      status = builder.Append(std::string());
    } else if (edge_len > 0L) {
      // Encode positives unary with 'a'
      status = builder.Append(std::string(edge_len, 'a'));
    } else {
      // Encode negatives unary with 'b'
      status = builder.Append(std::string(-edge_len, 'b'));
    }
    GALOIS_LOG_ASSERT(status.ok());
  }
  std::shared_ptr<arrow::StringArray> arr;
  status = builder.Finish(&arr);
  GALOIS_LOG_ASSERT(status.ok());
  std::shared_ptr<arrow::Table> tab =
      arrow::Table::Make(string_schema(), {arr});
  return tab;
}

std::vector<int64_t>
GenRandVec(uint64_t size, int64_t min, int64_t max) {
  std::vector<int64_t> data(size);
  std::generate(data.begin(), data.end(), [=]() {
    return galois::RandomUniformInt(min, max);
  });
  return data;
}

void
MutateGraph(tsuba::RDG& rdg) {
  // Nodes
  {
    GALOIS_LOG_VASSERT(
        node_property_num < rdg.node_table_->num_columns(),
        "Node property number is {:d} but only {:d} properties",
        node_property_num, rdg.node_table_->num_columns());
    auto col = rdg.node_table_->column(node_property_num);
    auto node_prop_name =
        rdg.node_table_->schema()->field(node_property_num)->name();
    std::vector<int64_t> col_values =
        GenRandVec(col->length() - 1, -1000000, 1000000);
    // Sum to 0
    col_values.push_back(
        0L - std::accumulate(col_values.begin(), col_values.end(), 0L));
    if (auto res = rdg.DropNodeProperty(node_property_num); !res) {
      GALOIS_LOG_FATAL(
          "DropNodeProperty {:d} {}", node_property_num, res.error());
    }
    auto node_prop_tab = MakeNodePropTable(col_values, node_prop_name);
    if (auto res = rdg.AddNodeProperties(node_prop_tab); !res) {
      GALOIS_LOG_FATAL("AddNodeProperties {}", res.error());
    }
  }
  // Edges
  {
    auto arr = rdg.edge_table_->GetColumnByName("str");
    std::vector<int64_t> edge_lens = GenRandVec(arr->length() - 1, -100, 100);
    // Sum to 0
    edge_lens.push_back(
        0L - std::accumulate(edge_lens.begin(), edge_lens.end(), 0L));
    if (auto res = rdg.DropEdgeProperty(0); !res) {
      GALOIS_LOG_FATAL("DropEdgeProperty 0 {}", res.error());
    }
    auto edge_lens_tab = MakeStrTable(edge_lens);
    if (auto res = rdg.AddEdgeProperties(edge_lens_tab); !res) {
      GALOIS_LOG_FATAL("AddEdgeProperties 0 {}", res.error());
    }
  }
}

void
ValidateGraph(tsuba::RDG& rdg) {
  // Nodes
  for (auto col_num = 0; col_num < rdg.node_table_->num_columns(); ++col_num) {
    int64_t total = 0L;
    auto arr = rdg.node_table_->column(col_num);
    for (auto chunk = 0; chunk < arr->num_chunks(); ++chunk) {
      auto int_arr =
          std::static_pointer_cast<arrow::Int64Array>(arr->chunk(chunk));
      for (auto i = 0; i < int_arr->length(); ++i) {
        total += int_arr->Value(i);
      }
    }
    GALOIS_LOG_VASSERT(total == 0L, "Total {:d}", total);
  }
  // Edges
  {
    auto arr = rdg.edge_table_->GetColumnByName("str");
    int64_t total = 0L;
    for (auto chunk = 0; chunk < arr->num_chunks(); ++chunk) {
      auto str_arr =
          std::static_pointer_cast<arrow::StringArray>(arr->chunk(chunk));
      for (auto i = 0; i < arr->length(); ++i) {
        std::string str = str_arr->GetString(i);
        auto len = str.length();
        if (len > 0L) {
          if (str[0] == 'a') {
            total += len;
          } else {
            GALOIS_LOG_VASSERT(
                str[0] == 'b', "Bad str {:d}: len {:d} {}", i, len, str);
            total -= len;
          }
        }
      }
    }
    GALOIS_LOG_VASSERT(total == 0L, "Total {:d}", total);
  }
}

galois::Result<tsuba::RDG>
OpenGraph(const std::string& pg_in, uint32_t flags) {
  auto handle_res = tsuba::Open(pg_in, flags);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto handle = handle_res.value();

  auto rdg_res = tsuba::RDG::Load(handle);
  if (!rdg_res) {
    GALOIS_LOG_FATAL("Load rdg error: {}", rdg_res.error());
  }
  return tsuba::RDG(std::move(rdg_res.value()));
}

void
OpenUpdateStore(const std::string& pg_in, uint32_t count) {
  auto handle_res = tsuba::Open(pg_in, tsuba::kReadWrite);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto handle = handle_res.value();

  auto rdg_res = tsuba::RDG::Load(handle);
  if (!rdg_res) {
    GALOIS_LOG_FATAL("Load rdg error: {}", rdg_res.error());
  }
  tsuba::RDG rdg = std::move(rdg_res.value());

  for (auto i = 0U; i < count; ++i) {
    ValidateGraph(rdg);
    MutateGraph(rdg);
    if (auto res = rdg.Store(handle); !res) {
      GALOIS_LOG_FATAL("Store local rdg: {}", res.error());
    }
  }

  if (auto res = tsuba::Close(handle); !res) {
    GALOIS_LOG_FATAL("Close local handle: {}", res.error());
  }
}

void
PrintGraph(const std::string& src_uri) {
  auto handle_res = tsuba::Open(src_uri, tsuba::kReadOnly);
  if (!handle_res) {
    GALOIS_LOG_FATAL("Open rdg: {}", handle_res.error());
  }
  auto handle = handle_res.value();

  auto rdg_res = tsuba::RDG::Load(handle);
  if (!rdg_res) {
    GALOIS_LOG_FATAL("Load rdg from s3: {}", rdg_res.error());
  }
  auto rdg = std::move(rdg_res.value());
  fmt::print("NODE\n");
  PrintTable(rdg.node_table_);
  for (auto i = 0; i < rdg.node_table_->num_columns(); ++i) {
    PrintInts(rdg.node_table_->column(i));
  }

  fmt::print("EDGE\n");
  PrintTable(rdg.edge_table_);
  PrintStrings(rdg.edge_table_->GetColumnByName("str"));
}

int
main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);

  if (opt_print) {
    PrintGraph(src_uri);
    exit(0);
  }

  if (opt_validate) {
    auto open_res = OpenGraph(src_uri, tsuba::kReadOnly);
    if (!open_res) {
      GALOIS_LOG_FATAL("OpenGraph failed: {}", open_res.error());
    }
    tsuba::RDG rdg = std::move(open_res.value());
    ValidateGraph(rdg);
    exit(0);
  }

  if (run_length > UINT64_C(0)) {
    tsuba::internal::FaultTestInit(
        tsuba::internal::FaultMode::RunLength, 0.0f, run_length);
  } else if (independent_failure_probability > 0.0f) {
    tsuba::internal::FaultTestInit(
        tsuba::internal::FaultMode::Independent,
        independent_failure_probability, UINT64_C(0));
  }

  OpenUpdateStore(src_uri, count);

  tsuba::internal::FaultTestReport();
  return 0;
}
