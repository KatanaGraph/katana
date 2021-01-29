#include <cstdlib>
#include <string>

#include <arrow/type.h>
#include <boost/algorithm/string/predicate.hpp>

#include "katana/DistMemSys.h"
#include "katana/Network.h"
#include "katana/PropertyFileGraph.h"
#include "llvm/Support/CommandLine.h"
#include "tsuba/RDGPrefix.h"
#include "tsuba/tsuba.h"

namespace cll = llvm::cl;

static cll::opt<std::string> src_uri(
    cll::Positional, cll::desc("<graph file>"), cll::Required);

std::string
TypeAsString(
    std::shared_ptr<arrow::Schema> schema, const std::string& prop_name) {
  auto field = schema->GetFieldByName(prop_name);
  return field->type()->name();
}
void
PrintNames(
    std::shared_ptr<arrow::Schema> schema, std::vector<std::string> names) {
  std::sort(
      names.begin(), names.end(),
      [](const std::string& a, const std::string& b) {
        return boost::ilexicographical_compare<std::string, std::string>(a, b);
      });
  for (size_t i = 0, end = names.size(); i < end; i += 4) {
    switch (end - i) {
    case 1: {
      fmt::print("{:15}:{:8}\n", names[i], TypeAsString(schema, names[i]));
      break;
    }
    case 2: {
      fmt::print(
          "{:15}:{:8} {:15}:{:8}\n", names[i], TypeAsString(schema, names[i]),
          names[i + 1], TypeAsString(schema, names[i + 1]));
      break;
    }
    case 3: {
      fmt::print(
          "{:15}:{:8} {:15}:{:8} {:15}:{:8}\n", names[i],
          TypeAsString(schema, names[i]), names[i + 1],
          TypeAsString(schema, names[i + 1]), names[i + 2],
          TypeAsString(schema, names[i + 2]));
      break;
    }
    default: {
      fmt::print(
          "{:15}:{:8} {:15}:{:8} {:15}:{:8} {:15}:{:8}\n", names[i],
          TypeAsString(schema, names[i]), names[i + 1],
          TypeAsString(schema, names[i + 1]), names[i + 2],
          TypeAsString(schema, names[i + 2]), names[i + 3],
          TypeAsString(schema, names[i + 3]));
      break;
    }
    }
  }
}
// We could find out this information without loading the graph,
// but that would take more effort
void
PrintDist(const std::string& src_uri, int this_host, int num_hosts) {
  auto res = katana::PropertyFileGraph::Make(src_uri);
  if (!res) {
    KATANA_LOG_FATAL("error loading graph: {}", res.error());
  }
  auto g = std::move(res.value());

  fmt::print("{:16} : {:2} : {:>7}\n", "Nodes", this_host, g->num_nodes());
  fmt::print("{:16} : {:2} : {:>7}\n", "Edges", this_host, g->num_edges());

  auto masters_range = g->masters();
  auto outgoing_mirrors_range = g->outgoing_mirrors();
  auto incoming_mirrors_range = g->incoming_mirrors();
  fmt::print(
      "{:16} : {:2} : {:>7}\n", "Masters", this_host,
      std::distance(masters_range.begin(), masters_range.end()));
  fmt::print(
      "{:16} : {:2} : {:>7}\n", "Outgoing mirrors", this_host,
      std::distance(
          outgoing_mirrors_range.begin(), outgoing_mirrors_range.end()));
  fmt::print(
      "{:16} : {:2} : {:>7}\n", "Incoming mirrors", this_host,
      std::distance(
          incoming_mirrors_range.begin(), incoming_mirrors_range.end()));

  if (this_host == (num_hosts - 1)) {
    fmt::print(
        "{:16} : {:>2}\n", "Node Properties", g->GetNodePropertyNames().size());
    PrintNames(g->node_schema(), g->GetNodePropertyNames());

    fmt::print(
        "{:16} : {:>2}\n", "Edge Properties", g->GetEdgePropertyNames().size());
    PrintNames(g->edge_schema(), g->GetEdgePropertyNames());
  }
}

tsuba::RDGPrefix
OpenPrefix(const std::string& src_uri) {
  auto open_res = tsuba::Open(src_uri, tsuba::kReadOnly);
  if (!open_res) {
    KATANA_LOG_FATAL("Open RDG failed: {}", open_res.error());
  }
  auto prefix_res = tsuba::RDGPrefix::Make(open_res.value());
  if (!prefix_res) {
    KATANA_LOG_FATAL("RDGPrefix make failed: {}", prefix_res.error());
  }
  return std::move(prefix_res.value());
}

int
main(int argc, char** argv) {
  llvm::cl::ParseCommandLineOptions(argc, argv);
  // build this after CLI parsing since it might cause MPI to fail
  auto dist_mem_sys = std::make_unique<katana::DistMemSys>();
  auto& net = katana::getSystemNetworkInterface();
  auto num_hosts = net.Num;
  auto this_host = net.ID;

  if (this_host == 0) {
    fmt::print("{:16} : {}\n", "URI", src_uri);
  }

  if (num_hosts == 1) {
    auto prefix = OpenPrefix(src_uri);
    if (this_host == 0) {
      fmt::print("{:16} : {}\n", "Nodes", prefix.num_nodes());
      fmt::print("{:16} : {}\n", "Edges", prefix.num_edges());
      fmt::print("{:16} : {}\n", "Version", prefix.version());
    }
  } else {
    PrintDist(src_uri, this_host, num_hosts);
  }
  // dist_mem_sys->Fini()  prints Stat output, so do our own shutdown
  dist_mem_sys->Fini(false);

  return 0;
}
