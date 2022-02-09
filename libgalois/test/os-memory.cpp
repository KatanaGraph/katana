#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arrow/type.h>

#include "katana/ArrowInterchange.h"
#include "katana/MemoryPolicy.h"
#include "katana/MemorySupervisor.h"
#include "katana/ProgressTracer.h"
#include "katana/PropertyManager.h"
#include "katana/Random.h"
#include "katana/TextTracer.h"
#include "katana/Time.h"

// Create a memory-intensive workload that stresses the property manager, property
// cache, and memory supervisor so we can develop useful memory policies.

namespace {
// Globals
std::vector<katana::Uri> prop_names;
std::unordered_map<
    katana::Uri, std::shared_ptr<arrow::Table>, katana::Uri::Hash>
    name_to_table;

uint32_t rand_seed0 = 0x12345678;
uint32_t rand_seed1 = 0x9ABCDEFE;

}  // namespace

// Limit our memory use to so we don't blow out the machine
// Unfortunately, it manifests as failed allocations, not early OOM killing
void
Limit(uint64_t max) {
  struct rlimit rlim;
  rlim.rlim_cur = max;
  rlim.rlim_max = max;
  int res = setrlimit(RLIMIT_AS, &rlim);
  if (res != 0) {
    perror("setrlimit failed");
  }
}

katana::count_t
MakeSize(katana::count_t log_size) {
  return (static_cast<katana::count_t>(1) << (30 + log_size));
}

// Each property name starts with a number i, which means
// a property of size 2^i GB.  0<=i<=5
void
MakePropertyNames(katana::RandGenerator& gen, katana::count_t goal_wss) {
  katana::count_t prop_size{};

  // Pick 0..5 inclusive for sizes 1GB..32GB
  std::geometric_distribution<int> geo_dist(0.5);
  while ((prop_size + MakeSize(0)) < goal_wss) {
    int log_size = geo_dist(gen);
    while (log_size > 5 || (prop_size + MakeSize(log_size) >= goal_wss)) {
      log_size = geo_dist(gen);
    }
    static int64_t unique = 0;
    std::string prop_name = fmt::format("{}_{}", log_size, unique++);
    prop_size += MakeSize(log_size);
    fmt::print(
        "Prop {:2} size {} ({} of {})\n", (unique - 1),
        katana::ToGB(MakeSize(log_size)), katana::ToGB(prop_size),
        katana::ToGB(goal_wss));
    auto res = katana::Uri::Make(prop_name);
    KATANA_LOG_ASSERT(res);
    prop_names.push_back(res.value());
  }
}

std::shared_ptr<arrow::Table>
GenArrowTable(katana::count_t size, const katana::Uri& prop_name) {
  auto num_entries = size / sizeof(int64_t);
  arrow::Int64Builder prop_builder;
  if (auto status = prop_builder.Resize(num_entries); !status.ok()) {
    KATANA_LOG_FATAL("arrow error: {}", status);
  }
  for (uint64_t i = 0; i < num_entries; ++i) {
    prop_builder.UnsafeAppend(i);
  }
  std::shared_ptr<arrow::Int64Array> test_prop;
  if (auto status = prop_builder.Finish(&test_prop); !status.ok()) {
    KATANA_LOG_FATAL("arrow error: {}", status);
  }
  return arrow::Table::Make(
      arrow::schema({std::make_shared<arrow::Field>(
          prop_name.string(), test_prop->type())}),
      {test_prop});
}

void
GetPropertyFromStorage(const katana::Uri& prop_name) {
  int log_size = std::atoi(prop_name.BaseName().c_str());
  auto size = MakeSize(log_size);

  auto table = GenArrowTable(size, prop_name);
  name_to_table[prop_name] = table;
}

void
ExerciseProperties(katana::PropertyManager& manager, katana::count_t goal_wss) {
  std::vector<std::seed_seq::result_type> myseed{rand_seed0, rand_seed1};
  auto [gen, seed] = katana::CreateGenerator(myseed);
  MakePropertyNames(gen, goal_wss);
  while (true) {
    // Simulate access to a set of properties (distribution is inclusive)
    std::uniform_int_distribution<size_t> num_props_picker(
        1, prop_names.size());
    std::uniform_int_distribution<size_t> prop_picker(0, prop_names.size() - 1);
    auto num_props = num_props_picker(gen);
    std::unordered_set<katana::Uri, katana::Uri::Hash> active_prop_names;
    while (active_prop_names.size() < num_props) {
      auto prop_name = prop_names[prop_picker(gen)];
      active_prop_names.insert(prop_name);
    }
    for (const auto& active : active_prop_names) {
      std::shared_ptr<arrow::Table> table = manager.GetProperty(active);
      if (table == nullptr) {
        GetPropertyFromStorage(active);
        table = name_to_table[active];
        KATANA_LOG_ASSERT(table);
        manager.PropertyLoadedCallback(table);
        fmt::print(
            "load {} from storage {} GB\n", active,
            katana::ToGB(katana::ApproxTableMemUse(table)));
      }
      // Access the property
      auto ca = table->column(0);
      auto arr = ca->chunk(0);
      KATANA_LOG_ASSERT(arr->IsValid(0));
      KATANA_LOG_ASSERT(arr->IsValid(arr->length() - 1));
      KATANA_LOG_ASSERT(arr->IsValid(arr->length() / 2));
    }
    for (const auto& active : active_prop_names) {
      manager.PutProperty(active, std::move(name_to_table[active]));
    }
  }
}

void
Run() {
  // Set up property manager and memory supervisor
  katana::PropertyManager property_manager;
  uint64_t physical = katana::MemorySupervisor::GetTotalSystemMemory();
  katana::MemorySupervisor::Get().SetPolicy(
      std::make_unique<katana::MemoryPolicyPerformance>());
  katana::MemorySupervisor::Get().Register(&property_manager);

  // Goal working set size
  // Occupy a lot of memory, but it should fit
  katana::count_t goal_wss = physical * 0.89;
  ExerciseProperties(property_manager, goal_wss);
}

int
main(int, char**) {
  katana::ProgressTracer::Set(katana::TextTracer::Make());

  Run();

  return 0;
}
