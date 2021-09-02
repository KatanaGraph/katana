#include "katana/Cache.h"

#include <map>
#include <random>

#include "katana/Cache.h"
#include "katana/Logging.h"
#include "katana/Random.h"

enum class NodeEdge { kNode, kEdge };

// This code is replicated from libtsuba::PropertyCache.h to allow testing of
// libsupport::Cache.h (libtsuba depends on libsupport, not the other way around)
struct KATANA_EXPORT PropertyCacheKey {
  NodeEdge node_edge;
  std::string name;
  PropertyCacheKey(NodeEdge _node_edge, const std::string& _name = "")
      : node_edge(_node_edge), name(_name) {}
  bool operator==(const PropertyCacheKey& o) const {
    return node_edge == o.node_edge && name == o.name;
  }
  struct Hash {
    std::size_t operator()(const PropertyCacheKey& k) const {
      using boost::hash_combine;
      using boost::hash_value;

      std::size_t seed = 0;
      hash_combine(seed, hash_value(k.node_edge));
      hash_combine(seed, hash_value(k.name));

      // Return the result.
      return seed;
    }
  };
};

struct CacheValue {
  int64_t a;
  int32_t b;
  uint32_t c;
};
static uint64_t count_evictions{0};

CacheValue
RandomValue() {
  std::uniform_int_distribution<int64_t> dist(
      0, std::numeric_limits<int64_t>::max());
  CacheValue elt;
  elt.a = dist(katana::GetGenerator());
  elt.b = static_cast<int32_t>(elt.a);
  elt.c = static_cast<uint32_t>(elt.a);
  return elt;
}

size_t
BytesInValue(const CacheValue& value) {
  if (value.a & 1) {
    return 5;
  } else {
    return 1;
  }
}

CacheValue
SizeOneValue() {
  auto v = RandomValue();
  v.a = 0;
  return v;
}

CacheValue
SizeFiveValue() {
  auto v = RandomValue();
  v.a = 1;
  return v;
}

void
InsertRandom(
    const std::vector<PropertyCacheKey>& keys,
    katana::Cache<PropertyCacheKey, CacheValue>& cache) {
  for (const auto& key : keys) {
    cache.Insert(key, RandomValue());
  }
}

void
AssertLRUElements(
    std::vector<PropertyCacheKey>::const_iterator endit, size_t num,
    katana::Cache<PropertyCacheKey, CacheValue>& cache) {
  for (auto it = endit - num; it < endit; ++it) {
    KATANA_LOG_ASSERT(cache.Get(*it).has_value());
  }
  KATANA_LOG_ASSERT(!cache.Get(*(endit - num - 1)).has_value());
}

void
TestLRUBytes(
    const std::vector<PropertyCacheKey>& node_keys,
    const std::vector<PropertyCacheKey>& edge_keys) {
  count_evictions = 0;
  size_t byte_size = 4;
  katana::Cache<PropertyCacheKey, CacheValue> cache(
      byte_size, [](const CacheValue& value) { return BytesInValue(value); },
      [&](const PropertyCacheKey&) { count_evictions++; });

  PropertyCacheKey key(NodeEdge::kNode, "not gonna happen");
  KATANA_LOG_ASSERT(!cache.Get(key).has_value());
  auto nodeit = --node_keys.end();
  KATANA_LOG_ASSERT(nodeit != node_keys.begin());
  cache.Insert(*nodeit--, SizeOneValue());
  KATANA_LOG_ASSERT(nodeit != node_keys.begin());
  cache.Insert(*nodeit--, SizeOneValue());
  KATANA_LOG_ASSERT(nodeit != node_keys.begin());
  cache.Insert(*nodeit--, SizeOneValue());
  KATANA_LOG_ASSERT(nodeit != node_keys.begin());
  cache.Insert(*nodeit--, SizeOneValue());

  KATANA_LOG_ASSERT((byte_size + 1) < node_keys.size());
  AssertLRUElements(node_keys.end(), byte_size, cache);
  // Check eviction count
  KATANA_LOG_ASSERT(count_evictions == 0);

  KATANA_LOG_ASSERT(nodeit != node_keys.begin());
  cache.Insert(*nodeit--, SizeOneValue());
  KATANA_LOG_ASSERT(count_evictions == 1);

  // EDGE keys
  nodeit = --edge_keys.end();
  KATANA_LOG_ASSERT(nodeit != edge_keys.begin());
  cache.Insert(*nodeit--, SizeFiveValue());
  KATANA_LOG_ASSERT(count_evictions == 5);
  KATANA_LOG_ASSERT(cache.size() == 5);

  cache.Insert(*nodeit--, SizeOneValue());
  KATANA_LOG_ASSERT(count_evictions == 6);
  KATANA_LOG_ASSERT(cache.size() == 1);
}

void
TestLRUSize(
    size_t lru_size, const std::vector<PropertyCacheKey>& node_keys,
    const std::vector<PropertyCacheKey>& edge_keys) {
  count_evictions = 0;
  katana::Cache<PropertyCacheKey, CacheValue> cache(
      lru_size, [&](const PropertyCacheKey&) { count_evictions++; });

  InsertRandom(node_keys, cache);

  fmt::print(
      "Inserted {} node keys, size {}\n", node_keys.size(), cache.size());
  KATANA_LOG_VASSERT(
      cache.size() == lru_size, "size {} allocated {}", cache.size(), lru_size);

  InsertRandom(edge_keys, cache);

  fmt::print(
      "Inserted {} edge keys, size {}\n", edge_keys.size(), cache.size());
  KATANA_LOG_VASSERT(
      cache.size() == lru_size, "size {} allocated {}", cache.size(), lru_size);

  // Make sure we have the LRU elements and only them.
  KATANA_LOG_ASSERT((lru_size + 1) < edge_keys.size());

  AssertLRUElements(edge_keys.end(), lru_size, cache);
}

int
main(int argc, char** argv) {
  constexpr size_t lru_size = 10;

  uint64_t size = 11 * lru_size;
  if (argc > 1) {
    size = strtol(argv[1], nullptr, 10);
  }
  if (size <= 0) {
    size = 1000000;
  }

  std::vector<PropertyCacheKey> node_keys;
  std::vector<PropertyCacheKey> edge_keys;
  // Generate maximum overlap of names
  std::vector<std::string> names;
  size_t mid = size / 2;
  if (mid * mid < size) {
    mid++;
  }
  KATANA_LOG_ASSERT(mid * mid >= size);
  for (size_t i = 0; i < mid; ++i) {
    names.emplace_back(katana::RandomAlphanumericString(16));
    node_keys.emplace_back(PropertyCacheKey(NodeEdge::kNode, names.back()));
  }

  for (size_t i = 0, end = size - node_keys.size(); i < end; ++i) {
    edge_keys.emplace_back(PropertyCacheKey(NodeEdge::kEdge, names[i]));
  }

  TestLRUSize(lru_size, node_keys, edge_keys);

  TestLRUBytes(node_keys, edge_keys);

  return 0;
}
