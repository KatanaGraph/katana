#include "katana/Cache.h"

#include <map>
#include <random>

#include "katana/Cache.h"
#include "katana/Logging.h"
#include "katana/Random.h"

struct CacheValue {
  int64_t a;
  int32_t b;
  uint32_t c;
};

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
    const std::vector<katana::Uri>& keys, katana::Cache<CacheValue>& cache) {
  for (const auto& key : keys) {
    cache.Insert(key, RandomValue());
    KATANA_LOG_ASSERT(cache.LRUPosition(key) == 0);
  }
}

void
AssertLRUElements(
    std::vector<katana::Uri>::const_iterator endit, size_t num,
    katana::Cache<CacheValue>& cache) {
  for (auto it = endit - num; it < endit; ++it) {
    KATANA_LOG_ASSERT(cache.Get(*it).has_value());
    KATANA_LOG_VASSERT(
        cache.LRUPosition(*it) == 0, "{} LRUPosition {}", *it,
        cache.LRUPosition(*it));
  }
  auto outofboundsit = endit - num - 1;
  KATANA_LOG_ASSERT(!cache.Get(*outofboundsit).has_value());
  KATANA_LOG_ASSERT(cache.LRUPosition(*outofboundsit) == -1L);
}

// NB: The code that runs after this function assumes that exactly 4 size 1 elements
// have been inserted.
std::vector<katana::Uri>::const_iterator
TestBasicLRU(
    const std::vector<katana::Uri>& keys, katana::Cache<CacheValue>& cache) {
  auto uri_res = katana::Uri::Make("not gonna happen");
  KATANA_LOG_ASSERT(uri_res);
  katana::Uri badKey = uri_res.value();
  KATANA_LOG_ASSERT(!cache.Get(badKey).has_value());
  KATANA_LOG_ASSERT(cache.LRUPosition(badKey) == -1L);
  auto keyit = --keys.end();
  KATANA_LOG_ASSERT(keyit != keys.begin());
  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*(keyit + 1)) == 0);
  KATANA_LOG_ASSERT(keyit != keys.begin());
  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*(keyit + 1)) == 0);
  KATANA_LOG_ASSERT(keyit != keys.begin());
  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*(keyit + 1)) == 0);
  KATANA_LOG_ASSERT(keyit != keys.begin());
  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*(keyit + 1)) == 0);

  auto key_count = std::distance(keyit, keys.end()) - 1;
  AssertLRUElements(keys.end(), key_count, cache);
  return keyit;
}

void
TestLRUExplicit(const std::vector<katana::Uri>& keys) {
  katana::Cache<CacheValue> cache(
      [](const CacheValue& value) { return BytesInValue(value); });

  auto keyit = TestBasicLRU(keys, cache);

  KATANA_LOG_ASSERT(keyit != keys.begin());
  cache.Insert(*keyit--, SizeOneValue());
  size_t key_count = std::distance(keyit, keys.end()) - 1;
  KATANA_LOG_ASSERT(cache.size() == key_count);

  KATANA_LOG_ASSERT(keyit != keys.begin());
  auto fiveit = keyit;
  cache.Insert(*keyit--, SizeFiveValue());
  KATANA_LOG_ASSERT(cache.size() == key_count + 5);

  // GetAndEvict the least recently used
  auto firstkey = *--keys.end();
  auto val = cache.GetAndEvict(firstkey);
  KATANA_LOG_ASSERT(val.has_value());
  KATANA_LOG_ASSERT(val.value().a == 0);
  KATANA_LOG_ASSERT(cache.LRUPosition(*fiveit) == 0L);

  // Reclaim from the end of the LRU list
  auto reclaim = cache.Reclaim(1);
  KATANA_LOG_ASSERT(reclaim == 1);
  KATANA_LOG_ASSERT(cache.size() == key_count + 3);
  KATANA_LOG_ASSERT(cache.LRUPosition(*fiveit) == 0L);
  reclaim = cache.Reclaim(1);
  KATANA_LOG_ASSERT(reclaim == 1);
  KATANA_LOG_ASSERT(cache.size() == key_count + 2);
  KATANA_LOG_ASSERT(cache.LRUPosition(*fiveit) == 0L);
  // Relcaims remaining 2 size 1, but also the size 5
  reclaim = cache.Reclaim(3);
  KATANA_LOG_VASSERT(reclaim == 7, "reclaim {}", reclaim);
  KATANA_LOG_ASSERT(cache.size() == 0);

  // Insert two, GetAndEvict the most recently used
  keyit = --keys.end();
  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*(keyit + 1)) == 0);
  cache.Insert(*keyit, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*keyit) == 0);
  val = cache.GetAndEvict(*keyit);
  KATANA_LOG_ASSERT(val.has_value());
  KATANA_LOG_ASSERT(val.value().a == 0);
  KATANA_LOG_ASSERT(cache.LRUPosition(*--keys.end()) == 0L);
}

void
TestLRUBytes(const std::vector<katana::Uri>& keys) {
  size_t byte_size = 4;
  KATANA_LOG_ASSERT((byte_size + 1) < keys.size());
  katana::Cache<CacheValue> cache(
      byte_size, [](const CacheValue& value) { return BytesInValue(value); });

  KATANA_LOG_VASSERT(
      cache.capacity() == byte_size, "capacity {} allocated {}",
      cache.capacity(), byte_size);

  auto keyit = TestBasicLRU(keys, cache);

  KATANA_LOG_ASSERT(keyit != keys.begin());
  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.size() == 4);

  cache.Insert(*keyit--, SizeFiveValue());
  // Does not cache it, it is too big
  KATANA_LOG_ASSERT(cache.size() == 4);

  cache.Insert(*keyit--, SizeOneValue());
  KATANA_LOG_ASSERT(cache.LRUPosition(*(keyit + 1)) == 0);
  KATANA_LOG_ASSERT(cache.size() == 4);

  cache.clear();
  KATANA_LOG_VASSERT(
      cache.capacity() == byte_size, "capacity {} allocated {}",
      cache.capacity(), byte_size);
  KATANA_LOG_ASSERT(cache.empty());
  KATANA_LOG_ASSERT(cache.size() == 0);
}

void
TestLRUSize(size_t lru_size, const std::vector<katana::Uri>& keys) {
  katana::Cache<CacheValue> cache(lru_size);
  KATANA_LOG_VASSERT(
      cache.capacity() == lru_size, "capcity {} allocated {}", cache.capacity(),
      lru_size);

  InsertRandom(keys, cache);

  fmt::print(
      "Inserted {} keys, size {} keys[0]={} \n", keys.size(), cache.size(),
      keys.empty() ? "NO KEYS" : keys[0].string());

  KATANA_LOG_VASSERT(
      cache.size() == lru_size, "size {} allocated {}", cache.size(), lru_size);

  // Make sure we have the LRU elements and only them.
  KATANA_LOG_ASSERT((lru_size + 1) < keys.size());

  AssertLRUElements(keys.end(), lru_size, cache);

  cache.clear();
  KATANA_LOG_VASSERT(
      cache.capacity() == lru_size, "size {} allocated {}", cache.size(),
      lru_size);
  KATANA_LOG_ASSERT(cache.empty());
  KATANA_LOG_ASSERT(cache.size() == 0);
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

  std::vector<katana::Uri> keys(size);
  for (size_t i = 0; i < size; ++i) {
    auto uri_res = katana::Uri::Make(katana::RandomAlphanumericString(16));
    KATANA_LOG_ASSERT(uri_res);
    keys[i] = uri_res.value();
  }
  TestLRUSize(lru_size, keys);

  TestLRUBytes(keys);

  TestLRUExplicit(keys);

  return 0;
}
