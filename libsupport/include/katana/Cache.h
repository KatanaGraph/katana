#ifndef KATANA_LIBSUPPORT_KATANA_CACHE_H_
#define KATANA_LIBSUPPORT_KATANA_CACHE_H_

// This is single threaded only, it is not intended to store large objects,
// but rather metadata (e.g., a shared_ptr to a property column).

// The problem witchel had implementing a multi-threaded version using
// parallel-hashmap is a lock ordering problem.  parallel-hashmap 1.33 allows
// execution of code with the write lock held, only when modifying, not when adding an
// element.  We have to modify the LRU list when eviciting an element, so the natural
// lock ordering is parallel-hashmap write lock, then list lock.  But without a way to
// execute insert code with the parallel-hashmap write lock held, it seemed like there
// would be some form of race condition.

#include <cstdint>
#include <functional>
#include <list>
#include <optional>
#include <string>
#include <unordered_map>

#include <arrow/table.h>

#include "katana/Logging.h"
#include "katana/ProgressTracer.h"
#include "katana/URI.h"

namespace katana {

struct CacheStats {
  float get_hit_percentage() const {
    if (get_count == 0LL) {
      return 0.0;
    }
    return 100.0 * static_cast<float>(get_hit_count) / get_count;
  }
  float insert_hit_percentage() const {
    if (insert_count == 0ULL) {
      return 0.0;
    }
    return 100.0 * static_cast<float>(insert_hit_count) / insert_count;
  }
  float total_hit_percentage() const {
    if (total_count() == 0ULL) {
      return 0.0;
    }
    return 100.0 *
           (static_cast<float>(get_hit_count) +
            static_cast<float>(insert_hit_count)) /
           total_count();
  }
  uint64_t total_count() const { return insert_count + get_count; }
  void Log() const {
    katana::GetTracer().GetActiveSpan().Log(
        "cache stats",
        {
            {"get_per", fmt::format("{:.2f}%", get_hit_percentage())},
            {"insert_per", fmt::format("{:.2f}%", insert_hit_percentage())},
            {"total_per", fmt::format("{:.2f}%", total_hit_percentage())},
            {"total_count", total_count()},
            {"get_count", get_count},
            {"insert_count", insert_count},
        });
  }

  int64_t get_count{0LL};
  int64_t get_hit_count{0LL};
  int64_t insert_count{0LL};
  int64_t insert_hit_count{0LL};
};

template <typename Value>
class KATANA_EXPORT Cache {
  using Key = katana::URI;
  using ListType = std::list<Key>;
  struct MapValue {
    Value value;
    // This allows us to delete the old position in the LRU list without a scan
    typename ListType::iterator lru_it;
  };
  using MapType = std::unordered_map<Key, MapValue, Key::Hash>;
  // kLRUSize - LRU replacement when the number of elements is above threshold
  // kLRUBytes- LRU replacement when the byte count of elements is above threshold
  // kNone - LRU replacement only on demand
  enum class ReplacementPolicy { kLRUSize, kLRUBytes, kLRUExplicit };

public:
  /// Construct an LRU cache that has a fixed number of entries.
  Cache(int64_t capacity)  // number of entries
      : policy_(ReplacementPolicy::kLRUSize),
        capacity_(capacity),
        value_to_bytes_(nullptr) {
    KATANA_LOG_VASSERT(capacity_ > 0, "cache requires positive capacity");
  }
  /// Construct an LRU cache that holds fixed number of bytes.
  Cache(
      int64_t capacity,  // bytes of entries
      std::function<int64_t(const Value& value)> value_to_bytes)
      : policy_(ReplacementPolicy::kLRUBytes),
        capacity_(capacity),
        value_to_bytes_(std::move(value_to_bytes)) {
    KATANA_LOG_VASSERT(capacity_ > 0, "cache requires positive capacity");
    KATANA_LOG_VASSERT(
        value_to_bytes_ != nullptr,
        "kLRUBytes policy requires value to bytes function");
  }
  /// Construct an LRU cache that holds whatever we put in it and only evicts when we
  /// explicitly tell it to do so.
  /// NB: The way we use this, the insert hit rate is always 0 because we GetAndEvict and
  /// then possibly insert back.
  Cache(std::function<int64_t(const Value& value)> value_to_bytes)
      : policy_(ReplacementPolicy::kLRUExplicit),
        capacity_(std::numeric_limits<int64_t>::max()),
        value_to_bytes_(std::move(value_to_bytes)) {
    KATANA_LOG_VASSERT(
        value_to_bytes_ != nullptr,
        "kLRUExplicit policy requires value to bytes function");
  }

  /// Returns the size of the cache (in number of elements or size of elements,
  /// depending on the replacement policy).
  int64_t size() const {
    if (policy_ == ReplacementPolicy::kLRUSize) {
      return key_to_value_.size();
    }
    return total_bytes_;
  }

  /// Returns the capacity (in number of elements or size of elements, depending on
  /// the replacement policy).
  int64_t capacity() const {
    if (policy_ == ReplacementPolicy::kLRUExplicit) {
      return std::numeric_limits<int64_t>::max();
    }
    return capacity_;
  }

  /// Clear cache
  void clear() {
    key_to_value_.clear();
    lru_list_.clear();
    total_bytes_ = 0;
  }

  /// Returns true if the cache is empty
  bool empty() const { return key_to_value_.empty(); }

  /// Try to reclaim \p goal bytes (#entries), evicting least recently used entries to
  /// do it.  Returns the number of bytes actually evicted.
  int64_t Reclaim(int64_t goal) {
    int64_t reclaimed{};
    while (!empty() && reclaimed < goal) {
      reclaimed += EvictLastOne();
    }
    return reclaimed;
  }

  bool Contains(const Key& key) const {
    return key_to_value_.find(key) != key_to_value_.end();
  }

  void Insert(const Key& key, const Value& value) {
    cache_stats_.insert_count++;
    auto mapit = key_to_value_.find(key);
    if (mapit == key_to_value_.end()) {
      int64_t approx_bytes{};
      if (value_to_bytes_ != nullptr) {
        approx_bytes = value_to_bytes_(value);
        if (approx_bytes > capacity_) {
          // Object too big, don't insert
          return;
        }
      }
      lru_list_.push_front(key);
      key_to_value_[key] = {value, lru_list_.begin()};
      if (value_to_bytes_ != nullptr) {
        if (approx_bytes == 0) {
          KATANA_LOG_WARN(
              "caching zero sized object with LRUBytes policy is illogical");
        }
        total_bytes_ += approx_bytes;
      }
    } else {
      cache_stats_.insert_hit_count++;
      mapit->second.value = value;
      UpdateLRU(mapit);
    }
    EvictIfNecessary();
  }

  std::optional<Value> Get(const Key& key) {
    // lookup value in the cache
    cache_stats_.get_count++;
    std::optional<Value> ret;
    auto it = key_to_value_.find(key);
    if (it != key_to_value_.end()) {
      ret = UpdateLRU(it);
      cache_stats_.get_hit_count++;
    }
    return ret;
  }

  std::optional<Value> GetAndEvict(const Key& key) {
    // lookup value in the cache
    cache_stats_.get_count++;
    std::optional<Value> ret;
    auto it = key_to_value_.find(key);
    if (it != key_to_value_.end()) {
      ret = EvictMe(it->second.lru_it);
      cache_stats_.get_hit_count++;
    }
    return ret;
  }

  CacheStats GetStats() const { return cache_stats_; }

  // This is mostly a debugging function.  It also explains the cache data structures
  int64_t LRUPosition(const Key& key) {
    auto it = key_to_value_.find(key);
    if (it != key_to_value_.end()) {
      auto& lru_it = it->second.lru_it;
      return std::distance(lru_list_.begin(), lru_it);
    }
    return -1L;
  }

private:
  Value UpdateLRU(typename MapType::iterator mapit) {
    auto lru_it = mapit->second.lru_it;
    if (lru_it != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(lru_it);
      lru_list_.push_front(mapit->first);
      mapit->second.lru_it = lru_list_.begin();
    }
    return mapit->second.value;
  }

  Value EvictMe(ListType::iterator evictit) {
    KATANA_LOG_DEBUG_ASSERT(evictit != lru_list_.end());
    Key evicted_key = std::move(*evictit);
    lru_list_.erase(evictit);
    auto evicted_value = std::move(key_to_value_.at(evicted_key).value);
    key_to_value_.erase(evicted_key);
    if (value_to_bytes_ != nullptr) {
      total_bytes_ -= value_to_bytes_(evicted_value);
    }
    return evicted_value;
  }

  uint64_t EvictLastOne() {
    // evict item from the end of most recently used list
    auto tail = --lru_list_.end();
    auto evicted_value = EvictMe(tail);
    if (value_to_bytes_ != nullptr) {
      return value_to_bytes_(evicted_value);
    }
    return 1;
  }

  void EvictIfNecessary() {
    switch (policy_) {
    case ReplacementPolicy::kLRUSize:
    case ReplacementPolicy::kLRUBytes: {
      while (size() > capacity_) {
        EvictLastOne();
      }
    } break;
    case ReplacementPolicy::kLRUExplicit: {
      // Do nothing
    } break;
    default:
      KATANA_LOG_FATAL("bad cache replacement policy: {}", policy_);
    }
  }

  // Map from key to value
  MapType key_to_value_;
  // LRU list
  ListType lru_list_;

  ReplacementPolicy policy_;
  // for kLRUSize number of entries kLRUBytes it is byte total
  int64_t capacity_{0};
  int64_t total_bytes_{0};
  // Hit statistics for gets and inserts
  CacheStats cache_stats_;

  std::function<int64_t(const Value& value)> value_to_bytes_;
};

// The property cache contains properties NOT in use by the graph and never contains a
// property that IS in use by the graph.  When a graph unloads a property, it goes
// into the cache, and when it loads a property it (hopefully) comes from the cache.
using PropertyCache = Cache<std::shared_ptr<arrow::Table>>;

}  // namespace katana

#endif
