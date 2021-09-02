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

#include <list>
#include <optional>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "katana/Logging.h"

namespace katana {

template <typename Key, typename Value, typename CallerPointer = void*>
class KATANA_EXPORT Cache {
  using ListType = std::list<Key>;
  struct MapValue {
    Value value;
    // This allows us to delete the old position in the LRU list without a scan
    typename ListType::iterator lru_it;
  };
  using MapType = std::unordered_map<Key, MapValue, typename Key::Hash>;
  enum class ReplacementPolicy { kLRUSize, kLRUBytes };

public:
  /// Construct an LRU cache that has a fixed number of entries.
  Cache(
      size_t capacity,  // number of entries
      std::function<
          void(const Key& key, uint64_t approx_bytes, CallerPointer rdg)>
          evict_cb = nullptr)
      : policy_(ReplacementPolicy::kLRUSize),
        capacity_(capacity),
        value_to_bytes_(nullptr),
        evict_cb_(std::move(evict_cb)) {
    KATANA_LOG_VASSERT(capacity_ > 0, "cache requires positive capacity");
  }
  /// Construct an LRU cache that holds fixed number of bytes.
  Cache(
      size_t capacity,  // bytes of entries
      std::function<size_t(const Value& value)> value_to_bytes,
      std::function<
          void(const Key& key, uint64_t approx_bytes, CallerPointer rdg)>
          evict_cb = nullptr)
      : policy_(ReplacementPolicy::kLRUBytes),
        capacity_(capacity),
        value_to_bytes_(std::move(value_to_bytes)),
        evict_cb_(std::move(evict_cb)) {
    KATANA_LOG_VASSERT(capacity_ > 0, "cache requires positive capacity");
    KATANA_LOG_VASSERT(
        value_to_bytes_ != nullptr,
        "kLRUBytes policy requires value to bytes function");
  }

  size_t size() const {
    if (policy_ == ReplacementPolicy::kLRUSize) {
      return key_to_value_.size();
    } else {
      return total_bytes_;
    }
  }

  size_t capacity() const { return capacity_; }

  bool Empty() const { return key_to_value_.empty(); }

  bool Contains(const Key& key) const {
    return key_to_value_.find(key) != key_to_value_.end();
  }

  void Insert(const Key& key, const Value& value, CallerPointer rdg = nullptr) {
    auto mapit = key_to_value_.find(key);
    if (mapit == key_to_value_.end()) {
      lru_list_.push_front(key);
      key_to_value_[key] = {value, lru_list_.begin()};
      if (value_to_bytes_ != nullptr) {
        total_bytes_ += value_to_bytes_(value);
      }
    } else {
      mapit->second.value = value;
      UpdateLRU(mapit);
    }
    EvictIfNecessary(rdg);
  }

  std::optional<Value> Get(const Key& key) {
    // lookup value in the cache
    std::optional<Value> ret;
    auto it = key_to_value_.find(key);
    if (it != key_to_value_.end()) {
      ret = UpdateLRU(it);
    }
    return ret;
  }

private:
  Value UpdateLRU(typename MapType::iterator mapit) {
    auto lru_it = mapit->second.lru_it;
    auto lru_head = lru_list_.begin();
    if (lru_it != lru_head) {
      // move item to the front of the most recently used list
      lru_list_.splice(lru_head, lru_list_, lru_it);
      mapit->second.lru_it = lru_head;
    }
    return mapit->second.value;
  }

  void EvictOne(CallerPointer rdg) {
    // evict item from the end of most recently used list
    auto tail = --lru_list_.end();
    KATANA_LOG_ASSERT(tail != lru_list_.end());
    Key evicted_key = std::move(*tail);
    lru_list_.erase(tail);
    auto evicted_value = std::move(key_to_value_.at(evicted_key).value);
    uint64_t approx_evicted_bytes = 0;
    key_to_value_.erase(evicted_key);
    if (value_to_bytes_ != nullptr) {
      approx_evicted_bytes = value_to_bytes_(evicted_value);
      total_bytes_ -= approx_evicted_bytes;
    }
    if (evict_cb_) {
      evict_cb_(evicted_key, approx_evicted_bytes, rdg);
    }
  }

  void EvictIfNecessary(CallerPointer rdg) {
    switch (policy_) {
    case ReplacementPolicy::kLRUSize: {
      while (size() > capacity_) {
        EvictOne(rdg);
      }
    } break;
    case ReplacementPolicy::kLRUBytes: {
      KATANA_LOG_DEBUG_ASSERT(value_to_bytes_ != nullptr);
      // Allow a single entry to exceed our byte capacity.
      // The new entry has already been added to the cache, hence > 1.
      while (size() > capacity_ && key_to_value_.size() > 1) {
        EvictOne(rdg);
      }
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
  size_t capacity_{0};
  size_t total_bytes_{0};

  std::function<size_t(const Value& value)> value_to_bytes_;
  std::function<void(const Key& key, uint64_t approx_bytes, CallerPointer rdg)>
      evict_cb_;
};

}  // namespace katana

#endif
