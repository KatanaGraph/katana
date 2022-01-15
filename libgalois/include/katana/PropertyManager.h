#pragma once

#include <cstdint>
#include <string>

#include "katana/Manager.h"
#include "katana/MemoryManager.h"

/// Managers tend to the memory consumption for specific, large resources, e.g.,
/// properties or views.  They interact with the central MemoryManager (MM singleton)
/// to coordinate memory use.

class PropertyManager : public Manager {
public:
  /// Returns the coarse category of memory use
  ///   e.g., property for the property manager
  virtual const std::string& MemoryCategory() { return "property"; }

  /// Free standby memory, attempting to free \p goal bytes.
  /// Returns the number of bytes freed, which can only be less than goal if the
  /// manager's standby total is less than goal.
  virtual count_t FreeStandbyMemory(count_t goal);

  // void Insert(
  //     const katana::Uri& cache_key, const std::shared_ptr<arrow::table>& props);

  void AddProperty(const katana::Uri& property_path) {
    auto property = cache_->Get(property_path);
    if (property.has_value()) {
      auto bytes = katana::ApproxTableMemUse(property.value());
      MM().StandbyToActive(this, bytes);
      property_cache_->Evict(property_path);
    }
  }

  void PropertyLoadedCallback(const std::shared_ptr<arrow::table>& property) {
    MM.BorrowActive(*this, property.size());
  }

  void UnloadProperty(const std::shared_ptr<arrow::table>& property) {
    auto granted = MM.ActiveToStandby(*this, property.size());
    if (granted >= property.size()) {
      property_cache_->Insert(property);
    } else {
      MM.ReturnActive(property.size());
    }
  }

  count_t FreeStandbyMemory(count_t goal) {
    count_t total = 0;
    if (goal >= cache_.size()) {
      total = cache_.size();
      MM.ReturnStandby(total);
      cache_.clear();
    } else {
      total = cache_->Reclaim(goal);
      MM().ReturnStandby(total);
    }
    return total;
  }

private:
  PropertyCache cache_;
}
