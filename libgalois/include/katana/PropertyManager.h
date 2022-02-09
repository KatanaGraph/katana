#pragma once

#include <cstdint>
#include <string>

#include "katana/Cache.h"
#include "katana/Manager.h"

namespace katana {

/// Manager for property memory
class KATANA_EXPORT PropertyManager : public Manager {
public:
  PropertyManager();
  ~PropertyManager();
  /// Returns the coarse category of memory use
  ///   e.g., property for the property manager
  static const std::string memory_category;
  const std::string& MemoryCategory() const override { return memory_category; }
  count_t FreeStandbyMemory(count_t goal) override;

  /// Client wants a property, see if we have it in the cache and if so return it and make the memory active.
  /// Returns nullptr if manager does not have it in the cache
  std::shared_ptr<arrow::Table> GetProperty(const katana::Uri& property_path);

  /// The property data has come into memory from storage, so account for the new, active memory
  void PropertyLoadedCallback(const std::shared_ptr<arrow::Table>& property);

  /// We are done with the property.  Put it in the cache if we have room.
  void PutProperty(
      const katana::Uri& property_path,
      const std::shared_ptr<arrow::Table>& property);

  // TODO(witchel) eliminate this by having RDG call into PropertyManager
  PropertyCache* property_cache() { return cache_.get(); }

private:
  void MakePropertyCache();
  std::unique_ptr<PropertyCache> cache_;
};

}  // namespace katana
