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

  void AddProperty(const katana::Uri& property_path);

  void PropertyLoadedCallback(const std::shared_ptr<arrow::Table>& property);

  void UnloadProperty(
      const katana::Uri& property_path,
      const std::shared_ptr<arrow::Table>& property);

  // TODO(witchel) eliminate this by having RDG call into PropertyManager
  PropertyCache* property_cache() { return cache_.get(); }

private:
  void MakePropertyCache();
  std::unique_ptr<PropertyCache> cache_;
};

}  // namespace katana
