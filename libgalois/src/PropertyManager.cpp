#include "katana/PropertyManager.h"

#include "katana/ArrowInterchange.h"
#include "katana/MemorySupervisor.h"
#include "katana/ProgressTracer.h"

const std::string katana::PropertyManager::memory_category = "property";

// Anchor manager vtable
katana::Manager::~Manager() = default;

void
katana::PropertyManager::MakePropertyCache() {
  KATANA_LOG_DEBUG_ASSERT(!cache_);
  auto& tracer = katana::GetTracer();
  tracer.GetActiveSpan().Log("create property cache");

  cache_ = std::make_unique<katana::PropertyCache>(
      [](const std::shared_ptr<arrow::Table>& table) {
        return ApproxTableMemUse(table);
      });
}

katana::PropertyManager::PropertyManager() { MakePropertyCache(); }
katana::PropertyManager::~PropertyManager() { cache_.reset(); }

// TODO(witchel) should return a bool or the property table to caller
void
katana::PropertyManager::AddProperty(const katana::Uri& property_path) {
  auto property = cache_->GetAndEvict(property_path);
  if (property.has_value()) {
    auto bytes = katana::ApproxTableMemUse(property.value());
    MemorySupervisor::Get().StandbyToActive(this, bytes);
  }
}

void
katana::PropertyManager::PropertyLoadedCallback(
    const std::shared_ptr<arrow::Table>& property) {
  KATANA_LOG_DEBUG_ASSERT(property);
  auto bytes = katana::ApproxTableMemUse(property);
  MemorySupervisor::Get().BorrowActive(this, bytes);
}

void
katana::PropertyManager::UnloadProperty(
    const katana::Uri& property_path,
    const std::shared_ptr<arrow::Table>& property) {
  auto bytes = katana::ApproxTableMemUse(property);
  auto granted = MemorySupervisor::Get().ActiveToStandby(this, bytes);
  if (granted >= static_cast<count_t>(bytes)) {
    cache_->Insert(property_path, property);
  } else {
    MemorySupervisor::Get().ReturnActive(this, bytes);
  }
}

katana::count_t
katana::PropertyManager::FreeStandbyMemory(count_t goal) {
  count_t total = 0;
  if (goal >= static_cast<count_t>(cache_->size())) {
    total = static_cast<count_t>(cache_->size());
    MemorySupervisor::Get().ReturnStandby(this, total);
    cache_->clear();
  } else {
    total = cache_->Reclaim(goal);
    MemorySupervisor::Get().ReturnStandby(this, total);
  }
  return total;
}
