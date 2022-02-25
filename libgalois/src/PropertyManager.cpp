#include "katana/PropertyManager.h"

#include "katana/ArrowInterchange.h"
#include "katana/Logging.h"
#include "katana/MemorySupervisor.h"
#include "katana/ProgressTracer.h"

const std::string katana::PropertyManager::name_ = "property";

// Anchor manager vtable
katana::Manager::~Manager() = default;

void
katana::PropertyManager::MakePropertyCache() {
  KATANA_LOG_DEBUG_ASSERT(!cache_);
  auto scope = katana::GetTracer().StartActiveSpan("create property cache");

  cache_ = std::make_unique<katana::PropertyCache>(
      [](const std::shared_ptr<arrow::Table>& table) {
        return ApproxTableMemUse(table);
      });
}

katana::PropertyManager::PropertyManager() { MakePropertyCache(); }
katana::PropertyManager::~PropertyManager() { cache_.reset(); }

std::shared_ptr<arrow::Table>
katana::PropertyManager::GetProperty(const katana::URI& property_path) {
  auto property = cache_->GetAndEvict(property_path);
  if (property.has_value()) {
    auto bytes =
        static_cast<count_t>(katana::ApproxTableMemUse(property.value()));
    MemorySupervisor::Get().StandbyToActive(Name(), bytes);
    katana::GetTracer().GetActiveSpan().Log(
        "property cache get",
        {
            {"storage_name", property_path.BaseName()},
            {"approx_size_gb",
             ToGB(katana::ApproxTableMemUse(property.value()))},
        });
    return property.value();
  }
  MemorySupervisor::Get().CheckPressure();
  katana::GetTracer().GetActiveSpan().Log(
      "property cache get not found",
      {
          {"storage_name", property_path.BaseName()},
      });

  return nullptr;
}

void
katana::PropertyManager::PropertyLoadedActive(
    const std::shared_ptr<arrow::Table>& property) {
  KATANA_LOG_DEBUG_ASSERT(property);
  auto sz = katana::ApproxTableMemUse(property);
  stats.bytes_loaded += sz;
  katana::GetTracer().GetActiveSpan().Log(
      "property cache loaded active", {
                                          {"name", property->field(0)->name()},
                                          {"approx_size_gb", ToGB(sz)},
                                      });
}

void
katana::PropertyManager::PutProperty(
    const katana::URI& property_path,
    const std::shared_ptr<arrow::Table>& property) {
  auto bytes = static_cast<count_t>(katana::ApproxTableMemUse(property));
  cache_->Insert(property_path, property);
  katana::GetTracer().GetActiveSpan().Log(
      "property cache insert",
      {
          {"storage_name", property_path.BaseName()},
          {"approx_size_gb", ToGB(katana::ApproxTableMemUse(property))},
      });
  MemorySupervisor::Get().ActiveToStandby(Name(), bytes);
}

katana::count_t
katana::PropertyManager::FreeStandbyMemory(count_t goal) {
  auto scope = katana::GetTracer().StartActiveSpan("free standby memory");
  scope.span().Log(
      "before", {
                    {"goal_gb", ToGB(goal)},
                    {"cache_gb", ToGB(cache_->size())},
                });

  auto reclaim = static_cast<count_t>(cache_->Reclaim(goal));
  MemorySupervisor::Get().PutStandby(Name(), reclaim);

  scope.span().Log(
      "after", {
                   {"reclaimed_gb", ToGB(reclaim)},
                   {"cache_gb", ToGB(cache_->size())},
               });
  return reclaim;
}
