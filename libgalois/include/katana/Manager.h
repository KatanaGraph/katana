#pragma once

#include <cstdint>
#include <string>

#include "katana/config.h"

namespace katana {

// Using a signed type to make underflow more apparent
using count_t = int64_t;

/// Managers track the memory consumption for specific, large resources, e.g.,
/// properties or views.  They interact with the central MemorySupervisor (MS singleton)
/// to coordinate memory use.  They do not allocate memory, they only track it.

class KATANA_EXPORT Manager {
public:
  Manager() = default;
  virtual ~Manager();
  Manager(Manager const&) = delete;
  void operator=(Manager const&) = delete;
  Manager(Manager&&) = delete;
  Manager& operator=(Manager&&) = delete;

  /// Returns the coarse category of memory use
  ///   e.g., property for the property manager
  /// All managers must have unique names
  virtual const std::string& Name() const = 0;

  /// Free standby memory, attempting to free \p goal bytes.
  /// Returns the number of bytes freed, which can only be less than goal if the
  /// manager's standby total is less than goal.
  virtual count_t FreeStandbyMemory(count_t goal) = 0;
};

}  // namespace katana
