#ifndef KATANA_LIBGRAPH_KATANA_SHAREDMEMSYS_H_
#define KATANA_LIBGRAPH_KATANA_SHAREDMEMSYS_H_

#include <memory>

#include "katana/Galois.h"
#include "katana/TextTracer.h"
#include "katana/config.h"

namespace katana {

/**
 * SharedMemSys initializes the Galois library for shared memory. Most Galois
 * library operations are only valid during the lifetime of a SharedMemSys or a
 * DistMemSys.
 *
 * It is not advisable to create a SharedMemSys more than once. Certain
 * downstream implementation dependencies like the AWS SDK cannot be
 * reinitialized.
 */
class KATANA_EXPORT SharedMemSys {
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  SharedMemSys(std::unique_ptr<ProgressTracer> tracer = TextTracer::Make());
  ~SharedMemSys();

  SharedMemSys(const SharedMemSys&) = delete;
  SharedMemSys& operator=(const SharedMemSys&) = delete;

  SharedMemSys(SharedMemSys&&) = delete;
  SharedMemSys& operator=(SharedMemSys&&) = delete;
};

}  // namespace katana

#endif
