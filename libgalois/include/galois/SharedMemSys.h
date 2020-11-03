#ifndef GALOIS_LIBGALOIS_GALOIS_SHAREDMEMSYS_H_
#define GALOIS_LIBGALOIS_GALOIS_SHAREDMEMSYS_H_

#include <memory>

#include "galois/config.h"

namespace galois {

/**
 * SharedMemSys initializes the Galois library for shared memory. Most Galois
 * library operations are only valid during the lifetime of a SharedMemSys or a
 * DistMemSys.
 *
 * It is not advisable to create a SharedMemSys more than once. Certain
 * downstream implementation dependencies like the AWS SDK cannot be
 * reinitialized.
 */
class GALOIS_EXPORT SharedMemSys {
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  SharedMemSys();
  ~SharedMemSys();

  SharedMemSys(const SharedMemSys&) = delete;
  SharedMemSys& operator=(const SharedMemSys&) = delete;

  SharedMemSys(SharedMemSys&&) = delete;
  SharedMemSys& operator=(SharedMemSys&&) = delete;
};

}  // namespace galois

#endif
