#ifndef GALOIS_LIBGALOIS_GALOIS_SHAREDMEMSYS_H_
#define GALOIS_LIBGALOIS_GALOIS_SHAREDMEMSYS_H_

#include "galois/config.h"
#include "galois/runtime/SharedMem.h"

namespace tsuba {

class NameServerClient;

}  // namespace tsuba

namespace galois {

/**
 * SharedMemSys is an explicit class to initialize the Galois runtime. The
 * runtime is destroyed when this object is destroyed.
 */
class GALOIS_EXPORT SharedMemSys : public runtime::SharedMem<StatManager> {
  std::unique_ptr<tsuba::NameServerClient> ns_;

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
