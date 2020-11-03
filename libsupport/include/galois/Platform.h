#ifndef GALOIS_LIBSUPPORT_GALOIS_PLATFORM_H_
#define GALOIS_LIBSUPPORT_GALOIS_PLATFORM_H_

#include <sys/mman.h>

#if __linux__
#include <linux/version.h>
#if LINUX_VERSION_CODE > KERNEL_VERSION(2, 6, 22)
#define GALOIS_PLATFORM_MAP_POPULATE_AVAILABLE_
#endif
#endif

namespace galois {

static inline void*
MmapPopulate(void* addr, size_t size, int prot, int flags, int fd, off_t off) {
#ifdef GALOIS_PLATFORM_MAP_POPULATE_AVAILABLE_
  return mmap(addr, size, prot, flags | MAP_POPULATE, fd, off);
#else
  return mmap(addr, size, prot, flags, fd, off);
#endif
}

}  // namespace galois

#undef GALOIS_PLATFORM_MAP_POPULATE_AVAILABLE_

#endif
