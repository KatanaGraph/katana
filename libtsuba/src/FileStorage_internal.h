#ifndef GALOIS_LIBTSUBA_FILESTORAGEINTERNAL_H_
#define GALOIS_LIBTSUBA_FILESTORAGEINTERNAL_H_

#include "tsuba/FileStorage.h"

namespace tsuba {

std::vector<FileStorage*>& GetRegisteredFileStorages();

}  // namespace tsuba

#endif
