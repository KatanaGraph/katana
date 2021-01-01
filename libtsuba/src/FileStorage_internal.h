#ifndef KATANA_LIBTSUBA_FILESTORAGEINTERNAL_H_
#define KATANA_LIBTSUBA_FILESTORAGEINTERNAL_H_

#include "tsuba/FileStorage.h"

namespace tsuba {

std::vector<FileStorage*>& GetRegisteredFileStorages();

}  // namespace tsuba

#endif
