#ifndef KATANA_LIBTSUBA_FILESTORAGEINTERNAL_H_
#define KATANA_LIBTSUBA_FILESTORAGEINTERNAL_H_

#include "katana/FileStorage.h"

namespace katana {

std::vector<FileStorage*>& GetRegisteredFileStorages();

}  // namespace katana

#endif
