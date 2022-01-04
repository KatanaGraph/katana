#include "katana/FileStorage.h"

#include "FileStorage_internal.h"

katana::FileStorage::~FileStorage() = default;

std::vector<katana::FileStorage*>&
katana::GetRegisteredFileStorages() {
  static std::vector<FileStorage*> fs;
  return fs;
}

void
katana::RegisterFileStorage(FileStorage* fs) {
  GetRegisteredFileStorages().emplace_back(fs);
}
