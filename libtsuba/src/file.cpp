//
// @file libtsuba/srce/file.cpp
//
// Contains the unstructured entry points for interfacing with the tsuba storage
// server
//
#include "tsuba/file.h"

#include <sys/mman.h>

#include <cassert>
#include <fstream>
#include <iostream>
#include <mutex>
#include <unordered_map>

#include "GlobalState.h"
#include "katana/Logging.h"
#include "katana/Platform.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"

katana::Result<void>
tsuba::FileStore(const std::string& uri, const uint8_t* data, uint64_t size) {
  return FS(uri)->PutMultiSync(uri, data, size);
}

std::future<katana::Result<void>>
tsuba::FileStoreAsync(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  return FS(uri)->PutAsync(uri, data, size);
}

katana::Result<void>
tsuba::FileGet(
    const std::string& uri, uint8_t* result_buffer, uint64_t begin,
    uint64_t size) {
  return FS(uri)->GetMultiSync(uri, begin, size, result_buffer);
}

std::future<katana::Result<void>>
tsuba::FileGetAsync(
    const std::string& uri, uint8_t* result_buffer, uint64_t begin,
    uint64_t size) {
  return FS(uri)->GetAsync(uri, begin, size, result_buffer);
}

katana::Result<void>
tsuba::FileStat(const std::string& uri, StatBuf* s_buf) {
  return FS(uri)->Stat(uri, s_buf);
}

std::future<katana::Result<void>>
tsuba::FileListAsync(
    const std::string& directory, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  return FS(directory)->ListAsync(directory, list, size);
}

katana::Result<void>
tsuba::FileDelete(
    const std::string& directory,
    const std::unordered_set<std::string>& files) {
  return FS(directory)->Delete(directory, files);
}
