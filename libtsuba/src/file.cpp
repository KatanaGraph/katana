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
tsuba::FileStore(const std::string& uri, const void* data, uint64_t size) {
  return FS(uri)->PutMultiSync(uri, static_cast<const uint8_t*>(data), size);
}

std::future<katana::CopyableResult<void>>
tsuba::FileStoreAsync(const std::string& uri, const void* data, uint64_t size) {
  return FS(uri)->PutAsync(uri, static_cast<const uint8_t*>(data), size);
}

katana::Result<void>
tsuba::FileGet(
    const std::string& uri, void* result_buffer, uint64_t begin,
    uint64_t size) {
  return FS(uri)->GetMultiSync(
      uri, begin, size, static_cast<uint8_t*>(result_buffer));
}

std::future<katana::CopyableResult<void>>
tsuba::FileGetAsync(
    const std::string& uri, void* result_buffer, uint64_t begin,
    uint64_t size) {
  return FS(uri)->GetAsync(
      uri, begin, size, static_cast<uint8_t*>(result_buffer));
}

katana::Result<void>
tsuba::FileRemoteCopy(
    const std::string& source_uri, const std::string& dest_uri, uint64_t begin,
    uint64_t size) {
  auto source_fs = FS(source_uri);
  auto dest_fs = FS(dest_uri);

  if (source_fs != dest_fs) {
    KATANA_LOG_ERROR("cannot copy between different back-ends");
    return ErrorCode::NotImplemented;
  }

  return dest_fs->RemoteCopy(source_uri, dest_uri, begin, size);
}

katana::Result<void>
tsuba::FileStat(const std::string& uri, StatBuf* s_buf) {
  return FS(uri)->Stat(uri, s_buf);
}

std::future<katana::CopyableResult<void>>
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
