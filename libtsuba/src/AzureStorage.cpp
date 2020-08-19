#include "AzureStorage.h"

#include <regex>
#include <unordered_set>

#include "galois/Result.h"
#include "tsuba/file.h"
#include "tsuba/Errors.h"
#include "GlobalState.h"

#include "azure.h"

namespace {

const std::regex kAzureUriRegex("abfs://([-a-z0-9.]+)/(.+)");

} // namespace

namespace tsuba {

GlobalFileStorageAllocator azure_storage_allocator([]() {
  return std::unique_ptr<FileStorage>(new AzureStorage());
});

galois::Result<std::pair<std::string, std::string>>
AzureStorage::CleanURI(const std::string& uri) {
  std::smatch sub_match;
  if (!std::regex_match(uri, sub_match, kAzureUriRegex)) {
    return ErrorCode::InvalidArgument;
  }
  return std::make_pair(sub_match[1], sub_match[2]);
}

galois::Result<void> AzureStorage::Init() { return AzureInit(); }

galois::Result<void> AzureStorage::Fini() { return AzureFini(); }

galois::Result<void> AzureStorage::Stat(const std::string& uri,
                                        StatBuf* s_buf) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzureGetSize(container, blob, &s_buf->size);
}

galois::Result<void> AzureStorage::Create(const std::string& uri,
                                          bool overwrite) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  auto exists_res        = AzureExists(container, blob);
  if (!exists_res) {
    return exists_res.error();
  }

  if (!overwrite && exists_res.value()) {
    return ErrorCode::Exists;
  }
  return galois::ResultSuccess();
}

galois::Result<void> AzureStorage::GetMultiSync(const std::string& uri,
                                                uint64_t start, uint64_t size,
                                                uint8_t* result_buf) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzureGetSync(container, blob, start, size,
                      reinterpret_cast<char*>(result_buf)); // NOLINT
}

galois::Result<void> AzureStorage::PutMultiSync(const std::string& uri,
                                                const uint8_t* data,
                                                uint64_t size) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzurePutSync(container, blob,
                      reinterpret_cast<const char*>(data), // NOLINT
                      size);
}

galois::Result<std::unique_ptr<FileAsyncWork>>
AzureStorage::PutAsync(const std::string& uri, const uint8_t* data,
                       uint64_t size) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzurePutAsync(container, blob,
                       reinterpret_cast<const char*>(data), // NOLINT
                       size);
}

galois::Result<std::unique_ptr<FileAsyncWork>>
AzureStorage::GetAsync(const std::string& uri, uint64_t start, uint64_t size,
                       uint8_t* result_buf) {
  auto uri_res = CleanURI(std::string(uri));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzureGetAsync(container, blob, start, size,
                       reinterpret_cast<char*>(result_buf)); // NOLINT
}

galois::Result<std::unique_ptr<FileAsyncWork>>
AzureStorage::ListAsync(const std::string& directory,
                        std::unordered_set<std::string>* list) {
  auto uri_res = CleanURI(std::string(directory));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzureListAsync(container, blob, list);
}

galois::Result<void>
AzureStorage::Delete(const std::string& directory,
                     const std::unordered_set<std::string>& files) {
  auto uri_res = CleanURI(std::string(directory));
  if (!uri_res) {
    return uri_res.error();
  }
  auto [container, blob] = std::move(uri_res.value());
  return AzureDelete(container, blob, files);
}

} // namespace tsuba
