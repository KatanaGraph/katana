#ifndef GALOIS_LIBTSUBA_AZURE_H_
#define GALOIS_LIBTSUBA_AZURE_H_

#include <cstdint>
#include <future>
#include <string>
#include <string_view>
#include <unordered_set>

#include "galois/Result.h"

namespace tsuba {

galois::Result<void> AzureInit();
galois::Result<void> AzureFini();
galois::Result<void> AzureGetSize(
    const std::string& container, const std::string& blob, uint64_t* size);
galois::Result<bool> AzureExists(
    const std::string& container, const std::string& blob);

galois::Result<void> AzureGetSync(
    const std::string& container, const std::string& blob, uint64_t start,
    uint64_t size, char* result_buf);

galois::Result<void> AzurePutSync(
    const std::string& container, const std::string& blob, const char* data,
    uint64_t size);

galois::Result<std::future<galois::Result<void>>> AzureGetAsync(
    const std::string& container, const std::string& blob, uint64_t start,
    uint64_t size, char* result_buf);

galois::Result<std::future<galois::Result<void>>> AzurePutAsync(
    const std::string& container, const std::string& blob, const char* data,
    uint64_t size);

galois::Result<std::future<galois::Result<void>>> AzureListAsync(
    const std::string& container, const std::string& blob,
    std::vector<std::string>* list, std::vector<uint64_t>* size);

galois::Result<void> AzureDelete(
    const std::string& container, const std::string& blob,
    const std::unordered_set<std::string>& files);

} /* namespace tsuba */

#endif
