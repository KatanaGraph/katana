#ifndef GALOIS_LIBTSUBA_GS_H_
#define GALOIS_LIBTSUBA_GS_H_

#include <cstdint>
#include <future>
#include <string>
#include <unordered_set>

#include "galois/Result.h"

namespace tsuba {

galois::Result<void> GSInit();
galois::Result<void> GSFini();
galois::Result<void> GSGetSize(
    const std::string& bucket, const std::string& object, uint64_t* size);
galois::Result<bool> GSExists(
    const std::string& bucket, const std::string& object);

galois::Result<void> GSGetSync(
    const std::string& bucket, const std::string& object, uint64_t start,
    uint64_t size, uint8_t* result_buf);

galois::Result<void> GSPutSync(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size);

std::future<galois::Result<void>> GSGetAsync(
    const std::string& bucket, const std::string& object, uint64_t start,
    uint64_t size, uint8_t* result_buf);

std::future<galois::Result<void>> GSPutAsync(
    const std::string& bucket, const std::string& object, const uint8_t* data,
    uint64_t size);

std::future<galois::Result<void>> GSListAsync(
    const std::string& bucket, const std::string& object,
    std::vector<std::string>* list, std::vector<uint64_t>* size);

galois::Result<void> GSDelete(
    const std::string& bucket, const std::string& object,
    const std::unordered_set<std::string>& files);

} /* namespace tsuba */

#endif
