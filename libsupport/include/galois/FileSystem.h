#ifndef GALOIS_LIBSUPPORT_GALOIS_FILE_SYSTEM_H_
#define GALOIS_LIBSUPPORT_GALOIS_FILE_SYSTEM_H_

#include <string_view>
#include <string>

#include <boost/outcome/outcome.hpp>

#include "galois/Result.h"

namespace galois {

/// Create a file with the path: ${prefix}${unique number}${suffix}
Result<std::string> CreateUniqueFile(std::string_view prefix,
                                     std::string_view suffix = "");

/// Create a file with the path: ${prefix}${unique number}${suffix} open the
/// file and return an open file descriptor to the file
Result<std::pair<std::string, int>> OpenUniqueFile(std::string_view prefix,
                                                   std::string_view sufix = "");

/// Create a unique directory with the path: ${prefix}${unique number}
Result<std::string> CreateUniqueDirectory(std::string_view prefix);

} // namespace galois

#endif
