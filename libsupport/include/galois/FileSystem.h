#ifndef GALOIS_LIBSUPPORT_GALOIS_FILESYSTEM_H_
#define GALOIS_LIBSUPPORT_GALOIS_FILESYSTEM_H_

#include <string_view>
#include <string>

#include "galois/config.h"
#include "galois/Result.h"

namespace galois {

/// Create a file with the path: ${prefix}${unique number}${suffix}
GALOIS_EXPORT Result<std::string>
CreateUniqueFile(std::string_view prefix, std::string_view suffix = "");

/// Create a file with the path: ${prefix}${unique number}${suffix} open the
/// file and return an open file descriptor to the file
GALOIS_EXPORT Result<std::pair<std::string, int>>
OpenUniqueFile(std::string_view prefix, std::string_view sufix = "");

/// Create a unique directory with the path: ${prefix}${unique number}
GALOIS_EXPORT Result<std::string>
CreateUniqueDirectory(std::string_view prefix);

/// NewPath returns a new path in a directory with the given prefix. It works
/// by appending a random suffix. The generated paths may not be unique due
/// to the varying atomicity guarantees of future storage backends.
GALOIS_EXPORT Result<std::string> NewPath(const std::string& dir,
                                          const std::string& prefix);

// Return the filename portion of a path
GALOIS_EXPORT Result<std::string> ExtractFileName(const std::string& path);
// Return the "directory" portion of a path
GALOIS_EXPORT Result<std::string> ExtractDirName(const std::string& path);

} // namespace galois

#endif
