#include "LocalStorage.h"

#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include <boost/filesystem.hpp>

#include "galois/Result.h"
#include "galois/Logging.h"
#include "tsuba/file.h"
#include "tsuba/Errors.h"

namespace fs = boost::filesystem;

namespace tsuba {

void LocalStorage::CleanURI(std::string* uri) {
  if (uri->find(uri_scheme()) != 0) {
    return;
  }
  *uri = std::string(uri->begin() + uri_scheme().size(), uri->end());
}

galois::Result<void>
LocalStorage::WriteFile(std::string uri, const uint8_t* data, uint64_t size) {
  CleanURI(&uri);
  std::ofstream ofile(uri);
  if (!ofile.good()) {
    return galois::ResultErrno();
  }
  ofile.write(reinterpret_cast<const char*>(data), size); /* NOLINT */
  if (!ofile.good()) {
    return galois::ResultErrno();
  }
  return galois::ResultSuccess();
}

galois::Result<void> LocalStorage::ReadFile(std::string uri, uint64_t start,
                                            uint64_t size, uint8_t* data) {
  CleanURI(&uri);
  std::ifstream ifile(uri);
  if (!ifile.good()) {
    GALOIS_LOG_DEBUG("Failed to create ifstream");
    return galois::ResultErrno();
  }
  ifile.seekg(start);
  if (!ifile.good()) {
    GALOIS_LOG_DEBUG("Failed to seek");
    return galois::ResultErrno();
  }
  ifile.read(reinterpret_cast<char*>(data), size); /* NOLINT */

  // if the difference in what was read from what we wanted is less  than a
  // block it's because the file size isn't well aligned so don't complain.
  if (size - ifile.gcount() > kBlockSize) {
    return galois::ResultErrno();
  }
  return galois::ResultSuccess();
}

galois::Result<void> LocalStorage::Stat(const std::string& uri,
                                        StatBuf* s_buf) {
  std::string filename = uri;
  CleanURI(&filename);
  struct stat local_s_buf;
  if (int ret = stat(uri.c_str(), &local_s_buf); ret) {
    return galois::ResultErrno();
  }
  s_buf->size = local_s_buf.st_size;
  return galois::ResultSuccess();
}

galois::Result<void> LocalStorage::Create(const std::string& uri,
                                          bool overwrite) {
  std::string filename = uri;
  CleanURI(&filename);
  fs::path m_path{filename};
  if (overwrite && fs::exists(m_path)) {
    return ErrorCode::Exists;
  }
  fs::path dir = m_path.parent_path();
  if (boost::system::error_code err; !fs::create_directories(dir, err)) {
    if (err) {
      return err;
    }
  }
  // Creates an empty file
  std::ofstream output(m_path.string());
  return galois::ResultSuccess();
}

galois::Result<std::unique_ptr<FileAsyncWork>>
LocalStorage::ListAsync(const std::string& directory,
                        std::vector<std::string>& list_out) {
  // Implement with synchronous calls
  DIR* dirp;
  struct dirent* dp;
  list_out.clear();
  if ((dirp = opendir(directory.c_str())) == NULL) {
    GALOIS_LOG_ERROR("\n  Open dir failed: {}: {}", directory,
                     galois::ResultErrno().message());
    return ErrorCode::InvalidArgument;
  }

  do {
    errno = 0;
    if ((dp = readdir(dirp)) != NULL) {
      list_out.emplace_back(dp->d_name);
    }
  } while (dp != NULL);

  if (errno != 0) {
    GALOIS_LOG_ERROR("\n  Open dir failed: {}: {}", directory,
                     galois::ResultErrno().message());
    return ErrorCode::FilesystemError;
  }
  (void)closedir(dirp);

  return nullptr;
}

} // namespace tsuba
