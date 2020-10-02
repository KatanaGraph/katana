#include "LocalStorage.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include <boost/filesystem.hpp>

#include "GlobalState.h"
#include "galois/FileSystem.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace fs = boost::filesystem;

namespace tsuba {

GlobalFileStorageAllocator local_storage_allocator([]() {
  return std::unique_ptr<FileStorage>(new LocalStorage());
});

void
LocalStorage::CleanUri(std::string* uri) {
  if (uri->find(uri_scheme()) != 0) {
    return;
  }
  *uri = std::string(uri->begin() + uri_scheme().size(), uri->end());
}

galois::Result<void>
LocalStorage::WriteFile(std::string uri, const uint8_t* data, uint64_t size) {
  CleanUri(&uri);
  fs::path m_path{uri};
  fs::path dir = m_path.parent_path();
  if (boost::system::error_code err; !fs::create_directories(dir, err)) {
    if (err) {
      return err;
    }
  }

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

galois::Result<void>
LocalStorage::ReadFile(
    std::string uri, uint64_t start, uint64_t size, uint8_t* data) {
  CleanUri(&uri);
  std::ifstream ifile(uri);

  ifile.seekg(start);
  if (!ifile) {
    GALOIS_LOG_DEBUG("failed to seek");
    return galois::ResultErrno();
  }

  ifile.read(reinterpret_cast<char*>(data), size); /* NOLINT */
  if (!ifile) {
    GALOIS_LOG_DEBUG("failed to read");
    return galois::ResultErrno();
  }

  // if the difference in what was read from what we wanted is less  than a
  // block it's because the file size isn't well aligned so don't complain.
  if (size - ifile.gcount() > kBlockSize) {
    return galois::ResultErrno();
  }
  return galois::ResultSuccess();
}

galois::Result<void>
LocalStorage::Stat(const std::string& uri, StatBuf* s_buf) {
  std::string filename = uri;
  CleanUri(&filename);
  struct stat local_s_buf;

  if (int ret = stat(filename.c_str(), &local_s_buf); ret) {
    return galois::ResultErrno();
  }
  s_buf->size = local_s_buf.st_size;
  return galois::ResultSuccess();
}

galois::Result<void>
LocalStorage::Create(const std::string& uri, bool overwrite) {
  std::string filename = uri;
  CleanUri(&filename);
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

// Current implementation is not async
std::future<galois::Result<void>>
LocalStorage::ListAsync(
    const std::string& uri, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  // Implement with synchronous calls
  DIR* dirp;
  struct dirent* dp;
  std::string dirname = uri;
  CleanUri(&dirname);

  if ((dirp = opendir(dirname.c_str())) == nullptr) {
    if (errno == ENOENT) {
      // other storage backends are flat and so return an empty list here
      return std::async(
          []() -> galois::Result<void> { return galois::ResultSuccess(); });
    }
    GALOIS_LOG_DEBUG(
        "\n  Open dir failed: {}: {}", dirname,
        galois::ResultErrno().message());
    return std::async(
        []() -> galois::Result<void> { return galois::ResultErrno(); });
  }

  int dfd = dirfd(dirp);
  struct stat stat_buf;
  do {
    errno = 0;
    if ((dp = readdir(dirp)) != nullptr) {
      // I am filtering "." and ".." from local listing because I can't see how
      // to filter in clients in a reasonable way.
      if (strcmp(".", dp->d_name) && strcmp("..", dp->d_name)) {
        list->emplace_back(dp->d_name);
        if (size) {
          if (fstatat(dfd, dp->d_name, &stat_buf, 0) == 0) {
            size->emplace_back(stat_buf.st_size);
          } else {
            size->emplace_back(0UL);
            GALOIS_LOG_DEBUG(
                "dir file stat failed dir: {} file: {} : {}", dirname,
                dp->d_name, galois::ResultErrno().message());
          }
        }
      }
    }
  } while (dp != nullptr);

  if (errno != 0) {
    GALOIS_LOG_ERROR(
        "\n  readdir failed: {}: {}", dirname, galois::ResultErrno().message());
    return std::async(
        []() -> galois::Result<void> { return ErrorCode::LocalStorageError; });
  }
  (void)closedir(dirp);

  return std::async(
      []() -> galois::Result<void> { return galois::ResultSuccess(); });
}

galois::Result<void>
LocalStorage::Delete(
    const std::string& directory_uri,
    const std::unordered_set<std::string>& files) {
  std::string dir = directory_uri;
  CleanUri(&dir);
  for (const auto& file : files) {
    auto path = galois::JoinPath(dir, file);
    unlink(path.c_str());
  }
  return galois::ResultSuccess();
}

}  // namespace tsuba
