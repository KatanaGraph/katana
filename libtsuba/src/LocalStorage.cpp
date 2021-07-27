#include "LocalStorage.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <system_error>

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

#include "GlobalState.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "tsuba/Errors.h"
#include "tsuba/file.h"

namespace fs = boost::filesystem;

void
tsuba::LocalStorage::CleanUri(std::string* uri) {
  if (uri->find(uri_scheme()) != 0) {
    return;
  }
  *uri = std::string(uri->begin() + uri_scheme().size(), uri->end());
}

katana::Result<void>
tsuba::LocalStorage::WriteFile(
    std::string uri, const uint8_t* data, uint64_t size) {
  CleanUri(&uri);
  fs::path m_path{uri};
  fs::path dir = m_path.parent_path();
  if (!dir.empty()) {
    if (boost::system::error_code err; !fs::create_directories(dir, err)) {
      if (err) {
        return KATANA_ERROR(
            std::error_code(err.value(), err.category()),
            "creating parent directories: {}", err.message());
      }
    }
  }

  std::ofstream ofile(uri);
  if (!ofile.good()) {
    return KATANA_ERROR(
        ErrorCode::LocalStorageError, "opening file: {}", strerror(errno));
  }
  ofile.write(reinterpret_cast<const char*>(data), size); /* NOLINT */
  if (!ofile.good()) {
    return KATANA_ERROR(ErrorCode::LocalStorageError, "writing file");
  }
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::LocalStorage::RemoteCopyFile(
    std::string source_uri, std::string dest_uri, uint64_t begin,
    uint64_t size) {
  CleanUri(&source_uri);
  CleanUri(&dest_uri);

  std::ifstream ifile(source_uri, std::ios_base::binary);
  if (!ifile) {
    return KATANA_ERROR(
        ErrorCode::LocalStorageError, "failed to open source file");
  }
  ifile.seekg(begin, std::ios_base::beg);

  std::ofstream ofile(dest_uri, std::ios_base::binary | std::ios_base::trunc);
  if (!ofile) {
    return KATANA_ERROR(
        ErrorCode::LocalStorageError, "failed to open dest file");
  }

  std::copy_n(
      std::istreambuf_iterator<char>(ifile), size,
      std::ostreambuf_iterator<char>(ofile));
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::LocalStorage::ReadFile(
    std::string uri, uint64_t start, uint64_t size, uint8_t* data) {
  CleanUri(&uri);
  std::ifstream ifile(uri);

  ifile.seekg(start);
  if (!ifile) {
    return KATANA_ERROR(ErrorCode::LocalStorageError, "failed to seek");
  }

  ifile.read(reinterpret_cast<char*>(data), size); /* NOLINT */
  if (!ifile) {
    return KATANA_ERROR(ErrorCode::LocalStorageError, "failed to read");
  }

  // if the difference in what was read from what we wanted is less  than a
  // block it's because the file size isn't well aligned so don't complain.
  if (size - ifile.gcount() > kBlockSize) {
    return ErrorCode::LocalStorageError;
  }
  return katana::ResultSuccess();
}

katana::Result<void>
tsuba::LocalStorage::Stat(const std::string& uri, StatBuf* s_buf) {
  std::string filename = uri;
  CleanUri(&filename);
  struct stat local_s_buf;

  if (int ret = stat(filename.c_str(), &local_s_buf); ret) {
    return katana::ResultErrno();
  }
  s_buf->size = local_s_buf.st_size;
  return katana::ResultSuccess();
}

// Current implementation is not async
std::future<katana::CopyableResult<void>>
tsuba::LocalStorage::ListAsync(
    const std::string& uri, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  // Implement with synchronous calls
  DIR* dirp{};
  struct dirent* dp{};
  std::string dirname = uri;
  CleanUri(&dirname);

  if ((dirp = opendir(dirname.c_str())) == nullptr) {
    if (errno == ENOENT) {
      // other storage backends are flat and so return an empty list here
      return std::async(
          std::launch::deferred, []() -> katana::CopyableResult<void> {
            return katana::CopyableResultSuccess();
          });
    }

    std::error_code ec = katana::ResultErrno();

    return std::async(
        std::launch::deferred, [ec, dirname]() -> katana::CopyableResult<void> {
          return KATANA_ERROR(
              ErrorCode::LocalStorageError, "open dir failed: {}: {}", dirname,
              ec.message());
        });
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
            KATANA_LOG_DEBUG(
                "dir file stat failed dir: {} file: {} : {}", dirname,
                dp->d_name, katana::ResultErrno().message());
          }
        }
      }
    }
  } while (dp != nullptr);

  if (errno != 0) {
    std::error_code ec = katana::ResultErrno();

    return std::async(
        std::launch::deferred, [ec, dirname]() -> katana::CopyableResult<void> {
          return KATANA_ERROR(
              ErrorCode::LocalStorageError, "readdir failed: {}: {}", dirname,
              ec.message());
        });
  }

  (void)closedir(dirp);

  return std::async(
      std::launch::deferred, []() -> katana::CopyableResult<void> {
        return katana::CopyableResultSuccess();
      });
}

katana::Result<void>
tsuba::LocalStorage::Delete(
    const std::string& directory_uri,
    const std::unordered_set<std::string>& files) {
  std::string dir = directory_uri;
  CleanUri(&dir);

  if (files.empty()) {
    rmdir(dir.c_str());
  } else {
    for (const auto& file : files) {
      auto path = katana::Uri::JoinPath(dir, file);
      unlink(path.c_str());
    }
  }
  return katana::ResultSuccess();
}
