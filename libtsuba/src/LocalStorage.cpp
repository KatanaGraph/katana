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
#include "katana/ErrorCode.h"
#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/URI.h"
#include "katana/file.h"

namespace fs = boost::filesystem;

namespace {

katana::Result<void>
EnsureDirectories(const std::string& path) {
  fs::path m_path{path};
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
  return katana::ResultSuccess();
}

katana::Result<std::string>
GetPath(const std::string& uri) {
  auto u = KATANA_CHECKED(katana::URI::Make(uri));
  return u.path();
}

}  // namespace

katana::Result<void>
katana::LocalStorage::WriteFile(
    const std::string& uri, const uint8_t* data, uint64_t size) {
  std::string path = KATANA_CHECKED(GetPath(uri));
  KATANA_CHECKED(EnsureDirectories(path));

  std::ofstream ofile(path);
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
katana::LocalStorage::RemoteCopyFile(
    const std::string& source_uri, const std::string& dest_uri, uint64_t begin,
    uint64_t size) {
  std::string source_path = KATANA_CHECKED(GetPath(source_uri));
  std::string dest_path = KATANA_CHECKED(GetPath(dest_uri));

  KATANA_CHECKED(EnsureDirectories(dest_path));

  std::ifstream ifile(source_path, std::ios_base::binary);
  if (!ifile) {
    return KATANA_ERROR(
        ErrorCode::LocalStorageError, "failed to open source file");
  }
  ifile.seekg(begin, std::ios_base::beg);

  std::ofstream ofile(dest_path, std::ios_base::binary | std::ios_base::trunc);
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
katana::LocalStorage::ReadFile(
    const std::string& uri, uint64_t start, uint64_t size, uint8_t* data) {
  std::string path = KATANA_CHECKED(GetPath(uri));
  std::ifstream ifile(path, std::ios_base::binary);
  if (!ifile) {
    return KATANA_ERROR(
        ErrorCode::LocalStorageError, "failed to open source file {}",
        std::quoted(path));
  }

  ifile.seekg(start);
  if (!ifile) {
    return KATANA_ERROR(
        ErrorCode::LocalStorageError, "failed to seek to offset {}", start);
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
katana::LocalStorage::Stat(const std::string& uri, StatBuf* s_buf) {
  std::string path = KATANA_CHECKED(GetPath(uri));
  struct stat local_s_buf;

  if (int ret = stat(path.c_str(), &local_s_buf); ret) {
    return katana::ResultErrno();
  }
  s_buf->size = local_s_buf.st_size;
  return katana::ResultSuccess();
}

// Current implementation is not async
std::future<katana::CopyableResult<void>>
katana::LocalStorage::ListAsync(
    const std::string& uri, std::vector<std::string>* list,
    std::vector<uint64_t>* size) {
  // Implement with synchronous calls
  DIR* dirp{};
  struct dirent* dp{};

  auto get_path_res = GetPath(uri);

  if (!get_path_res) {
    CopyableErrorInfo ei = get_path_res.error();
    return std::async(
        std::launch::deferred, [ei, uri]() -> katana::CopyableResult<void> {
          return KATANA_ERROR(
              ErrorCode::LocalStorageError, "list dir failed: {}: {}", uri, ei);
        });
  }

  std::string dirname = std::move(get_path_res.value());

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
                "dir file stat failed dir: {} file: {}: {}", dirname,
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
katana::LocalStorage::Delete(
    const std::string& directory_uri,
    const std::unordered_set<std::string>& files) {
  std::string dir = KATANA_CHECKED(GetPath(directory_uri));

  if (files.empty()) {
    rmdir(dir.c_str());
  } else {
    for (const auto& file : files) {
      auto path = katana::URI::JoinPath(dir, file);
      unlink(path.c_str());
    }
  }
  return katana::ResultSuccess();
}
