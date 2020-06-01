#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>

#include <cstring>
#include <vector>
#include <iostream>
#include <limits>
#include <string>
#include <iomanip>

#include "tsuba/tsuba_api.h"

constexpr static const char* const output_fmt =
    "%24s (%4d) %7ld us/op %4ld ms/op\n";
constexpr static const char* const s3_url_base =
    "s3://witchel-tests-east2/test-";

// TODO: 2020/06/15 - Across different regions

/******************************************************************************/
/* Utilities */

struct timespec now() {
  struct timespec tp;
  int ret = clock_gettime(CLOCK_BOOTTIME, &tp);
  if (ret < 0) {
    perror("clock_gettime");
    std::cerr << "Bad return" << std::endl;
  }
  return tp;
}

struct timespec timespec_sub(struct timespec time, struct timespec oldTime) {
  if (time.tv_nsec < oldTime.tv_nsec)
    return (struct timespec){.tv_sec  = time.tv_sec - 1 - oldTime.tv_sec,
                             .tv_nsec = 1'000'000'000L + time.tv_nsec -
                                        oldTime.tv_nsec};
  else
    return (struct timespec){.tv_sec  = time.tv_sec - oldTime.tv_sec,
                             .tv_nsec = time.tv_nsec - oldTime.tv_nsec};
}

long timespec_to_us(struct timespec ts) {
  return ts.tv_sec * 1'000'000 + ts.tv_nsec / 1'000;
}

// 21 chars, with 1 null byte
void get_time_string(char* buf, int limit) {
  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buf, limit, "%Y/%m/%d %H:%M:%S ", timeinfo);
}

void init_data(uint8_t* buf, int limit) {
  if (limit < 0)
    return;
  if (limit < 19) {
    for (; limit; limit--) {
      *buf++ = 'a';
    }
    return;
  } else {
    char tmp[32];             // Generous with space
    get_time_string(tmp, 31); // Trailing null
                              // Copy without trailing null
    memcpy(buf, tmp, 19);
    buf += 19;
    if (limit > 19) {
      *buf++ = ' ';
      for (limit -= 20; limit; limit--) {
        *buf++ = 'a';
      }
    }
  }
}

// https://stackoverflow.com/questions/5590381/easiest-way-to-convert-int-to-string-in-c
// variadic template
template <typename... Args>
std::string sstr(Args&&... args) {
  std::ostringstream sstr;
  // fold expression
  (sstr << ... << args);
  return sstr.str();
}
std::string fillstr(int i, int width) {
  std::ostringstream sstr;
  sstr << std::setw(width) << std::setfill('0') << i;
  return sstr.str();
}

/******************************************************************************/
/* S3 interaction */

// 2020/06/06 - 33-63 us/op
void test_mem(int fd_batch, const uint8_t* data, uint64_t size) {
  std::vector<int> fds(fd_batch, 0);

  struct timespec start = now();
  for (int i = 0; i < fd_batch; ++i) {
    fds[i] = memfd_create(fillstr(i, 4).c_str(), 0);
    if (fds[i] < 0) {
      std::cerr << "fd " << fillstr(i, 4) << std::endl;
      perror("memfd_create");
    }
    ssize_t bwritten = write(fds[i], data, size);
    if (bwritten != (ssize_t)size) {
      perror("write");
      std::cerr << "Short write tried " << size << " did " << bwritten
                << std::endl;
    }
  }
  long us = timespec_to_us(timespec_sub(now(), start));
  printf(output_fmt, "memfd_create", fd_batch, (us / fd_batch),
         (us / (fd_batch * 1000)));

  for (int i = 0; i < fd_batch; ++i) {
    int sysret = close(fds[i]);
    if (sysret < 0) {
      perror("close");
    }
  }
}

// 2020/06/06 - 192-210 us/op (single sync at end)
// 2020/06/10 - 2.0-2.5 ms/op each open synced
void test_tmp(int fd_batch, const uint8_t* data, uint64_t size) {
  std::vector<int> fds(fd_batch, 0);
  std::vector<std::string> fnames;
  for (int i = 0; i < fd_batch; ++i) {
    fnames.push_back(sstr("/tmp/witchel/", fillstr(i, 4)));
  }

  struct timespec start = now();
  for (int i = 0; i < fd_batch; ++i) {
    fds[i] =
        open(fnames[i].c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRWXU | S_IRWXG);
    if (fds[i] < 0) {
      perror("/tmp O_CREAT");
      std::cerr << "fd " << i << std::endl;
    }
    ssize_t bwritten = write(fds[i], data, size);
    if (bwritten != (ssize_t)size) {
      perror("write");
      std::cerr << "Short write tried " << size << " did " << bwritten
                << std::endl;
    }
    // Make all data and directory changes persistent
    // sync is overkill, could sync fd and parent directory, but I'm being lazy
    sync();
  }
  for (int i = 0; i < fd_batch; ++i) {
    int sysret = close(fds[i]);
    if (sysret < 0) {
      perror("close");
    }
  }
  long us = timespec_to_us(timespec_sub(now(), start));
  printf(output_fmt, "/tmp create", fd_batch, (us / fd_batch),
         (us / (fd_batch * 1000)));

  for (int i = 0; i < fd_batch; ++i) {
    int sysret = unlink(fnames[i].c_str());
    if (sysret < 0) {
      perror("unlink");
    }
  }
}

// 2020/06/06 - 297ms/create us-east-2b to bucket in us-east-1 36 or 3 threads
// 2020/06/08 - 166ms/create us-east-2b to bucket in us-east-2 3 threads.
// 2020/06/10 - 152ms/create us-east-2b to bucket in us-east-2 36 threads.
// 2020/06/10 - 143-145ms/create us-east-2b to bucket in us-east-2 36 threads.
void test_s3_objs(int fd_batch, const uint8_t* data, uint64_t size) {
  std::vector<int> fds(fd_batch, 0);
  struct timespec start = now();
  for (int i = 0; i < fd_batch; ++i) {
    std::string s3url = sstr(s3_url_base, fillstr(i, 4));
    int tsubaret      = TsubaStore(s3url.c_str(), data, size);
    if (tsubaret != 0) {
      std::cerr << "Tsuba bad return " << tsubaret << std::endl;
      perror("s3");
    }
  }
  long us = timespec_to_us(timespec_sub(now(), start));
  printf(output_fmt, "S3 create", fd_batch, (us / fd_batch),
         (us / (fd_batch * 1000)));
}

// 2020/06/11 - 37ms
void test_s3_objs_sync(int fd_batch, const uint8_t* data, uint64_t size) {
  std::vector<int> fds(fd_batch, 0);
  std::vector<std::string> s3urls;
  for (int i = 0; i < fd_batch; ++i) {
    s3urls.push_back(sstr(s3_url_base, fillstr(i, 4)));
  }

  struct timespec start = now();
  for (int i = 0; i < fd_batch; ++i) {
    // Current API rejects empty writes
    int tsubaret = TsubaStoreSync(s3urls[i].c_str(), data, size);
    if (tsubaret != 0) {
      std::cerr << "Tsuba store sync bad return " << tsubaret << std::endl;
    }
  }
  long us = timespec_to_us(timespec_sub(now(), start));
  printf(output_fmt, "S3 sync create", fd_batch, (us / fd_batch),
         (us / (fd_batch * 1000)));
}

// 2020/06/10 - 19-21 ms/op,
void test_s3_objs_async_one(int fd_batch, const uint8_t* data, uint64_t size) {
  std::vector<int> fds(fd_batch, 0);
  std::vector<std::string> s3urls;
  for (int i = 0; i < fd_batch; ++i) {
    s3urls.push_back(sstr(s3_url_base, fillstr(i, 4)));
  }

  struct timespec start = now();
  for (int i = 0; i < fd_batch; ++i) {
    // Current API rejects empty writes
    int tsubaret = TsubaStoreAsync(s3urls[i].c_str(), data, size);
    if (tsubaret != 0) {
      std::cerr << "Tsuba store async bad return " << tsubaret << std::endl;
    }
    // Only 1 outstanding store at a time
    TsubaStoreAsyncFinish(s3urls[i].c_str());
  }
  long us = timespec_to_us(timespec_sub(now(), start));
  printf(output_fmt, "S3 async create one", fd_batch, (us / fd_batch),
         (us / (fd_batch * 1000)));
}

// 2020/06/10 - 0.98ms/op, 36 threads, batch size 1024
void test_s3_objs_async_batch(int fd_batch, const uint8_t* data,
                              uint64_t size) {
  std::vector<int> fds(fd_batch, 0);
  std::vector<std::string> s3urls;
  for (int i = 0; i < fd_batch; ++i) {
    s3urls.push_back(sstr(s3_url_base, fillstr(i, 4)));
  }

  struct timespec start = now();
  for (int i = 0; i < fd_batch; ++i) {
    // Current API rejects empty writes
    int tsubaret = TsubaStoreAsync(s3urls[i].c_str(), data, size);
    if (tsubaret != 0) {
      std::cerr << "Tsuba store async bad return " << tsubaret << std::endl;
    }
  }
  for (int i = 0; i < fd_batch; ++i) {
    int ret = TsubaStoreAsyncFinish(s3urls[i].c_str());
    if (ret != 0) {
      std::cerr << "TsubaStoreAsyncFinish bad return " << ret << std::endl;
    }
  }
  long us = timespec_to_us(timespec_sub(now(), start));
  printf(output_fmt, "S3 async create batch", fd_batch, (us / fd_batch),
         (us / (fd_batch * 1000)));
}

/******************************************************************************/
/* Main */

static uint8_t data_19B[19];
static uint8_t data_10MB[10 * (1 << 20)];
static uint8_t data_100MB[100 * (1 << 20)];

struct {
  uint8_t* data;
  uint64_t size;
  int fd_batch;
  const char* name;
} arr[] = {
    {.data     = data_19B,
     .size     = sizeof(data_19B),
     .fd_batch = 1024,
     .name     = "  19B"},
    {.data     = data_10MB,
     .size     = sizeof(data_10MB),
     .fd_batch = 128,
     .name     = " 10MB"},
    {.data     = data_100MB,
     .size     = sizeof(data_100MB),
     .fd_batch = 16,
     .name     = "100MB"},
};

int main() {
  TsubaInit(); // Done in Galois code
  for (unsigned long i = 0; i < sizeof(arr) / sizeof(arr[0]); ++i) {
    init_data(arr[i].data, arr[i].size);
  }
  int fd_batch = arr[0].fd_batch;

  // Make sure resources are ready and avilable
  struct rlimit rlim;
  int sysret;
  sysret = getrlimit(RLIMIT_NOFILE, &rlim);
  if (sysret < 0) {
    perror("getrlimit");
  }
  if ((rlim.rlim_cur + 5) > (rlim_t)fd_batch) { // +5 stdin/out/err+slop
    std::cerr << "Increase open file descriptors to " << (fd_batch * 2)
              << std::endl;
    rlim.rlim_cur = fd_batch * 2; // +5 probably sufficient
    sysret        = setrlimit(RLIMIT_NOFILE, &rlim);
    if (sysret < 0) {
      perror("setrlimit");
    }
  }
  if (access("/tmp/witchel", R_OK) != 0) {
    perror("Can't access /tmp/witchel");
    exit(10);
  }

  printf("*** VM and bucket same region\n");
  for (unsigned long i = 0; i < sizeof(arr) / sizeof(arr[0]); ++i) {
    printf("** size %s\n", arr[i].name);
    const uint8_t* data = arr[i].data;
    uint64_t size       = arr[i].size;
    fd_batch            = arr[i].fd_batch;
    test_mem(fd_batch, data, size);
    test_tmp(fd_batch, data, size);
    test_s3_objs_async_batch(fd_batch, data, size);
    test_s3_objs_async_one(fd_batch / 3, data, size);
    test_s3_objs_sync(fd_batch / 3, data, size);
    test_s3_objs(fd_batch / 3, data, size);
  }

  return 0;
}

// 2020/06/12

// ** size   19B
//             memfd_create (1024)      44 us/op  0 ms/op
//              /tmp create (1024)    2753 us/op  2 ms/op
//    S3 async create batch (1024)    1080 us/op  1 ms/op
//      S3 async create one ( 341)   20523 us/op 20 ms/op
//           S3 sync create ( 341)   36578 us/op 36 ms/op
//                S3 create ( 341)  147186 us/op 147 ms/op
// ** size  10MB
//             memfd_create ( 128)    4037 us/op  4 ms/op
//              /tmp create ( 128)   74648 us/op 74 ms/op
//    S3 async create batch ( 128)   86280 us/op 86 ms/op
//      S3 async create one (  42)  189471 us/op 189 ms/op
//           S3 sync create (  42)  214694 us/op 214 ms/op
//                S3 create (  42)  318105 us/op 318 ms/op
// ** size 100MB
//             memfd_create (  16)   53287 us/op 53 ms/op
//              /tmp create (  16)  761514 us/op 761 ms/op
//    S3 async create batch (  16)  864999 us/op 864 ms/op
//      S3 async create one (   5) 1233452 us/op 1233 ms/op
//           S3 sync create (   5) 1212307 us/op 1212 ms/op
//                S3 create (   5) 1067922 us/op 1067 ms/op
