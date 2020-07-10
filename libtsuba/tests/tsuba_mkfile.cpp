#include <cerrno>
#include <cstdlib>
#include <string>
#include <unistd.h>

#include <vector>
#include <limits>
#include <numeric>

#include "galois/Logging.h"
#include "galois/Platform.h"
#include "tsuba/tsuba.h"
#include "tsuba/file.h"

uint64_t bytes_to_write{0};
std::string dst_path{};

static constexpr auto chars = "0123456789"
                              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                              "abcdefghijklmnopqrstuvwxyz";
static auto chars_len = std::strlen(chars);

std::string usage_msg = "Usage: {} <number>[G|M|K|B] <full path>\n";

// 19 chars, with 1 null byte
void get_time_string(char* buf, int32_t limit) {
  time_t rawtime;
  struct tm* timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);
  strftime(buf, limit, "%Y/%m/%d %H:%M:%S ", timeinfo);
}

void init_data(uint8_t* buf, int32_t limit) {
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
      *buf++            = ' ';
      uint64_t char_idx = 0UL;
      for (limit -= 20; limit; limit--) {
        *buf++ = chars[char_idx++ % chars_len]; // We could make this faster...
      }
    }
  }
}

void parse_arguments(int argc, char* argv[]) {
  int c;
  char* p_end{nullptr};

  while ((c = getopt(argc, argv, "h")) != -1) {
    switch (c) {
    case 'h':
      fmt::print(stderr, usage_msg, argv[0]);
      exit(0);
      break;
    default:
      fmt::print(stderr, usage_msg, argv[0]);
      exit(EXIT_FAILURE);
    }
  }

  auto index = optind;
  GALOIS_LOG_VASSERT(index < argc,
                     "\n  Usage: {} <number>[G|M|K|B] <full path>\n", argv[0]);
  bytes_to_write = std::strtoul(argv[index], &p_end, 10);
  if (argv[index] == p_end) {
    fmt::print(stderr, "Can't parse first argument (size)\n");
    fmt::print(stderr, usage_msg, argv[0]);
    exit(EXIT_FAILURE);
  }
  switch (*p_end) {
  case 'G':
    bytes_to_write *= (1UL << 30);
    break;
  case 'M':
    bytes_to_write *= (1UL << 20);
    break;
  case 'K':
    bytes_to_write *= (1UL << 10);
    break;
  case 'B':
    break;
  default:
    fmt::print(stderr, "First argument must end in G|M|K|B\n");
    fmt::print(stderr, usage_msg, argv[0]);
    exit(EXIT_FAILURE);
  }

  GALOIS_LOG_VASSERT(argc > index,
                     "\n  Usage: {} <number>[G|M|K|B] <full path>\n", argv[0]);
  index++;
  dst_path = argv[index];
}

uint8_t* mymmap(uint64_t size) {
  auto res = galois::MmapPopulate(nullptr, size, PROT_READ | PROT_WRITE,
                                  MAP_ANONYMOUS | MAP_PRIVATE, -1, (off_t)0);
  if (res == MAP_FAILED) {
    perror("mmap");
    return nullptr;
  }
  return static_cast<uint8_t*>(res);
}

int main(int argc, char* argv[]) {
  if (auto init_good = tsuba::Init(); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }
  parse_arguments(argc, argv);
  uint8_t* buf = static_cast<uint8_t*>(malloc(bytes_to_write));
  if (buf == nullptr) {
    fmt::print(stderr, "malloc failed, trying mmap\n");
    buf = mymmap(bytes_to_write);
    if (buf == nullptr) {
      fmt::print(stderr, "Mmap failed\n");
      exit(EXIT_FAILURE);
    }
  }
  init_data(buf, bytes_to_write);

  fmt::print("Writing {}\n", dst_path);
  if (auto res = tsuba::FileStore(dst_path, buf, bytes_to_write); res != 0) {
    fmt::print(stderr, "FileStore error {:d}\n", res);
    exit(EXIT_FAILURE);
  }

  return 0;
}
