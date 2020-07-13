// Run some quick, basic sanity checks on tsuba
#include "galois/Logging.h"
#include "galois/FileSystem.h"
#include "galois/Random.h"

#include <stdlib.h>
// remove_all (rm -rf) is just too sweet
#include <boost/filesystem.hpp>

#include <vector>

constexpr uint8_t kPathSep = '/';

enum class TestType {
  kSystem,
  kMDsum,
};
struct Test {
  TestType type_;
  std::string name_;
  std::vector<std::string> cmd_;
  Test(TestType type, const std::string& name, const std::string& cmd)
      : type_(type), name_(name) {
    cmd_.push_back(cmd);
  }
  Test(TestType type, const std::string& name,
       const std::vector<std::string>& cmd)
      : type_(type), name_(name), cmd_(cmd) {}
};

std::string Bytes2Str(uint64_t bytes) {
  for (auto unit : {"B", "KB", "MB", "GB", "TB"}) {
    if (bytes < 1024) {
      // tsuba_mkfile assume no space between number and unit
      return fmt::format("{:d}{}", bytes, unit);
    }
    bytes >>= 10;
  }
  return "Invalid size";
}

void MkCpSumLocal(uint64_t num_bytes, const std::string& local,
                  const std::string& s3, std::vector<Test>& tests) {
  std::string bytes_str = Bytes2Str(num_bytes);
  tests.push_back(Test(TestType::kSystem,
                       fmt::format("Make a local file ({})", bytes_str),
                       fmt::format("tsuba_mkfile {} {}", bytes_str, local)));
  tests.push_back(Test(TestType::kSystem,
                       fmt::format("Copy local file to S3 ({})", bytes_str),
                       fmt::format("tsuba_cp {} {}", local, s3)));
  std::vector<std::string> cmds{fmt::format("tsuba_md5sum {}", local),
                                fmt::format("tsuba_md5sum {}", s3)};
  tests.push_back(Test(
      TestType::kMDsum,
      fmt::format("Compare MD5sum of local and remote files ({})", bytes_str),
      cmds));
  // Note, no tsuba_rm yet
  tests.push_back(Test(TestType::kSystem, "Remove S3 file via aws cli",
                       fmt::format("aws s3 rm {}", s3)));
}

void MkCpSumS3(uint64_t num_bytes, const std::string& local,
               const std::string& s3, std::vector<Test>& tests) {
  std::string bytes_str = Bytes2Str(num_bytes);
  tests.push_back(Test(TestType::kSystem,
                       fmt::format("Make S3 file ({})", bytes_str),
                       fmt::format("tsuba_mkfile {} {}", bytes_str, s3)));
  tests.push_back(Test(TestType::kSystem,
                       fmt::format("Copy S3 file to local ({})", bytes_str),
                       fmt::format("tsuba_cp {} {}", s3, local)));
  std::vector<std::string> cmds{fmt::format("tsuba_md5sum {}", local),
                                fmt::format("tsuba_md5sum {}", s3)};
  tests.push_back(Test(
      TestType::kMDsum,
      fmt::format("Compare MD5sum of local and remote files ({})", bytes_str),
      cmds));
  // Note, no tsuba_rm yet and local directory cleaned at end.
  tests.push_back(Test(TestType::kSystem, "Remove S3 file via aws cli",
                       fmt::format("aws s3 rm {}", s3)));
}

std::vector<Test> ConstructTests(std::string local_dir, std::string s3_dir) {
  std::vector<Test> tests;
  std::string rnd_str = galois::generate_random_alphanumeric_string(12);
  if (local_dir.back() != kPathSep) {
    local_dir.push_back(kPathSep);
  }
  std::string local_rnd = local_dir + "ci-test-" + rnd_str;
  std::string s3_rnd    = std::string(s3_dir) + "ci-test-" + rnd_str;

  // Each of these could be done on a different thread
  MkCpSumLocal(8, local_rnd, s3_rnd, tests);
  MkCpSumLocal(1UL << 13, local_rnd, s3_rnd, tests);
  MkCpSumLocal(1UL << 15, local_rnd, s3_rnd, tests);

  MkCpSumS3(15, local_rnd, s3_rnd, tests);
  MkCpSumS3((1UL << 13) - 1, local_rnd, s3_rnd, tests);
  MkCpSumS3((1UL << 15) - 1, local_rnd, s3_rnd, tests);

  return tests;
}

int RunPopen(const std::string& cmd, std::string& out) {
  char buff[4096];
  FILE* fp = popen(cmd.c_str(), "r");
  if (fp == NULL) {
    return -1;
  }
  while (fgets(buff, sizeof(buff), fp)) {
    out += buff;
  }
  pclose(fp);
  return 0;
}

int MD5sumRun(const std::string& cmd, std::string& out) {
  std::string cmd_out;
  int res = RunPopen(cmd, cmd_out);
  if (res != 0) {
    return res;
  }

  std::size_t first_space = cmd_out.find(' ');
  if (first_space == std::string::npos) {
    return -1;
  }
  out = cmd_out.substr(0, first_space);
  return 0;
}

int main() {
  int main_ret = 0;
  // Not sure if this PATH hack is kosher or treif
  std::string path = getenv("PATH");
  path.insert(0, "bin:");
  setenv("PATH", path.c_str(), 1);
  // CI buckets are in us-east-1
  setenv("AWS_DEFAULT_REGION", "us-east-1", 1);

  auto unique_result = galois::CreateUniqueDirectory("/tmp/tsuba_basic_quick-");
  GALOIS_LOG_ASSERT(unique_result);
  std::string tmp_dir(std::move(unique_result.value()));
  auto tests = ConstructTests(tmp_dir, "s3://katana-ci/delete_me/");

  for (auto const& test : tests) {
    fmt::print("Running: {}\n", test.name_);
    switch (test.type_) {
    case TestType::kSystem: {
      int res = system(test.cmd_[0].c_str());
      if (res != 0) {
        main_ret = EXIT_FAILURE;
        fmt::print("Test FAILED\n");
        break;
      }
    } break;
    case TestType::kMDsum: {
      std::vector<std::string> results;
      for (auto const& cmd : test.cmd_) {
        std::string out;
        int res = MD5sumRun(cmd, out);
        if (res != 0) {
          main_ret = EXIT_FAILURE;
          fmt::print("Test FAILED\n");
          break;
        }
        results.push_back(out);
      }
      // Now test that they are all equal
      auto first = results[0];
      for (auto const& result : results) {
        if (first != result) {
          fmt::print("Test FAILED, outputs\n  first: {}\n  other: {}\n", first,
                     result);
          main_ret = EXIT_FAILURE;
          break;
        }
      }
    } break;
    default:
      fmt::print("Unknown test type {}\n", test.name_);
      main_ret = EXIT_FAILURE;
    }
  }

  boost::filesystem::remove_all(tmp_dir);
  return main_ret;
}
