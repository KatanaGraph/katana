// Run many trials of tsuba_fault
#include <stdlib.h>
#include <spawn.h>
#include <sys/wait.h>

#include <vector>

#include "galois/Logging.h"
#include "galois/Result.h"

std::string src_uri{};
int64_t num_threads{1L};
uint64_t run_length_limit{UINT64_C(0)};
int32_t node_property_total{0}; // Which node property
float independent_failure_probability{0.0f};

std::string prog_name = "tsuba_fault_runner";
std::string usage_msg = "Usage: {} <RDG URI>\n"
                        "  [-t] number of threads (default=1)\n"
                        "  [-r] Test runs up to argument (default=0)\n"
                        "  [-n] Total number of node properties (default=0)\n"
                        "  [-h] usage message\n";

void parse_arguments(int argc, char* argv[]) {
  int c;

  while ((c = getopt(argc, argv, "i:t:r:n:h")) != -1) {
    switch (c) {
    case 't': {
      char* p_end{nullptr};
      num_threads = std::strtol(optarg, &p_end, 10);
      if (optarg == p_end) {
        fmt::print(stderr, "Can't parse -t number of threads argument\n");
        fmt::print(stderr, usage_msg, argv[0]);
        exit(EXIT_FAILURE);
      }
    } break;
    case 'n':
      node_property_total = std::atoi(optarg);
      break;
    case 'i':
      independent_failure_probability = std::atof(optarg);
      break;
    case 'r': {
      char* p_end{nullptr};
      run_length_limit = std::strtoul(optarg, &p_end, 10);
      if (optarg == p_end) {
        fmt::print(stderr, "Can't parse -r run length limit\n");
        fmt::print(stderr, usage_msg, prog_name);
        exit(EXIT_FAILURE);
      }
    } break;
    case 'h':
      fmt::print(stderr, usage_msg, prog_name);
      exit(0);
      break;
    default:
      fmt::print(stderr, "Bad option {}\n", (char)c);
      fmt::print(stderr, usage_msg, prog_name);
      exit(EXIT_FAILURE);
    }
  }

  // TODO: Validate paths
  auto index = optind;
  if (index >= argc) {
    fmt::print("{} requires property graph URI argument\n", prog_name);
    exit(EXIT_FAILURE);
  }

  src_uri = argv[index++];
}

int CrashAndVerify(const std::string& uri_in, char* const* envp,
                   const std::string& rl, const std::string& node_prop_num) {
  int status;
  pid_t len_pid;
  pid_t verify_pid;
  // -c ensures we modify graph enough times to call PtP atleast rl times
  const char* const fault_len_argv[] = {"bin/tsuba_fault",
                                        "-r",
                                        rl.c_str(),
                                        "-c",
                                        rl.c_str(),
                                        uri_in.c_str(),
                                        "-n",
                                        node_prop_num.c_str(),
                                        nullptr};
  status = posix_spawn(&len_pid, fault_len_argv[0], NULL, NULL,
                       const_cast<char* const*>(fault_len_argv), envp);
  if (status == 0) {
    if (waitpid(len_pid, &status, 0) != -1) {
      const char* const verify_argv[] = {"bin/tsuba_fault", "-v",
                                         uri_in.c_str(), NULL};
      status = posix_spawn(&verify_pid, verify_argv[0], NULL, NULL,
                           const_cast<char* const*>(verify_argv), envp);
      if (status == 0) {
        if (waitpid(verify_pid, &status, 0) == -1) {
          GALOIS_LOG_WARN("Verify waitpid: {}",
                          galois::ResultErrno().message());
        }
      } else {
        GALOIS_LOG_WARN("Fault waitpid: {}", galois::ResultErrno().message());
      }
    }
  } else {
    GALOIS_LOG_WARN("Fault waitpid: {}", std::strerror(status));
  }
  return status;
}

int RunLenFaulty(const std::string& uri_in, char* const* envp,
                 int64_t run_len_limit) {

  // Run len must be at least 1
  for (auto i = 1; i < run_len_limit; ++i) {
    std::string rl  = fmt::format("{:d}", i);
    std::string npn = fmt::format("{:d}", i % node_property_total);
    CrashAndVerify(uri_in, envp, rl, npn);
  }

  return 0;
}

int main(int argc, char* argv[], char* const* envp) {
  parse_arguments(argc, argv);

  if (run_length_limit > UINT64_C(0)) {
    RunLenFaulty(src_uri, envp, run_length_limit);
  }

  return 0;
}
