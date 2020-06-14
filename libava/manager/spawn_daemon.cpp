#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <nvml.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

class DaemonConfig {
public:
  static std::string const DefaultManagerAddress;
  static int const DefaultWorkerPortBase;

  DaemonConfig(std::string cf, std::string wp,
      std::string ma = DefaultManagerAddress, int wpb = DefaultWorkerPortBase) :
    config_file(cf), worker_path(wp), manager_address(ma), worker_port_base(wpb) {}

  void print() {
    std::cerr << "* Manager address: " << manager_address << std::endl
      << "* API server: " << worker_path << std::endl
      << "* Total GPU: " << visible_cuda_device.size() << std::endl;
    for (unsigned i = 0; i < visible_cuda_device_uuid.size(); ++i)
      std::cerr << "  - GPU-" << i << " UUID is " << visible_cuda_device_uuid[i] << std::endl;
  }

  std::string config_file;
  std::string worker_path;
  std::string manager_address;
  int worker_port_base;

  std::vector<std::string> visible_cuda_device;
  std::vector<std::string> visible_cuda_device_uuid;
};

std::string const DaemonConfig::DefaultManagerAddress = "0.0.0.0:3333";
int const DaemonConfig::DefaultWorkerPortBase = 4000;

DaemonConfig *parse_arguments(int argc, char *argv[]) {
  int c;
  opterr = 0;
  char *config_file_name = NULL;
  const char *worker_relative_path = NULL;
  char worker_path[PATH_MAX];
  std::string manager_address = DaemonConfig::DefaultManagerAddress;
  int worker_port_base = DaemonConfig::DefaultWorkerPortBase;

  while ((c = getopt(argc, argv, "f:w:m:p:")) != -1) {
    switch (c) {
    case 'f':
      config_file_name = optarg;
      break;
    case 'w':
      worker_relative_path = optarg;
      break;
    case 'm':
      manager_address = optarg;
      break;
    case 'p':
      worker_port_base = (uint32_t)atoi(optarg);
      break;
    default:
      fprintf(stderr, "Usage: %s <-f config_file_name> "
                      "<-w worker_path {./worker}> "
                      "[-m manager_address {%s}] "
                      "[-p worker_port_base {%d}]\n",
              argv[0], DaemonConfig::DefaultManagerAddress.c_str(), DaemonConfig::DefaultWorkerPortBase);
      exit(EXIT_FAILURE);
    }
  }

  if (config_file_name == NULL) {
    fprintf(stderr, "-f is mandatory. Please specify config file name\n");
    exit(EXIT_FAILURE);
  }
  if (worker_relative_path == NULL) {
    fprintf(stderr, "-w is mandatory. Please specify path to API server executable\n");
    exit(EXIT_FAILURE);
  }
  if (!realpath(worker_relative_path, worker_path)) {
    fprintf(stderr, "Worker binary (%s) not found. -w is optional\n", worker_relative_path);
    exit(EXIT_FAILURE);
  }

  auto config = new DaemonConfig(config_file_name, worker_path, manager_address, worker_port_base);
  return config;
}

void parse_config_file(DaemonConfig *config) {
  std::ifstream config_file(config->config_file);
  std::string line;
  nvmlReturn_t ret = nvmlInit();

  if (ret != NVML_SUCCESS) {
    fprintf(stderr, "Fail to get device by uuid: %s\n", nvmlErrorString(ret));
    exit(-1);
  }

  while (std::getline(config_file, line)) {
    nvmlDevice_t dev;
    nvmlMemory_t mem = {};
    char *line_cstr = (char *)line.c_str();
    char *pchr = strchr(line_cstr, '=');

    ret = nvmlDeviceGetHandleByUUID(pchr + 1, &dev);
    if (ret != NVML_SUCCESS) {
      fprintf(stderr, "Fail to get device by uuid: %s\n", nvmlErrorString(ret));
      exit(-1);
    }

    ret = nvmlDeviceGetMemoryInfo(dev, &mem);
    if (ret != NVML_SUCCESS) {
      fprintf(stderr, "Fail to get device by uuid: %s\n", nvmlErrorString(ret));
      exit(-1);
    }

    config->visible_cuda_device.push_back(line);
    config->visible_cuda_device_uuid.push_back(pchr + 1);
  }
}

int main(int argc, char *argv[]) {
  auto config = parse_arguments(argc, argv);
  parse_config_file(config);
  config->print();

  return 0;
}
