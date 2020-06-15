#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <nvml.h>
#include <grpc++/grpc++.h>

#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "manager_service.grpc.fb.h"
#include "manager_service_generated.h"

class DaemonConfig {
public:
  static std::string const DefaultManagerAddress;
  static int const DefaultDaemonPort;
  static int const DefaultWorkerPortBase;

  DaemonConfig(std::string cf, std::string wp,
      std::string ma = DefaultManagerAddress, int dp = DefaultDaemonPort, int wpb = DefaultWorkerPortBase) :
    config_file(cf), worker_path(wp), manager_address(ma), daemon_port(dp), worker_port_base(wpb) {}

  void print() {
    std::cerr << "* Manager address: " << manager_address << std::endl
      << "* Daemon port: " << daemon_port << std::endl
      << "* API server: " << worker_path << std::endl
      << "* Total GPU: " << visible_cuda_device.size() << std::endl;
    for (unsigned i = 0; i < visible_cuda_device_uuid.size(); ++i)
      std::cerr << "  - GPU-" << i << " UUID is " << visible_cuda_device_uuid[i] << std::endl;
  }

  std::string config_file;
  std::string worker_path;
  std::string manager_address;
  int daemon_port;
  int worker_port_base;

  std::vector<std::string> visible_cuda_device;
  std::vector<std::string> visible_cuda_device_uuid;
};

std::string const DaemonConfig::DefaultManagerAddress = "0.0.0.0:3333";
int const DaemonConfig::DefaultDaemonPort= 3334;
int const DaemonConfig::DefaultWorkerPortBase = 4000;

DaemonConfig *parse_arguments(int argc, char *argv[]) {
  int c;
  opterr = 0;
  char *config_file_name = NULL;
  const char *worker_relative_path = NULL;
  char worker_path[PATH_MAX];
  std::string manager_address = DaemonConfig::DefaultManagerAddress;
  int daemon_port = DaemonConfig::DefaultDaemonPort;
  int worker_port_base = DaemonConfig::DefaultWorkerPortBase;

  while ((c = getopt(argc, argv, "f:w:m:p:b:")) != -1) {
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
      daemon_port = atoi(optarg);
      break;
    case 'b':
      worker_port_base = atoi(optarg);
      break;
    default:
      fprintf(stderr, "Usage: %s <-f config_file_name> "
                      "<-w worker_path {./worker}> "
                      "[-m manager_address {%s}] "
                      "[-p daemon_port {%d}] "
                      "[-b worker_port_base {%d}]\n",
              argv[0], DaemonConfig::DefaultManagerAddress.c_str(),
              DaemonConfig::DefaultDaemonPort, DaemonConfig::DefaultWorkerPortBase);
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

  auto config = new DaemonConfig(config_file_name, worker_path, manager_address,
      daemon_port, worker_port_base);
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

class DaemonServiceImpl final : public DaemonService::Service {
  virtual grpc::Status SpawnWorker(
      grpc::ServerContext *context,
      const flatbuffers::grpc::Message<WorkerSpawnRequest> *request_msg,
      flatbuffers::grpc::Message<WorkerSpawnReply> *response_msg) override {
    const WorkerSpawnRequest *request = request_msg->GetRoot();
    std::cerr << "Request to spawn " << request->spawn_num() << " API servers" << std::endl;

    std::vector<flatbuffers::Offset<flatbuffers::String>> worker_address;
    flatbuffers::grpc::MessageBuilder mb;

    /* Spawn API servers. */
    // TODO
    auto ao = mb.CreateString("port_number");
    worker_address.push_back(ao);

    auto vo = mb.CreateVector(worker_address.empty() ? nullptr : &worker_address[0],
        worker_address.size());
    auto response_offset = CreateWorkerSpawnReply(mb, vo);
    mb.Finish(response_offset);
    *response_msg = mb.ReleaseMessage<WorkerSpawnReply>();

    return grpc::Status::OK;
  }
};

void run_daemon_service(DaemonConfig *config) {
  std::string server_address("0.0.0.0:" + std::to_string(config->daemon_port));
  DaemonServiceImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cerr << "Daemon Service listening on " << server_address << std::endl;
  server->Wait();
}

class ManagerServiceClient {
public:
  ManagerServiceClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(ManagerService::NewStub(channel)) {}

  grpc::Status RegisterDaemon(const std::string &self_address) {
    /* Build message. */
    flatbuffers::grpc::MessageBuilder mb;
    auto sa_offset = mb.CreateString(self_address);
    auto request_offset = CreateDaemonRegisterRequest(mb, sa_offset);
    mb.Finish(request_offset);
    auto request_msg = mb.ReleaseMessage<DaemonRegisterRequest>();

    /* Send request. */
    grpc::ClientContext context;
    auto status = stub_->RegisterDaemon(&context, request_msg, nullptr);
    if (!status.ok()) {
      std::cerr << status.error_code() << ": " << status.error_message()
                << std::endl;
    }
    return status;
  }

private:
  std::unique_ptr<ManagerService::Stub> stub_;
};

int main(int argc, char *argv[]) {
  auto config = parse_arguments(argc, argv);
  parse_config_file(config);
  config->print();

  std::thread server_thread(run_daemon_service, config);

  /* Register daemon. */
  auto channel = grpc::CreateChannel(config->manager_address, grpc::InsecureChannelCredentials());
  auto client = new ManagerServiceClient(channel);
  auto status = client->RegisterDaemon(std::to_string(config->daemon_port));

  server_thread.join();

  return 0;
}
