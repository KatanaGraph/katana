#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <nvml.h>
#include <grpc++/grpc++.h>

#include <algorithm>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "manager.h"
#include "manager_service.grpc.fb.h"
#include "manager_service_generated.h"
#include "common/cmd_channel_impl.h"
#include "common/cmd_handler.h"
#include "common/socket.h"

int listen_fd;

__sighandler_t original_sigint_handler = SIG_DFL;

void sigint_handler(int signo) {
  if (listen_fd > 0)
    close(listen_fd);
  signal(signo, original_sigint_handler);
  raise(signo);
}

class ManagerConfig {
public:
  static int const kDefaultManagerPort;
  static int const kDefaultWorkerPoolSize;

  ManagerConfig(int mp = kDefaultManagerPort, int wps = kDefaultWorkerPoolSize)
      : manager_port_(mp), worker_pool_size_(wps) {}

  void Print() {
    std::cerr << "* Manager port: " << manager_port_ << std::endl
              << "* API server pool size: " << worker_pool_size_ << std::endl;
  }

  int manager_port_;
  int worker_pool_size_;
  std::vector<std::unique_ptr<DaemonInfo>> daemons_;
};

int const ManagerConfig::kDefaultManagerPort    = 3334;
int const ManagerConfig::kDefaultWorkerPoolSize = 3;

std::shared_ptr<ManagerConfig> config;

std::shared_ptr<ManagerConfig> parseArguments(int argc, char* argv[]) {
  int c;
  opterr               = 0;
  int manager_port     = ManagerConfig::kDefaultManagerPort;
  int worker_pool_size = ManagerConfig::kDefaultWorkerPoolSize;

  while ((c = getopt(argc, argv, "m:n:")) != -1) {
    switch (c) {
    case 'm':
      manager_port = (uint32_t)atoi(optarg);
      break;
    case 'n':
      worker_pool_size = (uint32_t)atoi(optarg);
      break;
    default:
      fprintf(stderr,
              "Usage: %s "
              "[-m manager_port {%d}] "
              "[-n worker_pool_size {%d}]\n",
              argv[0], ManagerConfig::kDefaultManagerPort,
              ManagerConfig::kDefaultWorkerPoolSize);
      exit(EXIT_FAILURE);
    }
  }

  return std::make_shared<ManagerConfig>(manager_port, worker_pool_size);
}

class DaemonServiceClient {
public:
  DaemonServiceClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(DaemonService::NewStub(channel)) {}

  std::vector<std::string> SpawnWorker(std::vector<int>& count,
                                       std::vector<std::string>& uuid,
                                       std::string daemon_ip = "0.0.0.0") {
    /* Build message. */
    flatbuffers::grpc::MessageBuilder mb;
    std::vector<flatbuffers::Offset<flatbuffers::String>> _uuid;
    for (auto const& uu : uuid)
      _uuid.push_back(mb.CreateString(uu));
    auto cnt_offset     = mb.CreateVector(&count[0], count.size());
    auto uu_offset      = mb.CreateVector(&_uuid[0], _uuid.size());
    auto request_offset = CreateWorkerSpawnRequest(mb, cnt_offset, uu_offset);
    mb.Finish(request_offset);
    auto request_msg = mb.ReleaseMessage<WorkerSpawnRequest>();

    /* Send request. */
    flatbuffers::grpc::Message<WorkerSpawnReply> response_msg;
    grpc::ClientContext context;
    auto status = stub_->SpawnWorker(&context, request_msg, &response_msg);

    /* Parse response. */
    std::vector<std::string> worker_address;
    if (status.ok()) {
      const WorkerSpawnReply* response = response_msg.GetRoot();
      auto wa                          = response->worker_address();
      for (auto const& addr_offset : *wa) {
        std::string _wa = daemon_ip + ":" + addr_offset->str();
        std::cerr << "Register API server at " << _wa << std::endl;
        worker_address.push_back(_wa);
      }
    } else {
      std::cerr << status.error_code() << ": " << status.error_message()
                << std::endl;
    }
    return worker_address;
  }

private:
  std::unique_ptr<DaemonService::Stub> stub_;
};

class ManagerServiceImpl final : public ManagerService::Service {
  virtual grpc::Status RegisterDaemon(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<DaemonRegisterRequest>* request_msg,
      flatbuffers::grpc::Message<DaemonRegisterReply>* response_msg) override {
    const DaemonRegisterRequest* request = request_msg->GetRoot();
    std::string peer_address =
        context->peer().substr(context->peer().find(':') + 1);
    const std::string daemon_ip =
        peer_address.substr(0, peer_address.find(':'));
    const std::string daemon_address =
        daemon_ip + ":" + request->daemon_address()->str();
    std::cerr << "Register spawn daemon at " << daemon_address << std::endl;

    /* Register GPU information in a global table */
    auto daemon_info = std::make_unique<DaemonInfo>();
    daemon_info->ip_ = daemon_ip;
    for (auto const& uu_offset : *(request->uuid()))
      daemon_info->gpu_info_.push_back({uu_offset->str(), 0});
    int idx = 0;
    for (auto fm : *(request->free_memory())) {
      daemon_info->gpu_info_[idx].free_memory_ = fm;
      ++idx;
    }
    daemon_info->PrintGpuInfo();

    /* Request daemon to spawn an API server pool.
     * Currently each API server can see only one GPU, and every GPU has
     * `config->worker_pool_sizea_` API servers running on it. */
    auto channel =
        grpc::CreateChannel(daemon_address, grpc::InsecureChannelCredentials());
    daemon_info->client_ = std::make_unique<DaemonServiceClient>(channel);
    std::vector<int> count;
    std::vector<std::string> uuid;
    for (auto const& gi : daemon_info->gpu_info_) {
      count.push_back(config->worker_pool_size_);
      uuid.push_back(gi.uuid_);
    }
    std::vector<std::string> worker_address =
        daemon_info->client_->SpawnWorker(count, uuid, daemon_ip);

    /* Register API servers in a global table */
    int k = 0;
    for (unsigned i = 0; i < count.size(); ++i)
      for (int j = 0; j < count[i]; ++j) {
        daemon_info->workers_.Enqueue(worker_address[k], uuid[i]);
        ++k;
      }

    config->daemons_.push_back(std::move(daemon_info));
    return grpc::Status::OK;
  }

  virtual grpc::Status AssignWorker(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<WorkerAssignRequest>* request_msg,
      flatbuffers::grpc::Message<WorkerAssignReply>* response_msg) override {
    const WorkerAssignRequest* request = request_msg->GetRoot();
    int worker_count                   = request->worker_count();
    int gpu_count                      = request->gpu_count();
    std::vector<uint64_t> gpu_mem;
    if (request->gpu_mem()) {
      for (auto const& gm : *(request->gpu_mem())) {
        std::cerr << "Request GPU with " << (gm >> 20) << " MB free memory"
                  << std::endl;
        gpu_mem.push_back(gm);
      }
    }
    if (gpu_mem.size() != (size_t)gpu_count)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Mismatched gpu_count and gpu_mem vector");

    std::vector<std::string> assigned_workers =
        DoAssignWorker(worker_count, gpu_count, gpu_mem);
    if (assigned_workers.empty())
      return grpc::Status(
          grpc::StatusCode::UNAVAILABLE,
          "Failed to assign API servers: insufficient resource");

    /* Return assigned API servers. */
    std::vector<flatbuffers::Offset<flatbuffers::String>> worker_address;
    flatbuffers::grpc::MessageBuilder mb;
    for (auto const& worker : assigned_workers)
      worker_address.push_back(mb.CreateString(worker));
    auto wa_offset = mb.CreateVector(&worker_address[0], worker_address.size());
    auto response_offset = CreateWorkerAssignReply(mb, wa_offset);
    mb.Finish(response_offset);
    *response_msg = mb.ReleaseMessage<WorkerAssignReply>();

    return grpc::Status::OK;
  }

private:
  std::vector<std::string> DoAssignWorker(int worker_count, int gpu_count,
                                          std::vector<uint64_t>& gpu_mem) {
    std::vector<std::string> assigned_workers;

    /* If resource is insufficient, return an empty vector. */
    // TODO

    /* API server assignment policy.
     *
     * Currently it is assumed that every API server is running on only one GPU.
     * The policy simply assigns an available API server to the application;
     * if no idle API server exists, the manager requests a daemon to spawn
     * a new API server.
     *
     * The manager has the information of the free GPU memory on each GPU node,
     * while this information is not currently used as we need annotations of
     * every application's GPU usage or retrieve GPU usage dynamically.
     *
     * `config->daemons_` should be protected with locks in case of hot-plugged
     * spawn daemons. For now it is assumed that all daemons have been spawned
     * before any guestlib connects in.
     */
    for (auto const& daemon : config->daemons_) {
      auto worker = daemon->workers_.Dequeue();
      if (!worker.first.empty()) {
        /* Find an idle API server, respond guestlib and spawn a new idle
         * worker. */
        std::cerr << "[" << __func__ << "] Assign " << worker.first
                  << std::endl;
        assigned_workers.push_back(worker.first);

        std::vector<int> count        = {1};
        std::vector<std::string> uuid = {worker.second};
        std::vector<std::string> worker_address =
            daemon->client_->SpawnWorker(count, uuid, daemon->ip_);
        daemon->workers_.Enqueue(worker_address[0], worker.second);
        return assigned_workers;
      }
    }

    /* No idle API server was found, spawn a new API server.
     * Currently, we constantly spawn the new API server on the first GPU of the
     * first GPU node (which is assumed to exist).
     */
    std::vector<int> count        = {1};
    std::vector<std::string> uuid = {config->daemons_[0]->gpu_info_[0].uuid_};
    std::vector<std::string> worker_address =
        config->daemons_[0]->client_->SpawnWorker(count, uuid,
                                                  config->daemons_[0]->ip_);
    assigned_workers.push_back(worker_address.empty() ? "0.0.0.0:0"
                                                      : worker_address[0]);
    std::cerr << "[" << __func__ << "] Assign " << assigned_workers.back()
              << std::endl;

    return assigned_workers;
  }
};

void runManagerService(std::shared_ptr<ManagerConfig> config) {
  std::string server_address("0.0.0.0:" +
                             std::to_string(config->manager_port_));
  ManagerServiceImpl service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cerr << "Manager Service listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char* argv[]) {
  config = parseArguments(argc, argv);
  config->Print();

  std::thread server_thread(runManagerService, config);
  server_thread.join();

  return 0;
}
