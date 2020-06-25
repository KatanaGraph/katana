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

  std::shared_ptr<DaemonInfo> FindDaemonByIp(std::string ip) {
    for (auto& d : daemons_) {
      if (d->ip_ == ip)
        return d;
    }
    return nullptr;
  }

  void Print() {
    std::cerr << "* Manager port: " << manager_port_ << std::endl
              << "* API server pool size: " << worker_pool_size_ << std::endl;
  }

  int manager_port_;
  int worker_pool_size_;
  std::vector<std::shared_ptr<DaemonInfo>> daemons_;
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

    /**
     * Register GPU information in a global table.
     * 1. Every GPU server has a `DaemonInfo`.
     * 2. Every daemon has a `GpuList`, consisting of a number of
     * `GpuListEntry`. Other attributes: IP address.
     * 3. Every `GpuListEntry` has a (pooled) idle `Worker` queue, a (running)
     * busy `Worker` queue and a `GpuInfo`. (Busy `Worker` queue: the daemon
     * monitors the API server's termination and reports it to the manager. The
     * manager looks up the API server in this queue by the daemon's IP, GPU's
     * UUID and API server's address.) Other attributes: a raw pointer to its
     * `DaemonInfo` and a raw pointer to its `GpuList`.
     * 4. Every `GpuInfo` contains the GPU's UUID and free memory size.
     * 5. Every `WorkerInfo` contains the API server's address, used GPU memory
     * size. Other attributes: a raw pointer to its parent `GpuListEntry`.
     */
    auto daemon_info = std::make_shared<DaemonInfo>();
    std::vector<std::shared_ptr<GpuListEntry>> gpu_entries;
    daemon_info->ip_ = daemon_ip;
    for (auto const& uu_offset : *(request->uuid())) {
      auto entry = std::make_shared<GpuListEntry>(daemon_info.get(),
                                                  &daemon_info->gpu_list_);
      entry->SetUuid(uu_offset->str());
      gpu_entries.push_back(entry);
    }
    int idx = 0;
    for (auto fm : *(request->free_memory())) {
      gpu_entries[idx]->SetFreeMemory(fm);
      ++idx;
    }
    daemon_info->gpu_list_.AddEntries(gpu_entries);
    daemon_info->PrintGpuInfo();

    /* Request daemon to spawn an API server pool.
     * Currently each API server can see only one GPU, and every GPU has
     * `config->worker_pool_sizea_` API servers running on it. */
    auto channel =
        grpc::CreateChannel(daemon_address, grpc::InsecureChannelCredentials());
    daemon_info->client_ = std::make_unique<DaemonServiceClient>(channel);
    std::vector<int> count;
    std::vector<std::string> uuid;
    for (auto const& entry : gpu_entries) {
      count.push_back(config->worker_pool_size_);
      uuid.push_back(entry->GetUuid());
    }
    std::vector<std::string> worker_address =
        daemon_info->client_->SpawnWorker(count, uuid, daemon_ip);

    /* Register API servers in a global table */
    int k = 0;
    for (unsigned i = 0; i < count.size(); ++i)
      for (int j = 0; j < count[i]; ++j) {
        if (k >= worker_address.size())
          break;
        daemon_info->gpu_list_.GetEntryAtIndex(i)->AddIdleWorker(
            worker_address[k]);
        ++k;
      }

    config->daemons_.push_back(daemon_info);
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
        std::cerr << "[" << context->peer() << "] Request GPU with "
                  << (gm >> 20) << " MB free memory" << std::endl;
        gpu_mem.push_back(gm);
      }
    }
    if (gpu_mem.size() != (size_t)gpu_count)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Mismatched gpu_count and gpu_mem vector");

    std::vector<std::string> assigned_workers =
        DoAssignWorker(worker_count, gpu_mem);
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

  virtual grpc::Status NotifyWorkerExit(
      grpc::ServerContext* context,
      const flatbuffers::grpc::Message<WorkerExitNotifyRequest>* request_msg,
      flatbuffers::grpc::Message<WorkerExitNotifyReply>* response_msg)
      override {
    const WorkerExitNotifyRequest* request = request_msg->GetRoot();
    std::string peer_address =
        context->peer().substr(context->peer().find(':') + 1);
    const std::string worker_ip =
        peer_address.substr(0, peer_address.find(':'));
    const std::string worker_address =
        worker_ip + ":" + request->worker_address()->str();
    const std::string gpu_uuid = request->uuid()->str();
    std::cerr << "API server (" << gpu_uuid << ") at " << worker_address
              << " has exit" << std::endl;

    /* Find daemon. */
    auto daemon_info = config->FindDaemonByIp(worker_ip);
    if (!daemon_info)
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Invalid API server address");

    /* Find GPU. */
    auto entry = daemon_info->gpu_list_.FindEntryByUuid(gpu_uuid);

    /* Reclaim GPU memory. */
    entry->RemoveBusyWorker(worker_address);
    entry->PrintGpuInfo();
    return grpc::Status::OK;
  }

private:
  std::vector<std::string> DoAssignWorker(int worker_count,
                                          std::vector<uint64_t>& gpu_mem) {
    std::vector<std::string> assigned_workers;
    if (gpu_mem.empty())
      return assigned_workers;

    /**
     * Rule 1:
     * `@worker_count` is not used in this policy, but may be used as a hint for
     * other policies.
     *
     * API server assignment policy.
     * Rule 2:
     * The policy assigns `@gpu_count` API servers to the application.
     * Rule 3:
     * Every API server can see only one GPU on the node. Two assigned API
     * servers may see the same GPU (provisioning).
     *
     * GPU assignment policy.
     * Rule 4:
     * GPUs on the same node are preferred than GPUs distributed on multiple
     * nodes. The nodes (daemons) are checked in the round-robin order.
     * Rule 5:
     * Under Rule 4, If the GPU memory is enough, the GPU with fewer running API
     * servers will be assigned first.
     * Rule 6:
     * Under Rule 5, the GPU with more available memory will be assigned first.
     *
     * Pooling.
     * Rule 7.
     * After assigning an API server on a GPU, the manager shall request to
     * replenish the API server pool on that GPU. If no idle API server exists
     * on the GPU, the manager requests the daemon to spawn a new API server.
     *
     * Data structure.
     * The manager has the information of the free GPU memory on each GPU node,
     * and saves it in a list of available GPUs. The GPU list is sorted by the
     * number of running API servers on the GPUs, or by the available memory if
     * the numbers are the same. The GPU list is protected by a big
     * lock--daemons may add new GPUs to the list and applications may request
     * to consume GPUs from the list concurrently. The lock can be
     * finer-granularity.
     *
     * Algorithm.
     * The input `@gpu_mem` is sorted and the request with larger GPU memory is
     * processed first.
     * For each requested GPU, the algorithm iterates the GPU list to find a GPU
     * with enough memory. Then the GPU's available memory is updated, and the
     * GPU list is resorted (it can be done by an O(N) bubble sort, or simply by
     * std::sort whose performance is also close to O(N)). If there is no such
     * available GPU, all updates to the GPU list are revoked, and an empty
     * `@worker_address` vector is returned to the application.
     *
     * Oversubscription.
     * The GPU memory oversubscription can be supported with CUDA UVM. The
     * method is to implement `cudaMalloc` with `cudaMallocManaged` on the API
     * server. This is a
     * TODO task.
     */

    size_t gpu_count         = gpu_mem.size();
    std::vector<uint64_t> gm = gpu_mem;
    std::sort(gm.begin(), gm.end(), std::greater<uint64_t>());

    std::vector<std::shared_ptr<GpuListEntry>> assigned_entries;
    unsigned daemon_idx = 0;
    for (unsigned i = 0; i < gpu_count; ++i) {
      std::shared_ptr<GpuListEntry> entry;
      while (!entry && daemon_idx < config->daemons_.size()) {
        entry =
            config->daemons_[daemon_idx]->gpu_list_.FindEntryAndReserveMemory(
                gm[i]);
        if (!entry)
          ++daemon_idx;
        else
          entry->PrintGpuInfo();
      }

      /* Revoke any request cannot be satisfied. */
      if (!entry) {
        for (unsigned j = 0; j < assigned_entries.size(); ++j) {
          auto entry = assigned_entries[j];
          entry->GetGpuList()->RevokeEntryWithMemory(entry, gm[j]);
        }
        assigned_entries.clear();
        break;
      } else
        assigned_entries.push_back(entry);
    }

    /* If the resource is insufficient, return an empty vector. */
    if (assigned_entries.empty())
      return assigned_workers;

    /* Assign an API server from each entry. */
    for (unsigned i = 0; i < gpu_count; ++i) {
      auto entry  = assigned_entries[i];
      auto worker = entry->PopIdleWorker();
      if (worker) {
        /* Found an idle API server, insert it into the list and spawn a new
         * idle API server. */
        std::string address = worker->GetAddress();
        std::cerr << "[" << __func__ << "] Assign pooled " << address
                  << std::endl;
        entry->AddBusyWorker(worker, gm[i]);
        assigned_workers.push_back(address);

        // TODO: may need to spawn asynchronously.
        std::vector<int> count        = {1};
        std::vector<std::string> uuid = {entry->GetUuid()};
        std::vector<std::string> new_worker_address =
            entry->GetDaemon()->client_->SpawnWorker(count, uuid,
                                                     entry->GetDaemon()->ip_);
        entry->AddIdleWorker(new_worker_address[0]);
      } else {
        /* No idle API server was found, spawn a new API server. */
        std::vector<int> count        = {1};
        std::vector<std::string> uuid = {entry->GetUuid()};
        std::vector<std::string> new_worker_address =
            entry->GetDaemon()->client_->SpawnWorker(count, uuid,
                                                     entry->GetDaemon()->ip_);
        if (new_worker_address.empty()) {
          std::cerr << "[" << __func__
                    << "] Unexpected: failed to spawn new API server on GPU ("
                    << entry->GetUuid() << ") at " << entry->GetDaemon()->ip_
                    << std::endl;
          new_worker_address.push_back("0.0.0.0:0");
        }

        entry->AddBusyWorker(new_worker_address[0], gm[i]);
        assigned_workers.push_back(new_worker_address[0]);
        std::cerr << "[" << __func__ << "] Assign " << assigned_workers.back()
                  << std::endl;
      }
    }

    /* Restore the order of the assigned worker. This is an O(N^2) method; can
     * replace with any O(N) algorithm. */
    std::vector<std::string> returned_workers(gpu_count);
    for (unsigned i = 0; i < gpu_count; ++i) {
      for (unsigned j = 0; j < gpu_count; ++j)
        if (gm[i] == gpu_mem[j] && returned_workers[j].empty()) {
          returned_workers[j] = assigned_workers[i];
          break;
        }
    }
    return returned_workers;
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
