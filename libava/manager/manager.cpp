#include <errno.h>
#include <fcntl.h>
#include <glib.h>
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

#include <atomic>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <future>
#include <queue>
#include <string>
#include <thread>
#include <queue>
#include <mutex>
#include <nvml.h>

#include "common/cmd_channel_impl.h"
#include "common/cmd_handler.h"
#include "common/socket.h"

int listen_fd;
std::atomic<int> worker_id(1);
GHashTable *worker_info;

__sighandler_t original_sigint_handler = SIG_DFL;

void sigint_handler(int signo) {
  if (listen_fd > 0) close(listen_fd);
  signal(signo, original_sigint_handler);
  raise(signo);
}

class Workers {
 public:
  void enqueue(int port) {
    this->mtx.lock();
    this->worker_queue.push(port);
    this->mtx.unlock();
  }

  int dequeue() {
    this->mtx.lock();
    int ret = 0;
    if (this->worker_queue.size() > 0) {
      ret = this->worker_queue.front();
      this->worker_queue.pop();
    }
    this->mtx.unlock();
    return ret;
  }

  unsigned int size() {
    unsigned int size;
    this->mtx.lock();
    size = this->worker_queue.size();
    this->mtx.unlock();
    return size;
  }

 private:
  std::queue<int> worker_queue;
  std::mutex mtx;
};

// TODO: group into a class
int num_assigned_app = 0;
int total_gpu_num = 1;
unsigned worker_pool_size = 1;

inline int get_worker_port(int id) { return 4000 + id; }

int get_gpu_id_from_uuid(
    const std::vector<std::string> &visible_cuda_device_uuid,
    const char *gpu_uuid) {
  int gpu_id;
  auto itr = std::find(visible_cuda_device_uuid.begin(),
                       visible_cuda_device_uuid.end(), std::string(gpu_uuid));

  /* gpu_uuid is a correct UUID */
  if (itr != visible_cuda_device_uuid.end()) {
    gpu_id = itr - visible_cuda_device_uuid.begin();
    fprintf(stderr, "[manager] GPU UUID matches index %d\n", gpu_id);
  }
  /* gpu_uuid is a GPU index */
  else if (strlen(gpu_uuid) > 0 && strlen(gpu_uuid) < 2) {
    gpu_id = atoi(gpu_uuid);
  }
  /* Otherwise use round-robin */
  else {
    gpu_id = (num_assigned_app++) % total_gpu_num;
  }

  return gpu_id;
}

void reply_to_guestlib(int client_fd, int assigned_worker_port) {
  struct command_base response;
  uintptr_t *worker_port;

  response.api_id = INTERNAL_API;
  worker_port = (uintptr_t *)response.reserved_area;
  *worker_port = assigned_worker_port;
  send_socket(client_fd, &response, sizeof(struct command_base));
}

void spawn_worker() {
  // TODO: send spawn request to daemon
}

void handle_guestlib(int client_fd,
                     const std::vector<std::string> &visible_cuda_device,
                     const std::vector<std::string> &visible_cuda_device_uuid,
                     Workers *idle_workers) {
  pid_t child;

  int worker_port;
  int assigned_worker_port;
  int gpu_id = 0;
  char *gpu_uuid = "";

  struct command_base msg;

  visible_cuda_device.size();

  /* get guestlib info */
  recv_socket(client_fd, &msg, sizeof(struct command_base));
  switch (msg.command_type) {
    case NW_NEW_APPLICATION:
      /* Get GPU UUID */
      gpu_uuid = (char *)msg.reserved_area;
      printf("[manager] Receive request for GPU UUID = %s\n",
             strlen(gpu_uuid) ? gpu_uuid : "[NULL]");

      /* Lookup GPU index */
      gpu_id = get_gpu_id_from_uuid(visible_cuda_device_uuid, gpu_uuid);

      /* Assign a worker to the guestlib and get its port */
      assigned_worker_port = idle_workers[gpu_id].dequeue();
      if (assigned_worker_port != 0) {
        /* Respond guestlib and spawn a new idle worker */
        reply_to_guestlib(client_fd, assigned_worker_port);
        close(client_fd);

        if (idle_workers[gpu_id].size() > worker_pool_size - 1) break;

        worker_port = get_worker_port(worker_id++);
        child = fork();
        if (child == 0) {
          close(listen_fd);
          spawn_worker();
        } else {
          // TODO: acknowledge worker's initialization
          idle_workers[gpu_id].enqueue(worker_port);
        }
      } else {
        /* Spawn a new idle worker and let guestlib retry */
        worker_port = get_worker_port(worker_id++);
        child = fork();
        if (child == 0) {
          close(listen_fd);
          close(client_fd);
          spawn_worker();
        } else {
          // TODO: acknowledge worker's initialization
          reply_to_guestlib(client_fd, worker_port);
          close(client_fd);
        }
      }
      break;

    default:
      printf("[manager] Wrong message type\n");
      close(client_fd);
  }
}

class ManagerConfig {
public:
  static int const DefaultManagerPort;
  static int const DefaultWorkerPoolSize;

  ManagerConfig(int mp = DefaultManagerPort, int wps  = DefaultWorkerPoolSize) :
    manager_port(mp), worker_pool_size(wps) {}

  void print() {
    std::cerr << "* Manager port: " << manager_port << std::endl
      << "* API server pool size: " << worker_pool_size << std::endl;
  }

  int manager_port;
  int worker_pool_size;

  std::vector<std::string> visible_cuda_device;
  std::vector<std::string> visible_cuda_device_uuid;
};

int const ManagerConfig::DefaultManagerPort = 3333;
int const ManagerConfig::DefaultWorkerPoolSize = 3;

ManagerConfig *parse_arguments(int argc, char *argv[]) {
  int c;
  opterr = 0;
  int manager_port = ManagerConfig::DefaultManagerPort;
  int worker_pool_size = ManagerConfig::DefaultWorkerPoolSize;

  while ((c = getopt(argc, argv, "m:n:")) != -1) {
    switch (c) {
    case 'm':
      manager_port = (uint32_t)atoi(optarg);
      break;
    case 'n':
      worker_pool_size = (uint32_t)atoi(optarg);
      break;
    default:
      fprintf(stderr, "Usage: %s "
                      "[-m manager_port {%d}] "
                      "[-n worker_pool_size {%d}]\n",
              argv[0], ManagerConfig::DefaultManagerPort, ManagerConfig::DefaultWorkerPoolSize);
      exit(EXIT_FAILURE);
    }
  }

  auto config = new ManagerConfig(manager_port, worker_pool_size);
  return config;
}

int main(int argc, char *argv[]) {
  auto config = parse_arguments(argc, argv);
  config->print();

  // BELOW HAS NOT BEEN UPDATED

  std::vector<std::string> visible_cuda_device;
  std::vector<std::string> visible_cuda_device_uuid;

  /* setup signal handler */
  if ((original_sigint_handler = signal(SIGINT, sigint_handler)) == SIG_ERR)
    printf("failed to catch SIGINT\n");

  /* setup worker info hash table */
  worker_info =
      g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, free);

  /* initialize TCP socket */
  struct sockaddr_in address;
  int addrlen = sizeof(address);
  int opt = 1;
  int client_fd;
  pid_t child;
  int worker_port;

  /* GPU information */
  total_gpu_num = visible_cuda_device.size();
  fprintf(stderr, "* total GPU: %d\n", total_gpu_num);

  /* Worker information, each GPU has a pool of pre-spawned workers */
  Workers *idle_workers = new Workers[total_gpu_num];

  /* Spawn worker pool for each GPU. */
  for (int i = 0; i < total_gpu_num; i++) {
    for (unsigned j = 0; j < worker_pool_size; j++) {
      worker_port = get_worker_port(worker_id);
      idle_workers[i].enqueue(worker_port);

      child = fork();
      if (child == 0) {
        spawn_worker();
      }
      worker_id++;
    }
  }

  /* Start manager TCP server */
  if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket");
  }
  // Forcefully attaching socket to the manager port
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(config->manager_port);

  if (bind(listen_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed");
  }
  if (listen(listen_fd, 10) < 0) {
    perror("listen");
  }

  /* polling new applications */
  do {
    client_fd =
        accept(listen_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);
    std::async(std::launch::async | std::launch::deferred, handle_guestlib,
               client_fd, visible_cuda_device, visible_cuda_device_uuid,
               &idle_workers[0]);

  } while (1);

  return 0;
}
