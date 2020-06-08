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

int worker_pool_enabled;

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

int num_assigned_app = 0;
int total_gpu_num = 1;
int worker_port_base = 4000;

inline int get_worker_port(int id) { return worker_port_base + id; }

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

void spawn_worker(int gpu_id,
                  const std::vector<std::string> &visible_cuda_device,
                  const std::vector<std::string> &visible_cuda_device_uuid,
                  int worker_port_min, int worker_port_max) {
  char str_port[20], str_port_max[20];
  sprintf(str_port, "%d", worker_port_min);
  sprintf(str_port_max, "%d", worker_port_max);

  char *gpu_uuid = (char *)visible_cuda_device_uuid[gpu_id].c_str();
  fprintf(stderr, "[manager] Spawn new worker port=%s-%s on GPU-%d UUID=%s\n",
          str_port, str_port_max, gpu_id, gpu_uuid);

  char *const argv_list[] = {(char *)"worker", str_port, NULL};
  char *const envp_list[] = {(char *)visible_cuda_device[gpu_id].c_str(),
                             (char *)"AVA_CHANNEL=TCP", NULL};
  if (execvpe("../generated/worker", argv_list, envp_list) < 0) {
    perror("execv worker");
  }
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
      if (worker_pool_enabled && assigned_worker_port != 0) {
        /* Respond guestlib and spawn a new idle worker */
        reply_to_guestlib(client_fd, assigned_worker_port);
        close(client_fd);

        if (idle_workers[gpu_id].size() > WORKER_POOL_SIZE - 1) break;

        worker_port = get_worker_port(worker_id++);
        child = fork();
        if (child == 0) {
          close(listen_fd);
          spawn_worker(gpu_id, visible_cuda_device, visible_cuda_device_uuid,
                       worker_port, worker_port);
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
          spawn_worker(gpu_id, visible_cuda_device, visible_cuda_device_uuid,
                       worker_port, worker_port);
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

int main(int argc, char *argv[]) {
  int c;
  opterr = 0;
  char *config_file_name = NULL;
  while ((c = getopt(argc, argv, "f:p:")) != -1) {
    switch (c) {
      case 'f':
        config_file_name = optarg;
        break;
      case 'p':
        worker_port_base = (uint32_t)atoi(optarg);
        break;
      default:
        fprintf(stderr, "Usage: %s [-f config_file_name]\n", argv[0]);
        exit(EXIT_FAILURE);
    }
  }
  if (config_file_name == NULL) {
    fprintf(stderr, "-f is mandatory. Please specify config file name\n");
    exit(EXIT_FAILURE);
  }
  std::ifstream config_file(config_file_name);
  std::vector<std::string> visible_cuda_device;
  std::vector<std::string> visible_cuda_device_uuid;
  std::string line;
  nvmlReturn_t ret = nvmlInit();
  if (ret != NVML_SUCCESS) {
    fprintf(stderr, "fail to get device by uuid: %s\n", nvmlErrorString(ret));
    exit(-1);
  }
  while (std::getline(config_file, line)) {
    nvmlDevice_t dev;
    nvmlMemory_t mem = {};
    char *line_cstr = (char *)line.c_str();
    char *pchr = strchr(line_cstr, '=');

    fprintf(stderr, "* GPU-%lu UUID is %s\n", visible_cuda_device.size(),
            pchr + 1);
    ret = nvmlDeviceGetHandleByUUID(pchr + 1, &dev);
    if (ret != NVML_SUCCESS) {
      fprintf(stderr, "fail to get device by uuid: %s\n", nvmlErrorString(ret));
      exit(-1);
    }
    ret = nvmlDeviceGetMemoryInfo(dev, &mem);
    if (ret != NVML_SUCCESS) {
      fprintf(stderr, "fail to get device by uuid: %s\n", nvmlErrorString(ret));
      exit(-1);
    }
    visible_cuda_device.push_back(line);
    visible_cuda_device_uuid.push_back(pchr + 1);
  }

  /* parse environment variables */
  worker_pool_enabled = 1;
  printf("* worker pool: %s\n", "always");

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
  if (worker_pool_enabled) {
    for (int i = 0; i < total_gpu_num; i++) {
      for (int j = 0; j < WORKER_POOL_SIZE; j++) {
        worker_port = get_worker_port(worker_id);
        idle_workers[i].enqueue(worker_port);

        child = fork();
        if (child == 0) {
          spawn_worker(i, visible_cuda_device, visible_cuda_device_uuid,
                       worker_port, worker_port);
        }
        worker_id++;
      }
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
  address.sin_port = htons(WORKER_MANAGER_PORT);

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
