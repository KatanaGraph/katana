#ifndef __LIBAVA_MANAGER_MANAGER_H__
#define __LIBAVA_MANAGER_MANAGER_H__

#include <stdint.h>

#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <utility>

class GpuInfo {
public:
  std::string uuid;
  uint64_t free_memory;
};

class Workers {
 public:
  void enqueue(int port, std::string uuid = "") {
    this->mtx.lock();
    this->worker_queue.push({port, uuid});
    this->mtx.unlock();
  }

  std::pair<int, std::string> dequeue() {
    this->mtx.lock();
    std::pair<int, std::string> ret;
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
  std::queue<std::pair<int, std::string> > worker_queue;
  std::mutex mtx;
};

class DaemonServiceClient;

class DaemonInfo {
public:
  void print_gpu_info() {
    for (auto gi : gpu_info)
      std::cerr << "- " << gi.uuid << " (" << (gi.free_memory >> 20) << " MB)\n";
  }

  DaemonServiceClient* client;
  std::string ip;
  std::vector<GpuInfo> gpu_info;
  Workers workers;
};

#endif
