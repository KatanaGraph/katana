#ifndef LIBAVA_MANAGER_MANAGER_H_
#define LIBAVA_MANAGER_MANAGER_H_

#include <stdint.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <utility>

class GpuInfo {
public:
  std::string uuid_;
  uint64_t free_memory_;
};

class Workers {
public:
  void Enqueue(std::string worker_address, std::string uuid = "") {
    this->mtx_.lock();
    this->worker_queue_.push({worker_address, uuid});
    this->mtx_.unlock();
  }

  std::pair<std::string, std::string> Dequeue() {
    this->mtx_.lock();
    std::pair<std::string, std::string> ret;
    if (this->worker_queue_.size() > 0) {
      ret = this->worker_queue_.front();
      this->worker_queue_.pop();
    }
    this->mtx_.unlock();
    return ret;
  }

  unsigned int Size() {
    unsigned int size;
    this->mtx_.lock();
    size = this->worker_queue_.size();
    this->mtx_.unlock();
    return size;
  }

private:
  std::queue<std::pair<std::string, std::string>> worker_queue_;
  std::mutex mtx_;
};

class DaemonServiceClient;

class DaemonInfo {
public:
  void PrintGpuInfo() {
    for (auto const& gi : gpu_info_)
      std::cerr << "- " << gi.uuid_ << " (" << (gi.free_memory_ >> 20)
                << " MB)\n";
  }

  std::unique_ptr<DaemonServiceClient> client_;
  std::string ip_;
  std::vector<GpuInfo> gpu_info_;
  Workers workers_;
};

#endif // LIBAVA_MANAGER_MANAGER_H_
