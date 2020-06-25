#ifndef LIBAVA_MANAGER_MANAGER_H_
#define LIBAVA_MANAGER_MANAGER_H_

#include <stdint.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <utility>

class DaemonInfo;
class GpuList;

class GpuInfo {
public:
  GpuInfo() : free_memory_(0) {}

  GpuInfo(std::string uuid, uint64_t free_memory)
      : uuid_(uuid), free_memory_(free_memory) {}

  std::string uuid_;
  uint64_t free_memory_;
};

class WorkerInfo {
public:
  WorkerInfo(std::string address, uint64_t used_memory = 0)
      : address_(address), used_memory_(used_memory) {}

  std::string GetAddress() { return address_; }

  std::string address_;
  uint64_t used_memory_;
};

class WorkerQueue {
private:
  std::queue<std::shared_ptr<WorkerInfo>> worker_queue_;
  std::mutex mtx_;

public:
  void Enqueue(std::string worker_address, uint64_t mem_size = 0) {
    mtx_.lock();
    auto worker_info = std::make_shared<WorkerInfo>(worker_address, mem_size);
    worker_queue_.push(worker_info);
    mtx_.unlock();
  }

  void Enqueue(std::shared_ptr<WorkerInfo> worker_info) {
    mtx_.lock();
    worker_queue_.push(worker_info);
    mtx_.unlock();
  }

  std::shared_ptr<WorkerInfo> Dequeue() {
    std::shared_ptr<WorkerInfo> ret;
    mtx_.lock();
    if (worker_queue_.size() > 0) {
      ret = worker_queue_.front();
      worker_queue_.pop();
    }
    mtx_.unlock();
    return ret;
  }

  size_t Size() {
    size_t size;
    mtx_.lock();
    size = worker_queue_.size();
    mtx_.unlock();
    return size;
  }
};

class WorkerSet {
private:
  std::set<std::shared_ptr<WorkerInfo>> worker_set_;
  std::mutex mtx_;

public:
  void Insert(std::string worker_address, uint64_t mem_size = 0) {
    mtx_.lock();
    auto worker_info = std::make_shared<WorkerInfo>(worker_address, mem_size);
    worker_set_.insert(worker_info);
    mtx_.unlock();
  }

  void Insert(std::shared_ptr<WorkerInfo> worker_info) {
    mtx_.lock();
    worker_set_.insert(worker_info);
    mtx_.unlock();
  }

  uint64_t Remove(std::string address) {
    uint64_t ret = 0;
    mtx_.lock();
    for (auto wi = worker_set_.begin(); wi != worker_set_.end(); ++wi)
      if ((*wi)->GetAddress() == address) {
        ret = (*wi)->used_memory_;
        worker_set_.erase(wi);
        break;
      }
    mtx_.unlock();
    return ret;
  }

  size_t Size() {
    size_t size;
    mtx_.lock();
    size = worker_set_.size();
    mtx_.unlock();
    return size;
  }
};

class GpuListEntry {
private:
  GpuInfo gpu_info_; // TODO: need lock to protect
  WorkerQueue idle_workers_;
  WorkerSet busy_workers_;

  /* For indexing. */
  DaemonInfo* daemon_;
  GpuList* gpu_list_;

  friend class GpuList;

public:
  GpuListEntry(DaemonInfo* daemon, GpuList* gpu_list)
      : daemon_(daemon), gpu_list_(gpu_list) {}

  void ReserveMemory(uint64_t size) { gpu_info_.free_memory_ -= size; }

  void ReleaseMemory(uint64_t size) { gpu_info_.free_memory_ += size; }

  void AddIdleWorker(std::string address) { idle_workers_.Enqueue(address); }

  std::shared_ptr<WorkerInfo> PopIdleWorker() {
    return idle_workers_.Dequeue();
  }

  void AddBusyWorker(std::shared_ptr<WorkerInfo> worker_info,
                     uint64_t used_memory = 0) {
    if (used_memory)
      worker_info->used_memory_ = used_memory;
    busy_workers_.Insert(worker_info);
  }

  void AddBusyWorker(std::string address, uint64_t used_memory) {
    busy_workers_.Insert(address, used_memory);
  }

  void RemoveBusyWorker(std::string address) {
    uint64_t used_memory = busy_workers_.Remove(address);
    ReleaseMemory(used_memory);
  }

  DaemonInfo* GetDaemon() { return daemon_; }

  void SetDaemon(DaemonInfo* daemon) { this->daemon_ = daemon; }

  void SetGpuList(GpuList* gpu_list) { gpu_list_ = gpu_list; }

  GpuList* GetGpuList() { return gpu_list_; }

  std::string GetUuid() { return gpu_info_.uuid_; }

  void SetUuid(std::string uuid) { gpu_info_.uuid_ = uuid; }

  uint64_t GetFreeMemory() { return gpu_info_.free_memory_; }

  void SetFreeMemory(uint64_t mem) { gpu_info_.free_memory_ = mem; }

  void SetGpuInfo(GpuInfo& info) { gpu_info_ = info; }

  void PrintGpuInfo() {
    std::cerr << "- " << gpu_info_.uuid_ << " ("
              << (gpu_info_.free_memory_ >> 20) << " MB)\n";
  }
};

class GpuList {
private:
  std::vector<std::shared_ptr<GpuListEntry>> gpu_list_;
  std::mutex mtx_;

  void UnlockedSort() {
    using gpu_list_entry_t = std::shared_ptr<GpuListEntry>;
    std::sort(gpu_list_.begin(), gpu_list_.end(),
              [](const gpu_list_entry_t& a, const gpu_list_entry_t& b) -> bool {
                if (a->busy_workers_.Size() == b->busy_workers_.Size())
                  return a->GetFreeMemory() > b->GetFreeMemory();
                return a->busy_workers_.Size() < b->busy_workers_.Size();
              });
  }

public:
  void AddEntries(std::vector<std::shared_ptr<GpuListEntry>> entries) {
    mtx_.lock();
    gpu_list_.reserve(gpu_list_.size() +
                      std::distance(entries.begin(), entries.end()));
    gpu_list_.insert(gpu_list_.end(), entries.begin(), entries.end());
    UnlockedSort();
    mtx_.unlock();
  }

  void AddEntry(std::shared_ptr<GpuListEntry> entry) { AddEntries({entry}); }

  std::shared_ptr<GpuListEntry> GetEntryAtIndex(unsigned idx) {
    if (idx > gpu_list_.size())
      return nullptr;
    return gpu_list_[idx];
  }

  std::shared_ptr<GpuListEntry> FindEntryAndReserveMemory(uint64_t request) {
    std::shared_ptr<GpuListEntry> p;
    mtx_.lock();

    for (unsigned i = 0; i < gpu_list_.size(); ++i) {
      if (gpu_list_[i]->GetFreeMemory() >= request)
        p = gpu_list_[i];
    }
    if (p) {
      p->ReserveMemory(request);
      UnlockedSort();
    }

    mtx_.unlock();
    return p;
  }

  void RevokeEntryWithMemory(std::shared_ptr<GpuListEntry> entry,
                             uint64_t request) {
    if (entry->gpu_list_ != this) {
      std::cerr << "Unmatched GPU list entry" << std::endl;
      return;
    }

    mtx_.lock();
    entry->ReleaseMemory(request);
    UnlockedSort();
    mtx_.unlock();
  }

  std::shared_ptr<GpuListEntry> FindEntryByUuid(std::string uuid) {
    std::shared_ptr<GpuListEntry> ret;
    mtx_.lock();
    for (const auto& entry : gpu_list_)
      if (entry->GetUuid() == uuid) {
        ret = entry;
        break;
      }
    mtx_.unlock();
    return ret;
  }

  void Sort() {
    mtx_.lock();
    UnlockedSort();
    mtx_.unlock();
  }

  void PrintGpuInfo() {
    mtx_.lock();
    for (const auto& entry : gpu_list_)
      entry->PrintGpuInfo();
    mtx_.unlock();
  }
};

class DaemonServiceClient;

class DaemonInfo {
public:
  void PrintGpuInfo() { gpu_list_.PrintGpuInfo(); }

  std::unique_ptr<DaemonServiceClient> client_;
  std::string ip_;
  GpuList gpu_list_;
};

#endif // LIBAVA_MANAGER_MANAGER_H_
