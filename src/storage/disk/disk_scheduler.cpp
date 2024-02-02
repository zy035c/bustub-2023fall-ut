//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
  //     "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  this->request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    if (this->disk_manager_ == nullptr) {
      return;
    }

    auto req_opt = this->request_queue_.Get();
    if (req_opt == std::nullopt) {
      return;
    }
    auto req = std::move(req_opt.value());
    if (req.is_write_) {
      this->disk_manager_->WritePage(
        req.page_id_,
        req.data_
      );
    } else {
      this->disk_manager_->ReadPage(
        req.page_id_,
        req.data_
      );
    }

    // Remember to set the value on the DiskRequest's callback to
    // signal to the request issuer that the request has been completed.
    req.callback_.set_value(true);
  }
}

}  // namespace bustub
