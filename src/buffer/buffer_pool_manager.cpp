//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lock(this->latch_);
  frame_id_t *fid;

  // You should pick the replacement frame from either the free list or the replacer
  if (this->free_list_.empty()) {
    // nullptr if all frames are currently in use and not evictable (in another word, pinned)
    if (!this->replacer_->Evict(fid)) {
      return nullptr;
    }

    auto p_to_evict = this->pages_ + *fid;

    if (p_to_evict->IsDirty()) {
      // If the replacement frame has a dirty page, you should write it back to the disk first. 
      auto promise = this->disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      this->disk_scheduler_->Schedule({
        /*is_write=*/true, 
        p_to_evict->GetData(),
        /*page_id=*/p_to_evict->GetPageId(),
        std::move(promise)
      });
      assert(future.get());
    }

    // You also need to reset the memory and metadata for the new page.
    p_to_evict->ResetMemory();
    p_to_evict->is_dirty_ = false;
    page_table_.erase(p_to_evict->GetPageId());

  } else {

    *fid = free_list_.front();
    free_list_.pop_front();
  }

  // and then call the AllocatePage() method to get a new page id.
  // Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
  // Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
  auto pid = this->AllocatePage();
  auto p = this->pages_ + *fid;
  p->page_id_ = pid;
  this->replacer_->RecordAccess(*fid);
  this->replacer_->SetEvictable(*fid, false);
  this->page_table_[pid] = *fid;
  ++p->pin_count_;

  return p;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(this->latch_);
  auto it = page_table_.find(page_id);

  // 1.     Yes, the page is already loaded in the buffer pool
  if (it != page_table_.end()) {
    auto frame_id = it->second;
    auto p = this->pages_ + frame_id;
    this->replacer_->SetEvictable(frame_id, false);  // pin the page: set to not evictable
    ++p->pin_count_;

    return p;
  }

  // 2.     If not found, pick a replacement frame from either the free list or the replacer 
  //        (always find from the free list first)().
  frame_id_t *fid;

  if (this->free_list_.empty()) {
    // 2.     Return nullptr if page_id needs to be fetched from the disk 
    //        but all frames are currently in use and not evictable (in another word, pinned).
    if (!this->replacer_->Evict(fid)) {
      return nullptr;
    }

    auto p_to_evict = this->pages_ + *fid;

    if (p_to_evict->IsDirty()) {
      // Similar to NewPage(), if the old page is dirty, you need to write it 
      // back to disk and update the metadata of the new page In addition
      auto promise = this->disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      this->disk_scheduler_->Schedule({
        /*is_write=*/true, 
        p_to_evict->GetData(),
        /*page_id=*/p_to_evict->GetPageId(),
        std::move(promise)
      });
      assert(future.get());
    }

    // You also need to reset the memory and metadata for the new page.
    p_to_evict->ResetMemory();
    p_to_evict->is_dirty_ = false;
    page_table_.erase(p_to_evict->GetPageId());

  } else {
    *fid = this->free_list_.front();
    this->free_list_.pop_front();
  }
  
  // remember to disable eviction and record the access history 
  // of the frame like you did for NewPage().
  auto p = this->pages_ + *fid;

  auto promise = this->disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // read the page from disk by scheduling a read DiskRequest 
  // with disk_scheduler_->Schedule(), and replace the old page in 
  // the frame.
  this->disk_scheduler_->Schedule({
    /*is_write=*/false, 
    p->GetData(),  // Here very different than what I thought: how can Get method be used to modify an instance?
    /*page_id=*/page_id,
    std::move(promise)
  });
  assert(future.get());

  this->replacer_->RecordAccess(*fid);
  this->replacer_->SetEvictable(*fid, false);
  this->page_table_[page_id] = *fid;  
  ++p->pin_count_;

  return p;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // Add implementation Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already 0, return false. Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer. Also, set the dirty flag on the page to indicate if the page was modified.
  std::unique_lock<std::mutex> lock(this->latch_);
  auto it = this->page_table_.find(page_id);

  if (it == page_table_.end()) {
    return false;
  }

  auto p = this->pages_ + it->second;
  if (p->GetPinCount() <= 0) {
    return false;
  }

  if (p->GetPinCount() == 1) {
    this->replacer_->SetEvictable(it->second, true);
    p->is_dirty_ = true;
  }

  --p->pin_count_;

  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  std::unique_lock<std::mutex> lock(this->latch_);
  auto it = this->page_table_.find(page_id);

  if (it == page_table_.end()) {
    return false;
  }

  auto p = this->pages_ + it->second;
  auto promise = this->disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  this->disk_scheduler_->Schedule({
    /*is_write=*/true, 
    p->GetData(),
    /*page_id=*/p->GetPageId(),
    std::move(promise)
  });
  assert(future.get());

  // Unset the dirty flag of the page after flushing.
  p->is_dirty_ = false;
  return false; 
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lock(this->latch_);

  for (auto it = this->page_table_.begin(); it != this->page_table_.end(); ++it) {
    auto p = this->pages_ + it->second;
    auto promise = this->disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    this->disk_scheduler_->Schedule({
      /*is_write=*/true, 
      p->GetData(),
      /*page_id=*/p->GetPageId(),
      std::move(promise)
    });
    assert(future.get());

    // Unset the dirty flag of the page after flushing.
    p->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::unique_lock<std::mutex> lock(this->latch_);
  auto it = this->page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  auto fid = it->second;
  auto p = this->pages_ + fid;
  if (p->GetPinCount() > 0) {
    return false;
  }
  this->replacer_->Remove(fid);
  this->free_list_.push_back(fid);
  this->page_table_.erase(it);

  // reset metadata of the page
  p->ResetMemory();
  p->is_dirty_ = false;
  p->page_id_ = INVALID_PAGE_ID;
  p->pin_count_ = 0;

  this->DeallocatePage(page_id);

  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
