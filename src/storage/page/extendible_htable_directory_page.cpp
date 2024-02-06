//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  this->max_depth_ = std::min(
    max_depth,
    static_cast<uint32_t>(HTABLE_DIRECTORY_MAX_DEPTH)
  );
  this->global_depth_ = 0;
  std::fill(std::begin(bucket_page_ids_), std::end(bucket_page_ids_), INVALID_PAGE_ID);
  std::fill(std::begin(local_depths_), std::end(local_depths_), 0);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (this->max_depth_ == 0) {
    return 0;
  }
  uint32_t ones = 1;
  for (int i = 1; i < this->max_depth_; ++i) {
    ones << 1;
    ones += 1;
  }
  // one line: hash & ((1 << global_depth_) - 1)
  return hash & ones;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t { 
  return bucket_page_ids_[bucket_idx]; 
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { 
  // flip the bit before the first `local_depth` bit
  return (1 << (this->local_depths_[bucket_idx])) ^ bucket_idx;  // -1?
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { 
  return this->global_depth_;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  BUSTUB_ENSURE(global_depth_ != max_depth_, "global_depth_ has reach to max_depth_ and can not increase")
  // rehash
  for (uint32_t i = 0; i < this->Size(); i++) {
    if (local_depths_[i] == INVALID_PAGE_ID) continue;
    // should not be [i + (1U << global_depth_)] ?
    auto marker = local_depths_[i] & (1U << global_depth_);
    local_depths_[i + marker] = local_depths_[i];
    bucket_page_ids_[i + marker] = bucket_page_ids_[i];
  }
  ++this->global_depth_;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  --this->global_depth_;

  for (uint32_t i = 0; i < this->Size(); i++) {
    if (local_depths_[i] == INVALID_PAGE_ID) continue;
    // clear out-of-bound hash
    local_depths_[i + (1U << global_depth_)] = 0;
    bucket_page_ids_[i + (1U << global_depth_)] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  // NOT IMPLEMENTED
  return false;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { 
  return 1 << this->global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t { 
  return this->local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  this->local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  ++this->local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  --this->local_depths_[bucket_idx];
}

}  // namespace bustub
