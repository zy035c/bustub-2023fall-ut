//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"
#include <algorithm>

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  this->max_depth_ = std::min(
    max_depth,
    static_cast<uint32_t>(HTABLE_HEADER_MAX_DEPTH)
  );
  std::fill(
    std::begin(this->directory_page_ids_), 
    std::end(this->directory_page_ids_), 
    INVALID_PAGE_ID
  );
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (this->max_depth_ == 0) {
    return 0;
  }
  return static_cast<uint32_t>(hash >> (32 - this->max_depth_));
}
   
auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t { 
  return this->directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  this->directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  // Why not use size of directory_page_ids_? cause actual size can be smaller than it (but not larger)
  // this is why it's `extendible`
  return 1 << this->max_depth_;
}

}  // namespace bustub
