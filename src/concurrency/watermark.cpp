#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  std::unique_lock<std::mutex> lck(mtx_);
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // Watermark is the lowest read timestamp among all in-progress transactions.
  
  ++this->current_reads_[read_ts];
  /* get the smallest element from the pg and make it watermark */
  this->watermark_ = this->current_reads_.begin()->first;

}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  std::unique_lock<std::mutex> lck(mtx_);

  auto it = this->current_reads_.find(read_ts);


  if (it == this->current_reads_.end()) {
    throw Exception("read ts not found");
  }

  if (it->second == 0) {
    this->current_reads_.erase(read_ts);
    this->watermark_ = this->current_reads_.begin()->first;
    throw Exception("read ts count is already 0");
  }

  if (--it->second == 0) {
    this->current_reads_.erase(read_ts);
    this->watermark_ = this->current_reads_.begin()->first;
  }
}

}  // namespace bustub
