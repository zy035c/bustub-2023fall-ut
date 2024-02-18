#pragma once

#include <unordered_map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"
#include <map>

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }

  timestamp_t commit_ts_;

  timestamp_t watermark_;

  std::map<timestamp_t, int> current_reads_; // read_ts -> count
  /* use a map to track timestamp. keep order of the timestamp_t alias int64_t */
  
  /* mutex */
  std::mutex mtx_;
};

};  // namespace bustub
