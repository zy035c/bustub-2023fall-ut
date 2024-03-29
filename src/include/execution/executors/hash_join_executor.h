//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  bool left_is_end_{false};
  bool left_is_match_{false};

  Tuple left_tuple_;
  std::vector<std::unordered_map<HashJoinKey, std::vector<Tuple>>> hash_table_;

  auto inline HashJoinExecutor::MakeNullRightTuple(Tuple *left_tuple) -> Tuple;
  auto inline HashJoinExecutor::MakeJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple;
};

/** HashJoinKey represents a key in an hash join operation */
struct HashJoinKey {
  Value key_;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent key_ expressions, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on Value */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    return bustub::HashUtil::HashValue(&hash_key.key_);
  }
};

}  // namespace std
