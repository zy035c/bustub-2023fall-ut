//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
  this->left_executor_->Init();
  this->right_executor_->Init();

  RID left_rid_;  // not use
  bool ok = this->left_executor_->Next(&left_tuple_, &left_rid_);
  if (!ok) {
    this->left_is_end_ = true;
  }
}
/**
 * Pseudo code:
 *  def Next(self) -> bool:

        if self.left_end:
            return False

        ok, rt = self.right_exec.Next()
        if not ok:

            last_lt = self.lt
            last_is_left_matched = self.is_left_matched

            ok, self.lt = self.left_exec.Next()
            if not ok:
                self.left_end = True
                return False
            self.is_left_matched = False
            self.right_exec.Init()  # restart right exec

            if self.type == "left join" and not last_is_left_matched:
                join = self.MakeJoin(last_lt, self.right_exec.MakeNull())
                return True

            return self.Next()

        ok, join = self.EvaluateJoin(self.lt, rt)
        if not ok:
            return self.Next()

        self.self.is_left_matched = True
        return True
        pass
*/
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID right_rid;

  if (this->left_is_end_) return false;

  bool ok = this->right_executor_->Next(&right_tuple, &right_rid);

  if (!ok) {
    Tuple *old_left_tuple = &this->left_tuple_;
    bool old_left_is_match = this->left_is_match_;

    RID left_rid_;  // not use
    bool ok_ = this->left_executor_->Next(
      &this->left_tuple_, 
      &left_rid_
    );
    if (!ok_) {
      this->left_is_end_ = true;
      return false;
    }

    this->right_executor_->Init();  // restart right executor
    this->left_is_match_ = false;

    if (this->plan_->GetJoinType() == JoinType::LEFT && !old_left_is_match) {
      // should yield a tuple with null right
      *tuple = this->MakeNullRightTuple(old_left_tuple);
      *rid = tuple->GetRid();
      return true;
    }

    return this->Next(tuple, rid);
  }

  auto ret = plan_->Predicate()->EvaluateJoin(
    &this->left_tuple_,
    this->left_executor_->GetOutputSchema(),
    &right_tuple,
    this->right_executor_->GetOutputSchema()
  );

  if (ret.IsNull() || !ret.GetAs<bool>()) {
    return this->Next(tuple, rid);
  }

  this->left_is_match_ = true;
  *tuple = MakeJoinTuple(&this->left_tuple_, &right_tuple);
  *rid = tuple->GetRid();
  return true;
}

auto NestedLoopJoinExecutor::MakeNullRightTuple(Tuple *left_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), idx));
  }

  for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(
      ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(idx).GetType())
    );
  }

  return {values, &GetOutputSchema()};
}

auto inline NestedLoopJoinExecutor::MakeJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(
      left_tuple->GetValue(&left_executor_->GetOutputSchema(), idx)
    );
  }

  for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(
      right_tuple->GetValue(&left_executor_->GetOutputSchema(), idx)
    );
  }

  return {values, &GetOutputSchema()};
}

}  // namespace bustub
