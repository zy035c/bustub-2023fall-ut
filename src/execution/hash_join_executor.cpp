//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  this->left_executor_->Init();
  this->right_executor_->Init();
  /**
   * I skipped this one !!!!
  */
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  /**
   * I skipped this one !!!!
  */
 return false;
}

auto inline HashJoinExecutor::MakeNullRightTuple(Tuple *left_tuple) -> Tuple {
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

auto inline HashJoinExecutor::MakeJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
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
