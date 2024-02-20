//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), 
    aht_(this->plan_->GetAggregates(), this->plan_->agg_types_), aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // without Clear here leads to problems in repeated subquery
  // e.g. select * from __mock_table_123, (select count(*) as cnt from t1);
  aht_.Clear();
  this->child_executor_->Init();

  while (1) {
    Tuple tuple;
    RID rid;

    bool ok = child_executor_->Next(&tuple, &rid);
    if (!ok) break;

    // Make aggregate key and value for the tuple
    auto agg_key = this->MakeAggregateKey(&tuple);
    auto agg_val = this->MakeAggregateValue(&tuple);

    // Insert and combine the values in the hash table
    this->aht_.InsertCombine(agg_key, agg_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (this->is_end_) return false;
  }

  // Corner Case: table is empty
  // Hint: When performing aggregation on an empty table, CountStarAggregate should return zero and all other aggregate
  // types should return integer_null. This is why GenerateInitialAggregateValue initializes most aggregate values as
  // NULL.
  if (aht_.Begin() == aht_.End()) {
    std::vector<Value> values;
    if (!this->plan_->GetGroupBys().empty()) return false;

    for (const auto &expr : plan_->GetGroupBys()) {
        values.emplace_back(ValueFactory::GetNullValueByType(
            expr->GetReturnType().GetType()
        ));
    }

    for (const auto &agg_type : plan_->agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // For COUNT, return 0 if there are no tuples
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        default:
          // For other aggregates, return NULL if there are no tuples
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
   
    *tuple = Tuple(values, &GetOutputSchema());
    *rid = RID();
    is_end_ = true;
    return true;
  }

  // Get the current aggregated value
  const auto &agg_val = aht_iterator_.Val();

  // Create a tuple from the aggregated value
  std::vector<Value> values;
  for (const auto &val : agg_val.aggregates_) {
    values.push_back(val);
  }

  // Include group-by values if present
  if (!plan_->GetGroupBys().empty()) {
    const auto &agg_key = aht_iterator_.Key();
    values.insert(values.begin(), agg_key.group_bys_.begin(), agg_key.group_bys_.end());
  }
  *tuple = Tuple(values, &GetOutputSchema());
  *rid = RID();

  // Move to the next group
  ++aht_iterator_;
  this->is_end_ = true;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
