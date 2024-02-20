#include "optimizer/optimizer.h"
#include "seq_scan_plan.h"
#include "constant_value_expression.h"
#include "comparison_expression.h"
#include "column_value_expression.h"
#include "index_scan_plan.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::SeqScan) return optimized_plan;

  const auto &seq_scan_plan = dynamic_cast<SeqScanPlanNode &>(*optimized_plan);
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }
  
  // the expression has shape of
  // compare, Equal
  // index 0: ColumnValueExpression
  // index 1: ConstantValueExpression

  auto comparison_expression = dynamic_cast<ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
  if (comparison_expression == nullptr) return optimized_plan;
  if (comparison_expression->comp_type_ != ComparisonType::Equal) return optimized_plan;

  auto column_value = dynamic_cast<ColumnValueExpression *>(
    comparison_expression->GetChildAt(0).get()
  );
  if (column_value == nullptr) return optimized_plan;  // ?

  auto constant_value = dynamic_cast<ConstantValueExpression *>(
    comparison_expression->GetChildAt(1).get()
  );
  if (constant_value == nullptr) return optimized_plan;  // ?


  auto oid = seq_scan_plan.GetTableOid();
  auto table = this->catalog_.GetTable(oid);
  auto idxs =  this->catalog_.GetTableIndexes(table->name_);
  
  for (auto index: idxs) {
    // compare index
    auto attr = index->index_->GetKeyAttrs();
    if (attr.size() == 1 && attr[0] == column_value->GetColIdx()) {

      return std::make_shared<IndexScanPlanNode>(
        seq_scan_plan.OutputSchema(),
        oid,
        index->index_oid_,
        seq_scan_plan.filter_predicate_,
        constant_value
      );
    }
  }

  return optimized_plan;
}

}  // namespace bustub
