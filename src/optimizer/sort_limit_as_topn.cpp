#include "optimizer/optimizer.h"
#include "sort_plan.h"
#include "limit_plan.h"
#include "topn_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::Limit) return optimized_plan;

  const auto &lim_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
  BUSTUB_ENSURE(lim_plan.children_.size() == 1, "Limit Plan should have exactly 1 child.");

  if (lim_plan.GetChildAt(0)->GetType() != PlanType::Sort) return optimized_plan;

  const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*lim_plan.GetChildAt(0));
  BUSTUB_ENSURE(sort_plan.children_.size() == 1, "Sort Plan should have exactly 1 child.");

  return std::make_shared<TopNPlanNode>(
    plan->OutputSchema(),
    sort_plan.GetChildAt(0),
    sort_plan.order_bys_,
    lim_plan.GetLimit()
  );
}

}  // namespace bustub
