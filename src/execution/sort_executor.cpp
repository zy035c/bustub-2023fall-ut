#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
    this->child_executor_->Init();
    this->tuples.clear();

    /* Iteration to obtain all tuples */
    while (1) {
        Tuple tuple_;
        RID rid_;
        bool ok = this->child_executor_->Next(&tuple_, &rid_);
        if (!ok) break;
        this->tuples.push_back(tuple_);
    }
    /* end */

    /* Define cmp lambda */
    auto schema_cmp = this->child_executor_->GetOutputSchema();
    auto orders_bys_cmp = this->plan_->order_bys_;
    auto cmp = [schema_cmp, orders_bys_cmp](const Tuple &a, const Tuple &b) {

        for (auto [order_by_type, expr]: orders_bys_cmp) {

            auto a_val = expr->Evaluate(&a, schema_cmp);
            auto b_val = expr->Evaluate(&b, schema_cmp);

            switch(order_by_type) {
                case OrderByType::INVALID: // NOLINT
                case OrderByType::DEFAULT: // NOLINT
                case OrderByType::DESC:
                    if (a_val.CompareLessThan(b_val) == CmpBool::CmpTrue) return true;
                    if (b_val.CompareLessThan(a_val) == CmpBool::CmpTrue) return false;
                    break;
                case OrderByType::ASC: 
                    if (a_val.CompareGreaterThan(b_val) == CmpBool::CmpTrue) return true;
                    if (b_val.CompareGreaterThan(a_val) == CmpBool::CmpTrue) return false;
                    break;
            }
        }
        return false;
    };

    std::sort(tuples.begin(), tuples.end(), cmp);
    this->it_ = this->tuples.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (it_ == this->tuples.end()) return false;

    *tuple = *it_;
    *rid = tuple->GetRid();
    ++it_;

    return true; 
}

}  // namespace bustub
