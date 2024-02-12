#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
    this->child_executor_->Init();
    this->tuples.clear();

    /* Define cmp lambda */
    auto schema_cmp = this->child_executor_->GetOutputSchema();
    auto orders_bys_cmp = this->plan_->order_bys_;
    auto cmp = [schema_cmp, orders_bys_cmp](const Tuple &a, const Tuple &b) {

        for (auto [order_by_type, expr]: orders_bys_cmp) {

            auto a_val = expr->Evaluate(&a, schema_cmp);
            auto b_val = expr->Evaluate(&b, schema_cmp);

            switch(order_by_type) {
                case OrderByType::INVALID: // pass
                case OrderByType::DEFAULT: // pass
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

    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);

    /* Iteration adding all tuples to priority queue. Keep top n elems in this->tuples */
    while (1) {
        Tuple tuple_;
        RID rid_;
        bool ok = this->child_executor_->Next(&tuple_, &rid_);
        if (!ok) break;
        pq.push(tuple_);
        if (pq.size() > this->plan_->GetN()) {
            pq.pop();
        }
    }
    while (!pq.empty()) {
        this->tuples.push_back(pq.top());
        pq.pop();
    }
    /* end */

    std::reverse(this->tuples.begin(), this->tuples.end());
    this->it_ = this->tuples.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (it_ == this->tuples.end()) return false;

    *tuple = *it_;
    *rid = tuple->GetRid();
    ++it_;

    return true; 
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return this->tuples.size();
};

}  // namespace bustub
