//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
    this->child_executor_->Init();
    this->count_ = 0;
}
auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (this->count_ == this->plan_->GetLimit()) return false;

    bool ok = this->child_executor_->Next(tuple, rid);
    if (!ok) return false;

    ++this->count_;
    return true;
}

}  // namespace bustub
