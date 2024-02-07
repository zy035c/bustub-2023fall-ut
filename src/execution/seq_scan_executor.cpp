//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    auto oid = this->plan_->GetTableOid();
    auto table = this->GetExecutorContext()->GetCatalog()->GetTable(oid);
    this->it = &table->table_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (it->IsEnd()) {
        return false;
    }
    auto pair = it->GetTuple();
    auto rid_ = it->GetRID();
    if (pair.first.is_deleted_) {
        ++(*it);  // ? +it
        return this->Next(tuple, rid);
    }

    *tuple = pair.second;
    *rid = rid_;
    ++(*it);
    return true;
}

}  // namespace bustub
