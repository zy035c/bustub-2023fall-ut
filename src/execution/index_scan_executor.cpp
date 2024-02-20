//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
    auto oid = this->plan_->table_oid_;
    auto catalog = this->GetExecutorContext()->GetCatalog();
    this->table = catalog->GetTable(oid); 
    auto idxs = catalog->GetIndex(this->plan_->GetIndexOid());

    auto htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(idxs->index_.get());

    Tuple tuple_;

    auto value = this->plan_->pred_key_->Evaluate(
        &tuple_, 
        this->GetOutputSchema()
    );

    htable_->ScanKey(
        Tuple(std::vector<Value>{value}, &this->GetOutputSchema()),
        &this->results,
        &Transaction(INVALID_TXN_ID)
    );
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    if (this->results.size() == 0) {
        return false;
    }
    
    *rid = this->results.back();
    results.pop_back();

    auto pair = this->table->table_->GetTuple(*rid);
    if (pair.first.is_deleted_) {
        return this->Next(tuple, rid);
    }

    *tuple = pair.second;  // must copy
    return true;
}

}  // namespace bustub
