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
    auto [tupleMeta_, tuple_] = it->GetTuple();

    /* check tuple timestamp with current read stamp */
    if (tupleMeta_.ts_ <= this->exec_ctx_->GetTransaction()->GetReadTs()) {
        return YieldTuple(tupleMeta_, tuple_, tuple, rid);
    } else if (tupleMeta_.ts_ >> 63 
                && (tupleMeta_.ts_ << 1) >> 1 == this->exec_ctx_->GetTransaction()->GetTransactionId()
            ) {
        return YieldTuple(tupleMeta_, tuple_, tuple, rid);
    } else { 
        /* The tuple in the table heap is (1) modified by another uncommitted transaction, 
        or (2) newer than the transaction read timestamp. In this case, 
        you will need to iterate the version chain to collect all undo logs after the read timestamp, 
        and recover the past version of the tuple.*/
        auto tnx_mgr = this->exec_ctx_->GetTransactionManager();
        auto undoLink = tnx_mgr->GetUndoLink(tuple_.GetRid());
        if (undoLink == std::nullopt) {
            ++(*it);
            return this->Next(tuple, rid);
        }


        tnx_mgr->

        /* iterate version chain */
        auto log_ids = undoLink.value().;
        exec_ctx_->GetTransaction()->GetUndoLog()
        // auto opt_res = ReconstructTuple()
    }

    auto rid_ = it->GetRID();


    if (tupleMeta_.is_deleted_) {
        ++(*it);  // ? +it
        return this->Next(tuple, rid);
    }

    *tuple = tuple_;
    *rid = rid_;
    ++(*it);
    return true;
}

auto SeqScanExecutor::YieldTuple(TupleMeta& tupleMeta_, Tuple& tuple_, Tuple *tuple, RID *rid) -> bool {
    if (tupleMeta_.is_deleted_) {
        ++(*it);
        return this->Next(tuple, rid);
    } else {
        *tuple = tuple_;
        *rid = tuple->GetRid();
        ++(*it);
        return true;
    }
}

}  // namespace bustub
