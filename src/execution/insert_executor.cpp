//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    // TODO
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    auto oid = this->plan_->GetTableOid();
    auto catalog = this->GetExecutorContext()->GetCatalog();
    auto table = catalog->GetTable(oid); 
    auto idxs = catalog->GetTableIndexes(table->name_);

    int32_t count = 0;
    // For the insertion_txn_ and the deletion_txn_ fields, just set it to INVALID_TXN_ID. 
    // These fields are intended to be used in future semesters where we might switch to an MVCC storage.
    while (1) {
        Tuple tuple_;
        auto ok = this->child_executor_->Next(&tuple_, rid);
        if (!ok) {
            break;
        }

        auto rid__ = table->table_->InsertTuple({0, false}, tuple_);
        if (rid__ == std::nullopt) {
            throw bustub::Exception("insertion failed");
            return false;
        }

        for (auto index : idxs) {
            index->index_->InsertEntry(
                tuple_.KeyFromTuple(
                    table->schema_,
                    index->key_schema_,
                    index->index_->GetKeyAttrs()
                ),
                rid__.value(),
                &Transaction(INVALID_TXN_ID)
            );
        }

        ++count;
    }
    if (count == 0) {
        return false;
    }

    *tuple = Tuple{
        std::vector<Value>{{TypeId::INTEGER, count}}, 
        &this->GetOutputSchema()
    };
    return true; 
}

}  // namespace bustub
