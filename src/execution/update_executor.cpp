//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  
}

void UpdateExecutor::Init() { 
    this->child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    
    if (this->is_end_) return false;

    auto txn = this->exec_ctx_->GetTransaction();

    auto oid = this->plan_->GetTableOid();
    auto catalog = this->GetExecutorContext()->GetCatalog();
    auto table = catalog->GetTable(oid); 
    auto idxs = catalog->GetTableIndexes(table->name_);

    int count = 0;

    while (1) {
        Tuple tuple_;
        RID old_rid;

        // Get the next tuple        
        auto ok = this->child_executor_->Next(&tuple_, &old_rid);
        if (!ok) {
            break;
        }

        // compute expressions
        std::vector<Value> values{};
        for (auto expr : this->plan_->target_expressions_) {
            auto val = expr->Evaluate(&tuple_, child_executor_->GetOutputSchema());
            values.push_back(val);
        }
        auto new_tuple_ = Tuple{values, &child_executor_->GetOutputSchema()};

        // Implement your project 3 update executor as delete and insert. 
        table->table_->UpdateTupleMeta(
            {0, true}, old_rid
        );
        auto rid__ = table->table_->InsertTuple(
            {0, false}, new_tuple_
        );
        if (rid__ == std::nullopt) {
            throw bustub::Exception("insertion failed");
            return false;
        }

        // update index
        for (auto index : idxs) {
            index->index_->DeleteEntry(
                tuple_.KeyFromTuple(
                    child_executor_->GetOutputSchema(),
                    index->key_schema_,
                    index->index_->GetKeyAttrs()
                ),
                tuple_.GetRid(),
                &Transaction(INVALID_TXN_ID)
            );
            bool inserted = index->index_->InsertEntry(
                new_tuple_.KeyFromTuple(
                    child_executor_->GetOutputSchema(),
                    index->key_schema_,
                    index->index_->GetKeyAttrs()
                ),
                rid__.value(),
                &Transaction(INVALID_TXN_ID)
            );
            if (!inserted) {
                throw bustub::Exception("index insertion failed");
            }
        }

        ++count;
    }
    if (count == 0) return false;
    *tuple = Tuple{
        std::vector<Value>{{TypeId::INTEGER, count}}, 
        &this->GetOutputSchema()
    };
    return true; 
}

}  // namespace bustub
