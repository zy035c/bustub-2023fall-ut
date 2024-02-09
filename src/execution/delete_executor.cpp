//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 

    auto oid = this->plan_->GetTableOid();
    auto catalog = this->GetExecutorContext()->GetCatalog();
    auto table = catalog->GetTable(oid); 
    auto idxs = catalog->GetTableIndexes(table->name_);

    int count = 0;
    while (1) {
        Tuple tuple_;
        auto ok = this->child_executor_->Next(&tuple_, rid);
        if (!ok) {
            break;
        }

        table->table_->UpdateTupleMeta(
            {0, true},
            tuple_.GetRid()
        );

        // delete index
        for (auto index : idxs) {
            index->index_->DeleteEntry(
                tuple_.KeyFromTuple(
                    table->schema_,
                    index->key_schema_,
                    index->index_->GetKeyAttrs()
                ),
                tuple_.GetRid(),
                &Transaction(INVALID_TXN_ID)
            );
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
