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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = nullptr;
}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  RID cur_rid;
  Tuple cur_tuple;
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();

  while (true) {
    try {
      if (!child_executor_->Next(&cur_tuple, &cur_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor: child execute error");
    }

    if (lock_mgr != nullptr) {
      if (txn->IsSharedLocked(cur_rid)) {
        lock_mgr->LockUpgrade(txn, cur_rid);
      } else if (!txn->IsExclusiveLocked(cur_rid)) {
        lock_mgr->LockExclusive(txn, cur_rid);
      }
    }

    TableHeap *table_heap = table_info_->table_.get();
    table_heap->MarkDelete(cur_rid, txn);

    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_info = index->index_.get();
      index_info->DeleteEntry(
          cur_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), cur_rid,
          exec_ctx_->GetTransaction());
      IndexWriteRecord write_record(cur_rid, table_info_->oid_, WType::DELETE, cur_tuple, index->index_oid_,
                                    exec_ctx_->GetCatalog());
      txn->GetIndexWriteSet()->emplace_back(write_record);
    }

    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(txn, cur_rid);
    }
  }
  return false;
}

}  // namespace bustub
