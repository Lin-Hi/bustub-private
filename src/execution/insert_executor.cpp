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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_heap_ = nullptr;
  catalog_ = nullptr;
  table_info_ = nullptr;
}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (plan_->IsRawInsert()) {
    for (const auto &row_value : plan_->RawValues()) {
      Tuple cur_tuple(row_value, &(table_info_->schema_));
      InsertIntoTableWithIndex(&cur_tuple);
    }
    return false;
  }

  std::vector<Tuple> child_tuples;
  child_executor_->Init();
  try {
    Tuple cur_tuple;
    RID cur_rid;
    while (child_executor_->Next(&cur_tuple, &cur_rid)) {
      child_tuples.push_back(cur_tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor:child execute error.");
  }

  for (auto &child_tuple : child_tuples) {
    InsertIntoTableWithIndex(&child_tuple);
  }
  return false;
}

void InsertExecutor::InsertIntoTableWithIndex(Tuple *cur_tuple) {
  RID cur_rid;

  if (!table_heap_->InsertTuple(*cur_tuple, &cur_rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple.");
  }

  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  if (lock_mgr != nullptr) {
    if (txn->IsSharedLocked(cur_rid)) {
      lock_mgr->LockUpgrade(txn, cur_rid);
    } else if (!txn->IsExclusiveLocked(cur_rid)) {
      lock_mgr->LockExclusive(txn, cur_rid);
    }
  }

  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    index->index_->InsertEntry(
        cur_tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
        cur_rid, exec_ctx_->GetTransaction());
    IndexWriteRecord write_record(cur_rid, table_info_->oid_, WType::INSERT, *cur_tuple, index->index_oid_,
                                  exec_ctx_->GetCatalog());
    txn->GetIndexWriteSet()->emplace_back(write_record);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
    lock_mgr->Unlock(txn, cur_rid);
  }
}

}  // namespace bustub
