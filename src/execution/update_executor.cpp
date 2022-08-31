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
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple old_tuple;
  Tuple new_tuple;
  RID tuple_rid;
  Transaction *txn = exec_ctx_->GetTransaction();
  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  while (true) {
    try {
      if (!child_executor_->Next(&old_tuple, &tuple_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "UpdateExecutor:child execute error.");
    }

    if (lock_mgr != nullptr) {
      if (txn->IsSharedLocked(tuple_rid)) {
        lock_mgr->LockUpgrade(txn, tuple_rid);
      } else if (!txn->IsExclusiveLocked(tuple_rid)) {
        lock_mgr->LockExclusive(txn, tuple_rid);
      }
    }

    new_tuple = GenerateUpdatedTuple(old_tuple);
    TableHeap *table_heap = table_info_->table_.get();
    table_heap->UpdateTuple(new_tuple, tuple_rid, exec_ctx_->GetTransaction());

    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      auto index_info = index->index_.get();
      index_info->DeleteEntry(
          old_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()),
          tuple_rid, exec_ctx_->GetTransaction());
      index_info->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()),
          tuple_rid, exec_ctx_->GetTransaction());
      IndexWriteRecord write_record(tuple_rid, table_info_->oid_, WType::UPDATE, new_tuple, index->index_oid_,
                                    exec_ctx_->GetCatalog());
      write_record.old_tuple_ = old_tuple;
      txn->GetIndexWriteSet()->emplace_back(write_record);
    }

    if (lock_mgr != nullptr && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->Unlock(txn, tuple_rid);
    }
  }
  return false;
}

auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
