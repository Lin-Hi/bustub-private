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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap_(nullptr), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == table_heap_->End()) {
    return false;
  }

  RID original_rid = iter_->GetRid();
  const Schema *output_schema = plan_->OutputSchema();

  LockManager *lock_mgr = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();

  if (lock_mgr != nullptr) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      if (!txn->IsExclusiveLocked(original_rid) && !txn->IsSharedLocked(original_rid)) {
        lock_mgr->LockShared(txn, original_rid);
      }
    }
  }

  std::vector<Value> vals;
  vals.reserve(output_schema->GetColumnCount());
  for (size_t i = 0; i < vals.capacity(); i++) {
    vals.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(
        &(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)));
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
    lock_mgr->Unlock(txn, original_rid);
  }

  ++iter_;

  Tuple temp_tuple(vals, output_schema);

  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(&temp_tuple, output_schema).GetAs<bool>()) {
    *tuple = temp_tuple;
    *rid = original_rid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
