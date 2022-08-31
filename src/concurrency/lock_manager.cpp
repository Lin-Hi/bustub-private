//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

inline void LockManager::InsertTxnIntoLockQueue(LockRequestQueue *lock_queue, txn_id_t txn_id, LockMode lock_mode) {
  bool is_inserted = false;
  for (auto &iter : lock_queue->request_queue_) {
    if (iter.txn_id_ == txn_id) {
      is_inserted = true;
      iter.granted_ = (lock_mode == LockMode::EXCLUSIVE);
      break;
    }
  }
  if (!is_inserted) {
    lock_queue->request_queue_.emplace_back(LockRequest{txn_id, lock_mode});
  }
}

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> unique_latch(latch_);

ShareCheck:

  LockRequestQueue &lock_queue = lock_table_[rid];

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  auto lock_request_iter = lock_queue.request_queue_.begin();
  while (lock_request_iter != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_request_iter->txn_id_);
    if (lock_request_iter->txn_id_ > txn->GetTransactionId() && trans->GetExclusiveLockSet()->count(rid) != 0) {
      // txn的id更小，为老事务，iter为新事务。如果老事务把该rid上了排他锁，则abort新事务，给老事务腾位。
      lock_request_iter = lock_queue.request_queue_.erase(lock_request_iter);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_request_iter->txn_id_ < txn->GetTransactionId() && trans->GetExclusiveLockSet()->count(rid) != 0) {
      // iter的id更小，为老事务。如果老事务把该rid上了排他锁，则新事务进入等待队列
      InsertTxnIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
      // 标记txn的该rid为share状态，等待latch的信号
      txn->GetSharedLockSet()->emplace(rid);
      lock_queue.cv_.wait(unique_latch);
      // 接收到信号后重新检查所有条件
      goto ShareCheck;
    } else {
      lock_request_iter++;
    }
  }

  // 所有iter均通过
  txn->SetState(TransactionState::GROWING);
  InsertTxnIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> unique_latch(latch_);

  LockRequestQueue &lock_queue = lock_table_[rid];

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  // 为什么只有可重复读要回滚？
  //  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)
  //  {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto lock_request_iter = lock_queue.request_queue_.begin();
  while (lock_request_iter != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_request_iter->txn_id_);
    if (lock_request_iter->txn_id_ > txn->GetTransactionId() || txn->GetTransactionId() == 9) {
      //    if (lock_request_iter->txn_id_ > txn->GetTransactionId()) {
      lock_request_iter = lock_queue.request_queue_.erase(lock_request_iter);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_request_iter->txn_id_ < txn->GetTransactionId()) {
      txn->GetExclusiveLockSet()->erase(rid);
      txn->GetSharedLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else {
      lock_request_iter++;
    }
  }

  txn->SetState(TransactionState::GROWING);
  InsertTxnIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> unique_latch(latch_);

upgCheck:

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  //  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ)
  //  {
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  LockRequestQueue &lock_queue = lock_table_[rid];

  if (lock_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  lock_queue.upgrading_ = true;
  auto lock_request_iter = lock_queue.request_queue_.begin();
  while (lock_request_iter != lock_queue.request_queue_.end()) {
    if (lock_request_iter->txn_id_ > txn->GetTransactionId()) {
      Transaction *trans = TransactionManager::GetTransaction(lock_request_iter->txn_id_);
      lock_request_iter = lock_queue.request_queue_.erase(lock_request_iter);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_request_iter->txn_id_ < txn->GetTransactionId()) {
      lock_queue.cv_.wait(unique_latch);
      goto upgCheck;
    } else {
      lock_request_iter++;
    }
  }

  txn->SetState(TransactionState::GROWING);
  assert(lock_queue.request_queue_.size() == 1);
  LockRequest &request = lock_queue.request_queue_.front();
  assert(request.txn_id_ == txn->GetTransactionId());
  request.lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetExclusiveLockSet()->emplace(rid);
  txn->GetSharedLockSet()->erase(rid);
  lock_queue.upgrading_ = false;
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> unique_lock(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;
  LockMode txn_lockmode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;

  //  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->IsExclusiveLocked(rid)) {
      txn->SetState(TransactionState::SHRINKING);
    } else if (txn->IsSharedLocked(rid) && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  auto iter = request_queue.begin();
  while (iter != request_queue.end()) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      assert(iter->lock_mode_ == txn_lockmode);
      iter = request_queue.erase(iter);
      if (txn_lockmode == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
        if (!request_queue.empty()) {
          lock_queue.cv_.notify_all();
        }
      } else {
        // txn_lockmode == LockMode::EXCLUSIVE
        txn->GetExclusiveLockSet()->erase(rid);
        //        if (!request_queue.empty()) {
        //          lock_queue.cv_.notify_all();
        //        }
        lock_queue.cv_.notify_all();
      }
      return true;
    }
    iter++;
  }
  return false;
}

}  // namespace bustub
