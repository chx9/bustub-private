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

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
        lock_mode != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING || lock_mode == LockMode::SHARED ||
        lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  auto &request_queue = table_lock_map_[oid]->request_queue_;
  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [&txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });

  bool upgrading = false;
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  if (lock_request_iterator != request_queue.end()) {  // upgrade
    if ((*lock_request_iterator)->lock_mode_ == lock_mode) {
      return true;
    }
    if (table_lock_map_[oid]->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (UpgradeValid((*lock_request_iterator)->lock_mode_, lock_mode)) {
      switch ((*lock_request_iterator)->lock_mode_) {
        case LockMode::INTENTION_SHARED:
          txn->GetIntentionSharedTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        case LockMode::SHARED:
          txn->GetSharedTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          txn->GetIntentionExclusiveTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->GetSharedIntentionExclusiveTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        default:
          break;
      }
      LOG_INFO("【upgrade】");
      request_queue.erase(lock_request_iterator);
      request_queue.emplace_front(new_lock_request);
      table_lock_map_[oid]->upgrading_ = txn->GetTransactionId();
      upgrading = true;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else {
    request_queue.emplace_back(new_lock_request);
  }

  auto check = [&]() -> bool {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto it = request_queue.begin();
    while ((*it)->txn_id_ != txn->GetTransactionId()) {
      if ((lock_mode == LockMode::INTENTION_SHARED && (*it)->lock_mode_ == LockMode::EXCLUSIVE) ||
          (lock_mode == LockMode::INTENTION_EXCLUSIVE &&
           ((*it)->lock_mode_ != LockMode::INTENTION_SHARED && (*it)->lock_mode_ != LockMode::INTENTION_EXCLUSIVE)) ||
          (lock_mode == LockMode::SHARED &&
           ((*it)->lock_mode_ != LockMode::SHARED && (*it)->lock_mode_ != LockMode::INTENTION_SHARED)) ||
          (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && (*it)->lock_mode_ != LockMode::INTENTION_SHARED) ||
          (lock_mode == LockMode::EXCLUSIVE)) {
        return false;
      }
      it++;
    }
    it++;
    while (it != request_queue.end()) {
      if ((*it)->granted_ &&
          ((lock_mode == LockMode::INTENTION_SHARED && (*it)->lock_mode_ == LockMode::EXCLUSIVE) ||
           (lock_mode == LockMode::INTENTION_EXCLUSIVE &&
            ((*it)->lock_mode_ != LockMode::INTENTION_SHARED && (*it)->lock_mode_ != LockMode::INTENTION_EXCLUSIVE)) ||
           (lock_mode == LockMode::SHARED &&
            ((*it)->lock_mode_ != LockMode::SHARED && (*it)->lock_mode_ != LockMode::INTENTION_SHARED)) ||
           (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && (*it)->lock_mode_ != LockMode::INTENTION_SHARED) ||
           (lock_mode == LockMode::EXCLUSIVE))) {
        return false;
      }
      it++;
    }
    return true;
  };
  while (!check()) {
    table_lock_map_[oid]->cv_.wait(lock);
  }

  if (upgrading) {
    table_lock_map_[oid]->upgrading_ = INVALID_TXN_ID;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.remove(new_lock_request);
    table_lock_map_[oid]->cv_.notify_all();
    return false;
  }

  new_lock_request->granted_ = true;

  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->emplace(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->emplace(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->emplace(oid);
      break;
    default:
      break;
  }
  LOG_INFO("【lockTable END】txn_id = %d, oid = %d, lock_mode = %d", txn->GetTransactionId(), oid, lock_mode);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> lock(table_lock_map_latch_);
  LOG_INFO("【UnlockTable START】txn_id = %d, oid = %d", txn->GetTransactionId(), oid);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto &request_queue = table_lock_map_[oid]->request_queue_;

  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });

  if (lock_request_iterator == request_queue.end() || !(*lock_request_iterator)->granted_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto &s_row_lock_set = (*txn->GetSharedRowLockSet())[oid];
  auto &x_row_lock_set = (*txn->GetExclusiveRowLockSet())[oid];
  if (!s_row_lock_set.empty() || !x_row_lock_set.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto lock_mode = (*lock_request_iterator)->lock_mode_;
  auto iso_level = txn->GetIsolationLevel();

  request_queue.erase(lock_request_iterator);

  if (txn->GetState() == TransactionState::GROWING &&
      ((iso_level == IsolationLevel::REPEATABLE_READ &&
        (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) ||
       (iso_level == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::EXCLUSIVE) ||
       (iso_level == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::EXCLUSIVE))) {
    txn->SetState(TransactionState::SHRINKING);
  }

  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    default:
      break;
  }
  table_lock_map_[oid]->cv_.notify_all();
  LOG_INFO("【UnlockTable END】txn_id = %d, oid = %d", txn->GetTransactionId(), oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  LOG_INFO("【lockRow START】txn_id = %d, oid = %d, rid = %ld, lock_mode = %d", txn->GetTransactionId(), oid, rid.Get(),
           lock_mode);
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING && (lock_mode == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING || lock_mode == LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  if (lock_mode == LockMode::EXCLUSIVE) {
    std::lock_guard<std::mutex> lock(table_lock_map_latch_);
    if (table_lock_map_.find(oid) == table_lock_map_.end() ||
        std::find_if(table_lock_map_[oid]->request_queue_.begin(), table_lock_map_[oid]->request_queue_.end(),
                     [&](const auto &lock_request) {
                       return lock_request->granted_ &&
                              (lock_request->lock_mode_ == LockMode::EXCLUSIVE ||
                               lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                               lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE);
                     }) == table_lock_map_[oid]->request_queue_.end()) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &request_queue = row_lock_map_[rid]->request_queue_;
  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });
  bool upgrading = false;
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  if (lock_request_iterator != request_queue.end()) {         // upgrade
    if ((*lock_request_iterator)->lock_mode_ == lock_mode) {  // same return true
      return true;
    }
    if (row_lock_map_[rid]->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }
    if (UpgradeValid((*lock_request_iterator)->lock_mode_, lock_mode)) {
      switch ((*lock_request_iterator)->lock_mode_) {
        case LockMode::SHARED:  //  S -> [X, SIX]
          txn->GetSharedLockSet()->erase((*lock_request_iterator)->rid_);
          txn->GetSharedRowLockSet()->at(oid).erase((*lock_request_iterator)->rid_);
          break;
        default:
          break;
      }
      LOG_INFO("【upgrade】");
      request_queue.erase(lock_request_iterator);
      request_queue.emplace_front(new_lock_request);
      row_lock_map_[rid]->upgrading_ = txn->GetTransactionId();
      upgrading = true;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
    }
  } else {
    request_queue.emplace_back(new_lock_request);
  }

  auto check = [&]() -> bool {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto it = request_queue.begin();
    while ((*it)->txn_id_ != txn->GetTransactionId()) {
      if (lock_mode == LockMode::EXCLUSIVE || (*it)->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
      it++;
    }
    it++;
    while (it != request_queue.end()) {
      if (((*it)->granted_ && (lock_mode == LockMode::EXCLUSIVE || (*it)->lock_mode_ == LockMode::EXCLUSIVE))) {
        return false;
      }
      it++;
    }

    return true;
  };
  while (!check()) {
    row_lock_map_[rid]->cv_.wait(row_lock);
  }

  if (upgrading) {
    row_lock_map_[rid]->upgrading_ = INVALID_TXN_ID;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.remove(new_lock_request);
    row_lock_map_[rid]->cv_.notify_all();
    return false;
  }

  new_lock_request->granted_ = true;

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedLockSet()->emplace(rid);
      if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
        txn->GetSharedRowLockSet()->emplace(oid, std::unordered_set<RID>());
      }
      txn->GetSharedRowLockSet()->at(oid).emplace(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveLockSet()->emplace(rid);
      if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
        txn->GetExclusiveRowLockSet()->emplace(oid, std::unordered_set<RID>());
      }
      txn->GetExclusiveRowLockSet()->at(oid).emplace(rid);
      break;
    default:
      break;
  }

  LOG_INFO("【lockRow END】txn_id = %d, oid = %d, rid = %ld, lock_mode = %d", txn->GetTransactionId(), oid, rid.Get(),
           lock_mode);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  LOG_INFO("【UnlockRow START】txn_id = %d, oid = %d, rid = %ld", txn->GetTransactionId(), oid, rid.Get());
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto &request_queue = row_lock_map_[rid]->request_queue_;

  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });

  if (lock_request_iterator == request_queue.end() || !(*lock_request_iterator)->granted_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_mode = (*lock_request_iterator)->lock_mode_;
  auto iso_level = txn->GetIsolationLevel();

  request_queue.erase(lock_request_iterator);

  if (txn->GetState() == TransactionState::GROWING &&
      ((iso_level == IsolationLevel::REPEATABLE_READ &&
        (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) ||
       (iso_level == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::EXCLUSIVE) ||
       (iso_level == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::EXCLUSIVE))) {
    txn->SetState(TransactionState::SHRINKING);
  }

  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedLockSet()->erase(rid);
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveLockSet()->erase(rid);
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      break;
    default:
      break;
  }
  row_lock_map_[rid]->cv_.notify_all();
  LOG_INFO("【UnlockRow END】txn_id = %d, oid = %d, rid = %ld", txn->GetTransactionId(), oid, rid.Get());
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &wait : waits_for_) {
    auto t1 = wait.first;
    for (const auto &t2 : wait.second) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
