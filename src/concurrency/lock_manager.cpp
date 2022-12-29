
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
#include <utility>

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LOG_DEBUG("locking txn_id:%d, oid:%d, lock_mode:%s", (int)txn->GetTransactionId(), (int)oid,
            GetLockModeString(lock_mode));
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED &&
        lock_mode != LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING || lock_mode == LockMode::SHARED ||
        lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }

  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }

  std::unique_lock<std::mutex> queue_lock(table_lock_map_[oid]->latch_);
  auto &lock_request_queue = table_lock_map_[oid];

  auto &request_queue = lock_request_queue->request_queue_;
  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });

  bool upgrading = false;
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  if (lock_request_iterator != request_queue.end()) {         // upgrade
    if ((*lock_request_iterator)->lock_mode_ == lock_mode) {  // same return true
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    if (IsUpgradeValid((*lock_request_iterator)->lock_mode_, lock_mode)) {
      switch ((*lock_request_iterator)->lock_mode_) {
        case LockMode::INTENTION_SHARED:  //  IS -> [S, X, IX, SIX]
          txn->GetIntentionSharedTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        case LockMode::SHARED:  //  S -> [X, SIX]
          txn->GetSharedTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        case LockMode::INTENTION_EXCLUSIVE:  //  IX -> [X, SIX]
          txn->GetIntentionExclusiveTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:  //  SIX -> [X]
          txn->GetSharedIntentionExclusiveTableLockSet()->erase((*lock_request_iterator)->oid_);
          break;
        default:
          break;
      }
      request_queue.erase(lock_request_iterator);
      request_queue.emplace_front(new_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      upgrading = true;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  } else {
    request_queue.emplace_back(new_lock_request);
  }
  table_lock.unlock();

  auto check = [&]() -> bool {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto it = request_queue.begin();
    while ((*it)->txn_id_ != txn->GetTransactionId()) {
      if (!IsLockCompatible((*it)->lock_mode_, lock_mode)) {
        return false;
      }
      it++;
    }
    it++;
    while (it != request_queue.end()) {
      if ((*it)->granted_ && !IsLockCompatible((*it)->lock_mode_, lock_mode)) {
        return false;
      }
      it++;
    }
    return true;
  };
  while (!check()) {
    lock_request_queue->cv_.wait(queue_lock);
  }

  if (upgrading) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.remove(new_lock_request);
    lock_request_queue->cv_.notify_all();
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
  LOG_DEBUG("locked txn_id:%d, oid:%d, lock mode:%s", (int)txn->GetTransactionId(), (int)oid,
            GetLockModeString(lock_mode));
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_DEBUG("unlocking txn_id:%d, oid:%d", (int)txn->GetTransactionId(), (int)oid);
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  std::unique_lock<std::mutex> queue_lock(table_lock_map_[oid]->latch_);
  auto &lock_request_queue = table_lock_map_[oid];

  auto &request_queue = lock_request_queue->request_queue_;
  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });

  if (lock_request_iterator == request_queue.end() || !(*lock_request_iterator)->granted_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto &s_row_lock_set = (*txn->GetSharedRowLockSet())[oid];
  auto &x_row_lock_set = (*txn->GetExclusiveRowLockSet())[oid];
  if (!s_row_lock_set.empty() || !x_row_lock_set.empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }

  auto lock_mode = (*lock_request_iterator)->lock_mode_;
  auto iso_level = txn->GetIsolationLevel();

  request_queue.erase(lock_request_iterator);
  table_lock.unlock();

  // change state
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
  lock_request_queue->cv_.notify_all();
  LOG_DEBUG("unlocked txn_id:%d, oid:%d", (int)txn->GetTransactionId(), (int)oid);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING && (lock_mode == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING || lock_mode == LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }
  // lock row must lock table first
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
      return false;
    }
  }

  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  std::unique_lock<std::mutex> queue_lock(row_lock_map_[rid]->latch_);
  auto &lock_request_queue = row_lock_map_[rid];

  auto &request_queue = lock_request_queue->request_queue_;
  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });
  bool upgrading = false;
  auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  if (lock_request_iterator != request_queue.end()) {         // upgrade
    if ((*lock_request_iterator)->lock_mode_ == lock_mode) {  // same return true
      return true;
    }
    if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    if (IsUpgradeValid((*lock_request_iterator)->lock_mode_, lock_mode)) {
      switch ((*lock_request_iterator)->lock_mode_) {
        case LockMode::SHARED:  //  S -> [X, SIX]
          txn->GetSharedLockSet()->erase((*lock_request_iterator)->rid_);
          txn->GetSharedRowLockSet()->at(oid).erase((*lock_request_iterator)->rid_);
          break;
        default:
          break;
      }
      request_queue.erase(lock_request_iterator);
      request_queue.emplace_front(new_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      upgrading = true;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  } else {
    request_queue.emplace_back(new_lock_request);
  }
  row_lock.unlock();

  auto check = [&]() -> bool {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    auto it = request_queue.begin();
    while ((*it)->txn_id_ != txn->GetTransactionId()) {
      if (!IsLockCompatible((*it)->lock_mode_, lock_mode)) {
        return false;
      }
      it++;
    }
    it++;
    while (it != request_queue.end()) {
      if (((*it)->granted_ && !IsLockCompatible((*it)->lock_mode_, lock_mode))) {
        return false;
      }
      it++;
    }

    return true;
  };
  while (!check()) {
    lock_request_queue->cv_.wait(queue_lock);
  }

  if (upgrading) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.remove(new_lock_request);
    lock_request_queue->cv_.notify_all();
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
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  std::unique_lock<std::mutex> queue_lock(row_lock_map_[rid]->latch_);
  auto &lock_request_queue = row_lock_map_[rid];

  auto &request_queue = lock_request_queue->request_queue_;
  auto lock_request_iterator =
      std::find_if(request_queue.begin(), request_queue.end(),
                   [txn](const auto &lock_request) { return lock_request->txn_id_ == txn->GetTransactionId(); });

  if (lock_request_iterator == request_queue.end() || !(*lock_request_iterator)->granted_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }

  auto lock_mode = (*lock_request_iterator)->lock_mode_;
  auto iso_level = txn->GetIsolationLevel();

  request_queue.erase(lock_request_iterator);
  row_lock.unlock();

  // change state
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
  lock_request_queue->cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].emplace_back(t2);
  // LOG_DEBUG("add edge %d -> %d", (int)t1, (int)t2);
  // vertices_.insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (it != waits_for_[t1].end()) {
    waits_for_[t1].erase(it);
  }
}
auto LockManager::HasCycleUtil(txn_id_t &txn_id) -> bool {
  if (!visit_[txn_id]) {
    visit_[txn_id] = true;
    recurv_stack_[txn_id] = true;
    for (auto &holds : waits_for_[txn_id]) {
      if (!visit_[holds] && HasCycleUtil(holds)) {
        return true;
      }
      if (recurv_stack_[holds]) {
        return true;
      }
    }
    recurv_stack_[txn_id] = false;
    return false;
  }
  return true;
}
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::set<txn_id_t> txn_set;
  visit_.clear();
  recurv_stack_.clear();
  // LOG_DEBUG("graph size:%d", (int)waits_for_.size());
  for (const auto &wait : waits_for_) {
    txn_set.insert(wait.first);
  }
  // LOG_DEBUG("txn_set size:%d", (int)txn_set.size());
  for (auto vertice : txn_set) {
    if (!visit_[vertice]) {
      if (HasCycleUtil(vertice)) {
        // get the min t
        std::set<txn_id_t, std::greater<>> tp_set;
        for (auto it : recurv_stack_) {
          if (it.second) {
            tp_set.insert(it.first);
          }
        }
        *txn_id = *tp_set.begin();
        tp_set.clear();
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  // LOG_DEBUG("get edge");
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &wait : waits_for_) {
    auto t1 = wait.first;
    for (const auto &t2 : wait.second) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::DrawGraph() {
  for (auto &row_lock : row_lock_map_) {
    std::unique_lock<std::mutex> queue_lock(row_lock.second->latch_);
    auto &request_queue = row_lock.second->request_queue_;
    for (auto granted_request = request_queue.begin(); granted_request != request_queue.end(); granted_request++) {
      row_txn_request_[(*granted_request)->txn_id_].emplace_back(granted_request);

      if (!(*granted_request)->granted_) {
        continue;
      }
      for (auto &waiting_request : request_queue) {
        if (waiting_request->granted_) {
          continue;
        }
        if ((*granted_request)->lock_mode_ == LockMode::EXCLUSIVE ||
            waiting_request->lock_mode_ == LockMode::EXCLUSIVE) {
          AddEdge(waiting_request->txn_id_, (*granted_request)->txn_id_);
        }
      }
    }
    queue_lock.unlock();
  }
  for (auto &table_lock : table_lock_map_) {
    std::unique_lock<std::mutex> queue_lock(table_lock.second->latch_);
    auto &request_queue = table_lock.second->request_queue_;
    for (auto granted_request = request_queue.begin(); granted_request != request_queue.end(); granted_request++) {
      table_txn_request_[(*granted_request)->txn_id_].emplace_back(granted_request);
      if (!(*granted_request)->granted_) {
        continue;
      }
      for (auto &waiting_request : request_queue) {
        if (waiting_request->granted_) {
          continue;
        }
        if ((waiting_request->lock_mode_ == LockMode::INTENTION_SHARED &&
             (*granted_request)->lock_mode_ == LockMode::EXCLUSIVE) ||
            (waiting_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
             ((*granted_request)->lock_mode_ != LockMode::INTENTION_SHARED &&
              (*granted_request)->lock_mode_ != LockMode::INTENTION_EXCLUSIVE)) ||
            (waiting_request->lock_mode_ == LockMode::SHARED &&
             ((*granted_request)->lock_mode_ != LockMode::SHARED &&
              (*granted_request)->lock_mode_ != LockMode::INTENTION_SHARED)) ||
            (waiting_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE &&
             (*granted_request)->lock_mode_ != LockMode::INTENTION_SHARED) ||
            (waiting_request->lock_mode_ == LockMode::EXCLUSIVE)) {
          AddEdge(waiting_request->txn_id_, (*granted_request)->txn_id_);
        }
      }
    }
    queue_lock.unlock();
  }

  // sort;
  for (auto &it : waits_for_) {
    sort(it.second.begin(), it.second.end());
  }
}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      std::unique_lock<std::mutex> dld(waits_for_latch_);
      std::unique_lock<std::mutex> row_lock(row_lock_map_latch_);
      std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
      DrawGraph();
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        //
        LOG_DEBUG("cycle abort txn: %d", (int)txn_id);
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        txn->GetSharedRowLockSet()->clear();
        txn->GetExclusiveRowLockSet()->clear();
        txn->GetIntentionExclusiveTableLockSet()->clear();
        txn->GetIntentionSharedTableLockSet()->clear();
        txn->GetSharedTableLockSet()->clear();
        txn->GetExclusiveTableLockSet()->clear();
        txn->GetSharedIntentionExclusiveTableLockSet()->clear();
        txn->GetExclusiveLockSet()->clear();
        txn->GetSharedLockSet()->clear();
        waits_for_.erase(txn_id);
        auto &&edges = GetEdgeList();
        for (auto &edge : edges) {
          if (edge.second == txn_id) {
            RemoveEdge(edge.first, edge.second);
          }
        }
        for (auto &it : row_txn_request_[txn_id]) {
          auto rid = it->get()->rid_;
          auto granted = it->get()->granted_;
          row_lock_map_[rid]->request_queue_.erase(it);
          if (granted) {
            row_lock_map_[rid]->cv_.notify_all();
          }
        }
        for (auto &it : table_txn_request_[txn_id]) {
          auto oid = it->get()->oid_;
          auto granted = it->get()->granted_;
          table_lock_map_[oid]->request_queue_.erase(it);
          if (granted) {
            table_lock_map_[oid]->cv_.notify_all();
          }
        }
        //
      }
      waits_for_.clear();
      visit_.clear();
      recurv_stack_.clear();
      table_txn_request_.clear();
      row_txn_request_.clear();
    }
  }
}

}  // namespace bustub
