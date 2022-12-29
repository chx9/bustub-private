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

#include "common/logger.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
  index_infos_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  access_ = true;
  auto txn = GetExecutorContext()->GetTransaction();
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  LOG_DEBUG("iso level:%s", LockManager::GetIsolationLevelString(txn->GetIsolationLevel()));
  try {
    bool success = lock_mgr->LockTable(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_);
    if (!success) {
      txn->SetState(TransactionState::ABORTED);
      throw ExecutionException("lock failed");
    }
  } catch (TransactionAbortException e) {
    LOG_DEBUG("abort");
    switch (e.GetAbortReason()) {
      case AbortReason::LOCK_ON_SHRINKING:
      case AbortReason::UPGRADE_CONFLICT:
      case AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED:
      case AbortReason::TABLE_LOCK_NOT_PRESENT:
      case AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW:
      case AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS:
      case AbortReason::INCOMPATIBLE_UPGRADE:
      case AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD:
        break;
    }
  }
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  int i = 0;
  while (child_executor_->Next(&child_tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, GetExecutorContext()->GetTransaction())) {
      for (auto &index_info : index_infos_) {
        index_info->index_->DeleteEntry(
            child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            *rid, GetExecutorContext()->GetTransaction());
      }
      i++;
    }
  }

  if (access_) {
    access_ = false;
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    values.emplace_back(INTEGER, i);
    *tuple = Tuple{values, &GetOutputSchema()};

    auto lock_mgr = GetExecutorContext()->GetLockManager();
    auto txn = GetExecutorContext()->GetTransaction();
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->UnlockTable(txn, table_info_->oid_);
    }
    return true;
  }
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto txn = GetExecutorContext()->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    lock_mgr->UnlockTable(txn, table_info_->oid_);
  }
  return false;
}

}  // namespace bustub
