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
#include <memory>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
  auto lock_mgr = GetExecutorContext()->GetLockManager();
  auto txn = GetExecutorContext()->GetTransaction();
  LOG_DEBUG("iso level:%s", LockManager::GetIsolationLevelString(txn->GetIsolationLevel()));
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      auto succuss = lock_mgr->LockTable(txn, LockManager::LockMode::SHARED, table_info_->oid_);
      if (!succuss) {
        txn->SetState(TransactionState::ABORTED);
        throw ExecutionException("lock failed");
      }
    } catch (TransactionAbortException e) {
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
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*table_iterator_ == table_info_->table_->End()) {
    auto lock_mgr = GetExecutorContext()->GetLockManager();
    auto txn = GetExecutorContext()->GetTransaction();
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_mgr->UnlockTable(txn, table_info_->oid_);
    }
    return false;
  }
  auto &table_schema = table_info_->schema_;
  const Schema &output_schema = plan_->OutputSchema();
  auto row_tuple = *(*table_iterator_);

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  for (size_t i = 0; i < values.capacity(); i++) {
    values.emplace_back(row_tuple.GetValue(&table_schema, i));
  }
  ++(*table_iterator_);

  *tuple = Tuple(values, &output_schema);
  *rid = row_tuple.GetRid();
  return true;
}

}  // namespace bustub
