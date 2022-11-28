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
#include "concurrency/lock_manager.h"
#include "recovery/log_manager.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {}

void SeqScanExecutor::Init() { 
    table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    table_iterator_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(*table_iterator_ == table_info_->table_->End()){
        return false;
    }
    Tuple raw_tuple;
    raw_tuple = *(*table_iterator_);
    ++(*table_iterator_); 
    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
      std::transform(GetOutputSchema().GetColumns().begin(), GetOutputSchema().GetColumns().end(),
                 std::back_inserter(values), [&raw_tuple, &table_info = table_info_](const Column &col) {
                   return col.GetExpressions()->Evaluate(&raw_tuple, &(table_info->schema_));
                 });
 
    *tuple = Tuple{values, &GetOutputSchema()};
    *rid = tuple->GetRid();
    return true;
 }
}  // namespace bustub
