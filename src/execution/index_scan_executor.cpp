//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <memory>
#include "storage/index/b_plus_tree_index.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
    index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(index_info_->table_name_);
    tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
    index_iterator_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(tree_->GetBeginIterator());
 }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (index_iterator_->IsEnd()) {
        return false;
    }
    *rid = (**index_iterator_).second;

    ++(*index_iterator_);
    Tuple raw_tuple;
    table_info_->table_->GetTuple(*rid, &raw_tuple, GetExecutorContext()->GetTransaction());

    std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());
    for (size_t i = 0; i < values.capacity(); i++) {
        values.push_back(raw_tuple.GetValue(&table_info_->schema_, i));
    }
    *tuple = Tuple(values, &GetOutputSchema());
    return true;
 }

}  // namespace bustub
