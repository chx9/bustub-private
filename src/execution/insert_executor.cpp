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

#include "common/logger.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
 table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
 index_infos_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
 child_executor_->Init();
 access_ = true;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
 Tuple child_tuple{};
 int i = 0;
 while (child_executor_->Next(&child_tuple, rid)) {
  table_info_->table_->InsertTuple(child_tuple, rid, GetExecutorContext()->GetTransaction());
  for (auto &index_info : index_infos_) {
   index_info->index_->InsertEntry(
     child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
     *rid, GetExecutorContext()->GetTransaction());
  }
  i++;
 }

 if (access_) {
  access_ = false;
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(INTEGER, i);
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
 }
 return false;
}

}  // namespace bustub
