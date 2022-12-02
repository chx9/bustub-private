//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstddef>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/logger.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info_->name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(inner_index_info_->index_.get());
  index_iterator_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(tree_->GetBeginIterator());
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  Tuple left_tuple;
  Tuple right_raw_tuple;
  RID left_rid;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    Tuple prob_key = Tuple{{value}, inner_index_info_->index_->GetKeySchema()};
    std::vector<RID> result_set;
    tree_->ScanKey(prob_key, &result_set, exec_ctx_->GetTransaction());

    if (plan_->join_type_ == JoinType::LEFT && result_set.empty()) {
      std::vector<Value> values;
      for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < inner_table_info_->schema_.GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(inner_table_info_->schema_.GetColumn(i).GetType()));
      }
      results_.emplace_back(values, &GetOutputSchema());
    }
    for (auto rid : result_set) {
      inner_table_info_->table_->GetTuple(rid, &right_raw_tuple, exec_ctx_->GetTransaction());
      std::vector<Value> values;

      for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (size_t i = 0; i < inner_table_info_->schema_.GetColumnCount(); i++) {
        values.emplace_back(right_raw_tuple.GetValue(&inner_table_info_->schema_, i));
      }
      results_.emplace_back(values, &GetOutputSchema());
    }
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < results_.size()) {
    *tuple = results_[index_];
    index_++;
    return true;
  }
  return false;
}
}  // namespace bustub
