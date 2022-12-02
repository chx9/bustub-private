//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/table/tuple.h"
#include "type/limits.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  RID left_rid;
  RID right_rid;
  Tuple left_tuple;
  Tuple right_tuple;
  bool matched = false;
  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    matched = false;
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = plan_->Predicate().EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        matched = true;
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        Tuple raw_tuple{values, &GetOutputSchema()};
        results_.emplace_back(raw_tuple);
      }
    }
    if (plan_->join_type_ == JoinType::LEFT && !matched) {
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (size_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }

      for (size_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      Tuple raw_tuple{values, &GetOutputSchema()};
      results_.emplace_back(raw_tuple);
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < results_.size()) {
    *tuple = results_[index_];
    index_++;

    return true;
  }
  return false;
}

}  // namespace bustub
