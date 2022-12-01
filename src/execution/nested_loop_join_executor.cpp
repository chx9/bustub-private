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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID left_rid;
  RID right_rid;
  Tuple left_tuple;
  Tuple right_tuple;
  if (!left_executor_->Next(&left_tuple, &left_rid)) {
    return false;
  }
  if (!right_executor_->Next(&right_tuple, &right_rid)) {
    return false;
  }

  auto value = plan_->Predicate().EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                               right_executor_->GetOutputSchema());
  std::vector<Value> values;
  for(size_t i=0;i<left_tuple.GetLength();i++){
    values.emplace_back(left_tuple.GetValue(const Schema *schema, uint32_t column_idx))
  }
  // left_tuple.GetValue((const Schema *schema), uint32_t column_idx) 
  return true;
}

}  // namespace bustub
