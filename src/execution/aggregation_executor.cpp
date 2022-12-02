//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <vector>

#include "common/logger.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aht_.Clear();
  child_->Init();
  Tuple raw_tuple;
  RID rid;
  int insert_cnt = 0;
  while (child_->Next(&raw_tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&raw_tuple), MakeAggregateValue(&raw_tuple));
    insert_cnt++;
  }
  if (insert_cnt == 0 && plan_->GetGroupBys().empty()) {
    aht_.InitEmpty(MakeAggregateKey(&raw_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> group_bys;
  std::vector<Value> aggregates;
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  group_bys = aht_iterator_.Key().group_bys_;
  aggregates = aht_iterator_.Val().aggregates_;
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());
  for (auto &group_by : group_bys) {
    values.emplace_back(group_by);
  }
  for (auto &aggregate : aggregates) {
    values.emplace_back(aggregate);
  }
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
