#include "execution/executors/sort_executor.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  if (!is_inited_) {
    child_executor_->Init();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      tuples_.emplace_back(tuple);
    }
    std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &lhs, const Tuple &rhs) {
      for (auto &order_by : this->plan_->GetOrderBy()) {
        auto left_val = order_by.second->Evaluate(&lhs, this->child_executor_->GetOutputSchema());
        auto right_val = order_by.second->Evaluate(&rhs, this->child_executor_->GetOutputSchema());
        if (left_val.CompareEquals(right_val) == CmpBool::CmpTrue) {
          continue;
        }
        if (order_by.first == OrderByType::DESC) {
          return left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue;
        }
        return left_val.CompareLessThan(right_val) == CmpBool::CmpTrue;
      }
      return true;
    });
    it_ = tuples_.begin();
    is_inited_ = true;
    return;
  }
  it_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = it_->GetRid();
  it_++;
  return true;
}

}  // namespace bustub
