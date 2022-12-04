#include "execution/executors/topn_executor.h"
#include <cstddef>
#include "common/logger.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  if (!is_inited_) {
    child_executor_->Init();
    auto cmp = [this](const Tuple &lhs, const Tuple &rhs) {
      for (auto &order_by : this->plan_->GetOrderBy()) {
        auto left_val = order_by.second->Evaluate(&lhs, this->child_executor_->GetOutputSchema());
        auto right_val = order_by.second->Evaluate(&rhs, this->child_executor_->GetOutputSchema());
        if (left_val.CompareEquals(right_val) == CmpBool::CmpTrue) {
          continue;
        }
        if (order_by.first == OrderByType::DESC) {
          return left_val.CompareLessThan(right_val) == CmpBool::CmpTrue;
        }
        return left_val.CompareGreaterThan(right_val) == CmpBool::CmpTrue;
      }
      return true;
    };

    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> q(cmp);
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      q.push(tuple);
    }
    size_t n = plan_->GetN();
    while (n-- != 0 && !q.empty()) {
      tuples_.emplace_back(q.top());
      q.pop();
    }
    it_ = tuples_.begin();
    is_inited_ = true;
    return;
  }
  it_ = tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = it_->GetRid();
  it_++;
  return true;
}

}  // namespace bustub
