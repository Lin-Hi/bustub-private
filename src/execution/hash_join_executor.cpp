//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)),
      now_id_(0) {}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  Tuple left_tuple;
  RID left_rid;

  while (left_child_executor_->Next(&left_tuple, &left_rid)) {
    HashJoinKey dis_key;
    dis_key.key_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_child_executor_->GetOutputSchema());
    if (map_.count(dis_key) != 0) {
      map_[dis_key].emplace_back(left_tuple);
    } else {
      map_[dis_key] = std::vector{left_tuple};
    }
  }

  Tuple right_tuple;
  RID right_rid;
  while (right_child_executor_->Next(&right_tuple, &right_rid)) {
    HashJoinKey dis_key;
    dis_key.key_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_executor_->GetOutputSchema());
    if (map_.count(dis_key) != 0) {
      for (auto &cur_left_tuple : map_.find(dis_key)->second) {
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&cur_left_tuple, left_child_executor_->GetOutputSchema(),
                                                       &right_tuple, right_child_executor_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(output, GetOutputSchema()));
      }
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (now_id_ < result_.size()) {
    *tuple = result_[now_id_];
    *rid = tuple->GetRid();
    now_id_++;
    return true;
  }
  return false;
}

}  // namespace bustub
