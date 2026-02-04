//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "optimizer/optimizer_internal.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_batch_.clear();
  right_tuple_batch_.clear();
  left_index_ = 0;
  right_index_ = 0;
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  if (plan_->GetJoinType() == JoinType::INVALID) {
    return false;
  }

  tuple_batch->clear();
  rid_batch->clear();

  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();

  while (tuple_batch->size() < batch_size) {
    
    if (left_tuple_batch_.empty() || left_index_ == left_tuple_batch_.size()) {
      left_tuple_batch_.clear();
      if (!left_executor_->Next(&left_tuple_batch_, rid_batch, batch_size)) {
        return !tuple_batch->empty();
      }
      left_index_ = 0;

      right_executor_->Init();
      right_tuple_batch_.clear();
      right_index_ = 0;
    }

    if (right_tuple_batch_.empty() || right_index_ == right_tuple_batch_.size()) {
      right_tuple_batch_.clear();
      if (!right_executor_->Next(&right_tuple_batch_, rid_batch, batch_size)) {
        right_executor_->Init();
        right_tuple_batch_.clear();
        right_index_ = 0;

        if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
          std::vector<Value> values;
          auto left_count = left_schema.GetColumnCount();
          auto right_count = right_schema.GetColumnCount();
          values.reserve(left_count + right_count);
          auto &left_tuple = left_tuple_batch_[left_index_];
          for (size_t i = 0; i < left_count; ++i) {
            values.push_back(left_tuple.GetValue(&left_schema, i));
          }
          for (size_t i = 0; i < right_count; ++i) {
            values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
          }
          tuple_batch->emplace_back(values, &plan_->OutputSchema());
          rid_batch->emplace_back();  // RID is not used in nested loop join
        }
        left_matched_ = false;
        ++left_index_;
        continue;
      }
      right_index_ = 0;
    }

    auto &left_tuple = left_tuple_batch_[left_index_];
    auto &right_tuple = right_tuple_batch_[right_index_];

    auto match = plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema,
                                                  &right_tuple, right_schema);

    if (!match.IsNull() && match.GetAs<bool>()) {
      left_matched_ = true;
      std::vector<Value> values;
      auto left_count = left_schema.GetColumnCount();
      auto right_count = right_schema.GetColumnCount();
      values.reserve(left_count + right_count);
      for (size_t i = 0; i < left_count; ++i) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (size_t i = 0; i < right_count; ++i) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }
      tuple_batch->emplace_back(values, &plan_->OutputSchema());
      rid_batch->emplace_back();  // RID is not used in nested loop join
    }

    ++right_index_;
  }

  return true;
}


}  // namespace bustub
