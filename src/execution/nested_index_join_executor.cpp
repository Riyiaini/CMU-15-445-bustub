//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), 
      child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()).get();
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid()).get();
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();
  left_tuple_batch_.clear();
  left_index_ = 0;
}

auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {

  if (plan_->GetJoinType() == JoinType::INVALID) {
    return false;
  }

  tuple_batch->clear();
  rid_batch->clear();
  
  while (tuple_batch->size() < batch_size) {
    if (left_tuple_batch_.empty() || left_index_ == left_tuple_batch_.size()) {
      left_tuple_batch_.clear();
      if (!child_executor_->Next(&left_tuple_batch_, rid_batch, batch_size)) {
        return !tuple_batch->empty();
      }
      left_index_ = 0;
    }

    auto &left_tuple = left_tuple_batch_[left_index_];
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());

    auto key = Tuple(std::vector<Value>{value}, &index_info_->key_schema_);
    std::vector<RID> result_rids;
    index_info_->index_->ScanKey(key, &result_rids, exec_ctx_->GetTransaction());

    if (result_rids.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        auto left_count = child_executor_->GetOutputSchema().GetColumnCount();
        auto right_count = plan_->inner_table_schema_->GetColumnCount();
        for (uint32_t i = 0; i < left_count; ++i) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_count; ++i) {
          values.push_back(ValueFactory::GetNullValueByType(plan_->inner_table_schema_->GetColumn(i).GetType()));
        }
        tuple_batch->emplace_back(values, &plan_->OutputSchema());
        rid_batch->emplace_back();  // RID is not used in nested index join
      }
    } else {
      for (const auto &rid : result_rids) {
        auto [_, right_tuple] = inner_table_info_->table_->GetTuple(rid);
        std::vector<Value> values;
        auto left_count = child_executor_->GetOutputSchema().GetColumnCount();
        auto right_count = plan_->inner_table_schema_->GetColumnCount();
        for (uint32_t i = 0; i < left_count; ++i) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_count; ++i) {
          values.push_back(right_tuple.GetValue(plan_->inner_table_schema_.get(), i));
        }
        tuple_batch->emplace_back(values, &plan_->OutputSchema());
        rid_batch->emplace_back();  // RID is not used in nested index join
      }
    }

    ++left_index_;
  }

  return true;
}

}  // namespace bustub
