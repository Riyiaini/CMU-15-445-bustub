//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
      if (!plan->window_functions_.empty()) {
        // assum that all window functions have the same order by clause
        auto order_by = plan->window_functions_.begin()->second.order_by_;
        if (!order_by.empty()) {
          auto child_schema = SchemaRef(&child_executor->GetOutputSchema(), [](const Schema *) {});
          auto sort_plan = std::make_shared<SortPlanNode>(child_schema, plan->GetChildAt(0), order_by);
          plan_ = dynamic_cast<const WindowFunctionPlanNode *>(plan_->CloneWithChildren({sort_plan}).get());
          child_executor_ = std::make_unique<ExternalMergeSortExecutor<2>>(exec_ctx_, sort_plan.get(), std::move(child_executor)); 
          is_ordered_ = true;
          return;
        }
      }
      child_executor_ = std::move(child_executor);
    }

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {  
  child_executor_->Init();

  for (const auto &[index, wf] : plan_->window_functions_) {
    auto ht = SimpleWindowFunctionHashTable(wf.type_);
    ht.Init();
    hts_.emplace(index, std::move(ht));
  }

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  if (!is_ordered_) {
    while (child_executor_->Next(&child_tuples, &child_rids, BUSTUB_BATCH_SIZE)) {
      for (const auto &tuple : child_tuples) {
        for (const auto &[index, wf] : plan_->window_functions_) {
          auto agg_key = GetAggregateKey(wf.partition_by_, &tuple);
          auto agg_val = wf.function_->Evaluate(&tuple, child_executor_->GetOutputSchema());
          hts_.at(index).InsertCombine(agg_key, agg_val);
        }
      }
    }
  }
}

/**
 * Yield the next tuple batch from the window aggregation.
 * @param[out] tuple_batch The next tuple batch produced by the window aggregation
 * @param[out] rid_batch The next tuple RID batch produced by the window aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  std::vector<Value> values;

  if (!child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    return false;
  }

  for (const auto &tuple : child_tuples) {

    for (const auto &col : plan_->columns_) {
      const auto *col_expr = reinterpret_cast<const ColumnValueExpression *>(col.get());
      if (col_expr != nullptr && col_expr->GetColIdx() > 0) {
        values.push_back(col_expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
      } else {
        uint32_t index = values.size();
        const auto &wf_plan = plan_->window_functions_.at(index);
        const auto &partition_by = wf_plan.partition_by_;
        auto agg_key = GetAggregateKey(partition_by, &tuple);
        if (!is_ordered_) {
          values.push_back(hts_.at(index).GetAggregateValue(agg_key));
        } else {
          auto agg_val = wf_plan.function_->Evaluate(&tuple, child_executor_->GetOutputSchema());
          values.push_back(hts_.at(index).InsertCombine(agg_key, agg_val));
        }
      }
    }

    tuple_batch->emplace_back(values, &plan_->OutputSchema());
    values.clear();
  }

  return true;
}
}  // namespace bustub
