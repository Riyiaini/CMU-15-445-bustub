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
        auto order_bys = (*plan->window_functions_.begin()).second.order_by_;
        if (!order_bys.empty()) {
          auto child_schema = SchemaRef(&child_executor->GetOutputSchema(), [](const Schema *) {});
          auto sort_plan = SortPlanNode(child_schema, nullptr, order_bys);                            
          child_executor_ = std::make_unique<ExternalMergeSortExecutor<2>>(exec_ctx_, &sort_plan, std::move(child_executor)); 
          return;
        }
      }
      child_executor_ = std::move(child_executor);
    }

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {  
  child_executor_->Init();
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

  /* auto GetAggregateKey = [&](const std::vector<AbstractExpressionRef> &partition_by, const Tuple *tuple) -> AggregateKey {
    std::vector<Value> values;
    for (const auto &expr : partition_by) {
      auto res = expr->Evaluate(tuple, child_executor_->GetOutputSchema());
      values.push_back(res);
    }
    return AggregateKey{values};
  }; */

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  // std::vector<Value> values;

  if (!child_executor_->Next(&child_tuples, &child_rids, BUSTUB_BATCH_SIZE)) {
    return false;
  }

  /* for (const auto &tuple : child_tuples) {
    for (const auto &col : plan_->columns_) {
      const auto *col_expr = reinterpret_cast<const ColumnValueExpression *>(col.get());
      if (col_expr != nullptr) {
        values.push_back(col_expr->Evaluate(nullptr, child_executor_->GetOutputSchema()));
      } else {
        values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }

    for (const auto &[index, wf_plan] : plan_->window_functions_) {
      auto &partition_by = wf_plan.partition_by_;
      auto &func = wf_plan.function_;
      while (child_executor_->Next(&child_tuples, &child_rids, BUSTUB_BATCH_SIZE)) {
        for (const auto &tuple : child_tuples) {
          auto agg_key = GetAggregateKey(partition_by, &tuple);
          auto val = func->Evaluate(&tuple, child_executor_->GetOutputSchema());

        }
      }
    }
  } */

  return true;
}
}  // namespace bustub
