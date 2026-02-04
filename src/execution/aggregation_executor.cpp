//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
      aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()}, aht_iterator_(aht_.Begin()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  child_executor_->Init();
  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;
  aht_.Clear();
  bool is_empty = true;
  while (child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuple_batch) {  
      is_empty = false;
      aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }
  }
  if (is_empty && plan_->GetGroupBys().empty()) {
    aht_.Init();
  }
  aht_iterator_ = aht_.Begin();
}

/**
 * Yield the next tuple batch from the aggregation.
 * @param[out] tuple_batch The next batch of tuples produced by the aggregation
 * @param[out] rid_batch The next batch of tuple RIDs produced by the aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if any tuples were produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  while (aht_iterator_ != aht_.End() && tuple_batch->size() < batch_size) {
    std::vector<Value> values;
    values.reserve(plan_->GetGroupBys().size() + plan_->GetAggregates().size());
    // Add group by keys
    for (const auto &key : aht_iterator_.Key().group_bys_) {
      values.push_back(key);
    }
    // Add aggregate values
    for (const auto &val : aht_iterator_.Val().aggregates_) {
      values.push_back(val);
    }
    tuple_batch->emplace_back(values, &plan_->OutputSchema());
    rid_batch->emplace_back();  // RID is not used in aggregation
    ++aht_iterator_;
  }
  return !tuple_batch->empty();
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
