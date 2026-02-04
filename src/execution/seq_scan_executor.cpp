//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"
#include "optimizer/optimizer_internal.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
  : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  table_iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
  is_finished_ = false;
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {

  tuple_batch->clear();
  rid_batch->clear();
        
  if (is_finished_ || IsPredicateFalse(plan_->filter_predicate_)) {
    return false;
  }
  size_t batch_idx = 0;
  while (batch_idx < batch_size) {
    if (table_iterator_->IsEnd()) {
      is_finished_ = true;
      return !tuple_batch->empty();
    }
    auto [tuple_meta, tuple] = table_iterator_->GetTuple();
    if (tuple_meta.is_deleted_) {
      ++(*table_iterator_);
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*table_iterator_);
        continue;
      }
    }
    tuple_batch->push_back(std::move(tuple));
    rid_batch->push_back(table_iterator_->GetRID());
    ++(*table_iterator_);
    ++batch_idx;
  }
  return true;
}

}  // namespace bustub
