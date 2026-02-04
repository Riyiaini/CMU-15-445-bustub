//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "optimizer/optimizer_internal.h"
#include "common/macros.h"

namespace bustub {

auto TupleFromPrefixExprs(const std::vector<AbstractExpressionRef> &exprs, const IndexInfo *index, bool get_min=true) -> Tuple {
  std::vector<Value> values;
  for (size_t i = 0; i < index->index_->GetKeyAttrs().size(); ++i) {
    if (i < exprs.size() && exprs[i] != nullptr) {
      values.push_back(exprs[i]->Evaluate(nullptr, index->key_schema_));
    } else {
      values.push_back(get_min ? 
        Type::GetMinValue(index->key_schema_.GetColumn(i).GetType()) :
        Type::GetMaxValue(index->key_schema_.GetColumn(i).GetType()));
    }
  }
  return Tuple(values, &index->key_schema_);
}

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get();
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()).get();
  index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

void IndexScanExecutor::Init() {
  is_finished_ = false;
  // case 1: no filter predicate
  if (plan_->filter_predicate_ == nullptr) {
    index_iterator_ = index_->GetBeginIterator();
    iterator_end_ = index_->GetEndIterator();
    return;
  }
  // case 2: point lookup
  if (plan_->is_point_lookup_) {
    if (plan_->pred_keys_.empty()) {
      is_finished_ = true;
      return;
    }
    point_lookup_tuples_.clear();
    point_lookup_tuples_.reserve(plan_->pred_keys_.size());
    for (const auto &const_expr : plan_->pred_keys_) {
      point_lookup_tuples_.push_back(TupleFromPrefixExprs(const_expr, index_info_));
    }
  } 
  // case 3: range scan
  else {
    auto start_tuple = TupleFromPrefixExprs(plan_->pred_keys_[0], index_info_, true);
    auto end_tuple = TupleFromPrefixExprs(plan_->pred_keys_[1], index_info_, false);
    IntegerKeyType_BTree start_key, end_key;
    start_key.SetFromKey(start_tuple);
    end_key.SetFromKey(end_tuple);
    index_iterator_ = index_->GetBeginIterator(start_key);
    iterator_end_ = index_->GetBeginIterator(end_key);
    ++iterator_end_; 
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (is_finished_ || IsPredicateFalse(plan_->filter_predicate_)) {
    return false;
  }

  auto match_predicate = [&](const Tuple &tuple, std::vector<AbstractExpressionRef> preds = {}) -> bool {
    if (preds.empty()) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {
        return false;
      }
      return true;
    }
    for (const auto &pred : preds) {
      auto value = pred->Evaluate(&tuple, table_info_->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {
        return false;
      }
    }
    return true;
  };

  // case 1: point lookup
  if (plan_->is_point_lookup_) {
    auto iter = point_lookup_tuples_.begin();
    while (iter != point_lookup_tuples_.end() && tuple_batch->size() < batch_size) {
      Tuple cur_tuple = *iter;
      std::vector<RID> rids;
      index_->ScanKey(cur_tuple, &rids, exec_ctx_->GetTransaction());
      if (!rids.empty()) {
        auto [_, tuple] = table_info_->table_->GetTuple(rids[0]);
        if (match_predicate(tuple)) {
          tuple_batch->push_back(tuple);
          rid_batch->push_back(rids[0]);
        }
      }
      iter = point_lookup_tuples_.erase(iter);
    }
    if (!point_lookup_tuples_.empty()) {
      return true;
    } else {
      is_finished_ = true;
      return !tuple_batch->empty();
    }
  }

  // case 2: range scan or full index scan
  while (index_iterator_ != iterator_end_ && tuple_batch->size() < batch_size) {
    auto [cur_key, cur_rid] = *index_iterator_;
    auto [tuple_meta, tuple] = table_info_->table_->GetTuple(cur_rid);
    if (plan_->remaining_preds_.empty() || 
        (!plan_->remaining_preds_.empty() &&
         match_predicate(tuple, plan_->remaining_preds_))) {
      tuple_batch->push_back(tuple);
      rid_batch->push_back(cur_rid);
    }
    ++index_iterator_;
  }

  if (index_iterator_ == iterator_end_) {
    is_finished_ = true;
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
