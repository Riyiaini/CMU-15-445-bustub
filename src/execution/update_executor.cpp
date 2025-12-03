//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
}

/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) {
    return false;
  }
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  std::vector<Tuple> tuples;

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  int32_t total_updated = 0;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    
    for (size_t i = 0; i < child_tuples.size(); ++i) {
      auto tuple = child_tuples[i];
      auto rid = child_rids[i];
      auto tuple_meta = table_info_->table_->GetTupleMeta(rid);
      tuple_meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(tuple_meta, rid);

      for (const auto &index : indexes) {
        index->index_->DeleteEntry(
          tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          rid, exec_ctx_->GetTransaction());
      }

      std::vector<Value> values;
      for (auto &expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&tuple, table_info_->schema_));
      }
      tuples.emplace_back(values, &table_info_->schema_);
      total_updated++;
    }
    
    child_tuples.clear();
    child_rids.clear();
  }

  // To avoid violating unique index constraints, first delete all old entries, then insert new entries
  for (auto &tuple : tuples) {
    auto rid_opt = table_info_->table_->InsertTuple({0, false}, tuple, exec_ctx_->GetLockManager(),
                                                    exec_ctx_->GetTransaction(), table_info_->oid_);
    for (const auto &index : indexes) {
      index->index_->InsertEntry(
        tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
        rid_opt.value(), exec_ctx_->GetTransaction());
    }
  }

  tuple_batch->clear();
  rid_batch->clear();

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, total_updated);
  tuple_batch->emplace_back(values, &GetOutputSchema());

  is_finished_ = true;
  return true;
}

}  // namespace bustub
