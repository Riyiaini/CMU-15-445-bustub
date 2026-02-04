//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
      table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished) {
    return false;
  }

  tuple_batch->clear();
  rid_batch->clear();

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  int32_t total_inserted = 0;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    
    for (auto &tuple : child_tuples) {
      auto rid_opt = table_info_->table_->InsertTuple({0, false}, tuple, exec_ctx_->GetLockManager(), 
                                      exec_ctx_->GetTransaction(), table_info_->oid_);
      for (const auto &index : indexes) {
        index->index_->InsertEntry(
          tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
          rid_opt.value(), exec_ctx_->GetTransaction());
      }
      total_inserted++;
    }
    
    child_tuples.clear();
    child_rids.clear();
  }

  tuple_batch->clear();
  rid_batch->clear();

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, total_inserted);
  tuple_batch->emplace_back(values, &GetOutputSchema());
  rid_batch->emplace_back();  // RID is not used in insert

  is_finished = true;
  return true;
}

}  // namespace bustub
