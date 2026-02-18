//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), 
      plan_(plan), 
      cmp_(plan->GetOrderBy()), 
      child_executor_(std::move(child_executor)),
      min_heap_(HeapComp(&cmp_)){
}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  auto buffer_pool_size = exec_ctx_->GetBufferPoolManager()->Size() * BUSTUB_PAGE_SIZE;

  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;

  std::vector<SortEntry> entries;
  size_t entries_size = 0;

  runs_.clear();
  iterators_.clear();

  child_executor_->Init();

  while (child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    entries.reserve(entries.size() + tuple_batch.size());

    for (auto &tuple : tuple_batch) {
      auto [key, key_size] = GenerateSortKeyAndSize(tuple, plan_->GetOrderBy(), plan_->OutputSchema());
      // Compute the size increase after inserting this entry
      size_t delta = sizeof(SortEntry) + tuple.GetLength() + key_size + key.capacity() * sizeof(Value);
      entries_size += delta;
      if (entries_size > buffer_pool_size * 0.8) {
        std::vector<page_id_t> pages;
        InsertEntries(entries, pages);
        /* std::cout << "entries size / buffer pool size: " << entries_size << " / " << buffer_pool_size << ", page size :" << pages.size() << std::endl;
        std::cout << "key size: " << key_size << ", tuple len: " << tuple.GetLength() << "key cap: " << key.capacity() << std::endl; */
        runs_.emplace_back(std::move(pages), exec_ctx_->GetBufferPoolManager());
        entries.clear();
        entries_size = delta;
      }
      entries.emplace_back(SortEntry{std::move(key), std::move(tuple)});
    }
    tuple_batch.clear();
    rid_batch.clear();
  }

  if (!entries.empty()) {
    std::vector<page_id_t> pages;
    InsertEntries(entries, pages);
    runs_.emplace_back(std::move(pages), exec_ctx_->GetBufferPoolManager());
    entries.clear();
  }

  size_t k = std::max(K, size_t(2));

  while (runs_.size() > k) {
    std::vector<MergeSortRun> new_runs;
    new_runs.reserve((runs_.size() + k - 1) / k);
    for (size_t i = 0; i < runs_.size(); i+=k) {
      auto end = std::min(i + k, runs_.size());
      if (end - i == 1) {
        new_runs.emplace_back(std::move(runs_[i]));
        break;
      }
      std::vector<page_id_t> new_pages;
      MergeKRuns(i, end, new_pages);
      new_runs.emplace_back(std::move(new_pages), exec_ctx_->GetBufferPoolManager());
    }
    runs_ = std::move(new_runs);
  }

  is_finished_ = false;
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge sort.
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (is_finished_) {
    return false;
  }

  if (runs_.size() == 1) {
    if (iterators_.empty()) {
      iterators_.push_back(runs_[0].Begin());
    }
    auto &it = iterators_[0];
    while (tuple_batch->size() < batch_size) {
      auto tuple = it.GetAndIncrement();
      if (tuple.GetLength() == 0) {
        is_finished_ = true;
        return !tuple_batch->empty();
      }
      tuple_batch->emplace_back(tuple);
      rid_batch->emplace_back(tuple.GetRid());
    }
    return true;
  }

  if (iterators_.empty()) {
    for (size_t idx = 0; idx < runs_.size(); ++idx) {
      auto it = runs_[idx].Begin();
      auto tuple = it.GetAndIncrement();
      auto entry = std::make_pair(
        GenerateSortKey(tuple, plan_->GetOrderBy(), plan_->OutputSchema()), std::move(tuple));
      min_heap_.push(HeapElement(entry, idx));
      iterators_.emplace_back(std::move(it));
    }
  }
  while (tuple_batch->size() < batch_size && !min_heap_.empty()) {
    const HeapElement &top = min_heap_.top();
    const auto &tuple = tuple_batch->emplace_back(top.entry_.second);
    rid_batch->emplace_back(tuple.GetRid());
    auto run_idx = top.run_idx_;
    auto next_tuple = iterators_[run_idx].GetAndIncrement();
    min_heap_.pop();
    if (next_tuple.GetLength() == 0) {
      runs_[run_idx].CleanPages();
      continue;
    }
    auto entry = std::make_pair(
      GenerateSortKey(next_tuple, plan_->GetOrderBy(), plan_->OutputSchema()), std::move(next_tuple));
    min_heap_.push(HeapElement(entry, run_idx));
  }
  if (min_heap_.empty()) {
    is_finished_ = true;
  }
  return !tuple_batch->empty();
}

template <size_t K>
void ExternalMergeSortExecutor<K>::InsertEntries(std::vector<SortEntry> &entries, std::vector<page_id_t> &pages) {
  std::sort(entries.begin(), entries.end(), cmp_);
  auto *bpm = exec_ctx_->GetBufferPoolManager();

  pages.clear();
  auto new_page_id = bpm->NewPage();
  pages.push_back(new_page_id);

  auto write_guard = bpm->WritePage(new_page_id);
  auto intermediate_page = write_guard.AsMut<IntermediateResultPage>();

  for (const auto &[key, tuple] : entries) {

    if (!intermediate_page->AppendTuple(tuple)) {
      BUSTUB_ASSERT(intermediate_page->GetSize() != 0, "Tuple size should not exceed page size");
      write_guard.Drop();
      
      auto new_page_id = bpm->NewPage();
      pages.push_back(new_page_id);
      
      write_guard = bpm->WritePage(new_page_id);
      intermediate_page = write_guard.AsMut<IntermediateResultPage>();
      intermediate_page->AppendTuple(tuple);
    }
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::MergeKRuns(size_t start_idx, size_t end_idx, std::vector<page_id_t> &merged_pages) {
  std::vector<MergeSortRun::Iterator> iterators;
  std::priority_queue<HeapElement, std::vector<HeapElement>, HeapComp> min_heap{HeapComp(&cmp_)};

  for (size_t idx = start_idx; idx < end_idx; ++idx) {
    auto it = runs_[idx].Begin();
    auto tuple = it.GetAndIncrement();
    auto entry = std::make_pair(
      GenerateSortKey(tuple, plan_->GetOrderBy(), GetOutputSchema()), tuple);
    min_heap.push(HeapElement(entry, idx));
    iterators.emplace_back(std::move(it));
  }

  std::vector<SortEntry> entries;
  auto bpm = exec_ctx_->GetBufferPoolManager();
  auto new_page_id = bpm->NewPage();
  merged_pages.push_back(new_page_id);

  auto write_guard = bpm->WritePage(new_page_id);
  auto intermediate_page = write_guard.AsMut<IntermediateResultPage>();

  while (!min_heap.empty()) {
    const HeapElement &top = min_heap.top();
    const auto &tuple = top.entry_.second;
    auto run_idx = top.run_idx_;
    if (!intermediate_page->AppendTuple(tuple)) {
      BUSTUB_ASSERT(intermediate_page->GetSize() != 0, "Tuple size should not exceed page size");
      write_guard.Drop();
      
      auto new_page_id = bpm->NewPage();
      merged_pages.push_back(new_page_id);
      
      write_guard = bpm->WritePage(new_page_id);
      intermediate_page = write_guard.AsMut<IntermediateResultPage>();
      intermediate_page->AppendTuple(tuple);
    }
    min_heap.pop();
    auto next_tuple = iterators[run_idx - start_idx].GetAndIncrement();
    if (next_tuple.GetLength() == 0) {
      runs_[run_idx].CleanPages();
      continue;
    }
    auto entry = std::make_pair(
      GenerateSortKey(next_tuple, plan_->GetOrderBy(), plan_->OutputSchema()), std::move(next_tuple));
    min_heap.push(HeapElement(entry, run_idx));
  }
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
