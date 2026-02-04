//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> &&pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  void CleanPages() {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
  }

  MergeSortRun(const MergeSortRun&) = delete;
  MergeSortRun& operator=(const MergeSortRun&) = delete;
  MergeSortRun(MergeSortRun &&) noexcept = default;
  MergeSortRun& operator=(MergeSortRun &&) noexcept = default;
 
  auto GetPageCount() const -> size_t { return pages_.size(); }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      while (current_page_idx_ < run_->pages_.size()) {
        auto read_guard = run_->bpm_->ReadPage(run_->pages_[current_page_idx_]);
        auto page = read_guard.As<IntermediateResultPage>();
        auto tuple_opt = page->GetTupleAtOffset(current_offset_);
        BUSTUB_ASSERT(tuple_opt.has_value(), "Page invariant violated during iteration");

        size_t advance = sizeof(RID) + sizeof(uint32_t) + tuple_opt->GetLength();
        current_offset_ += advance;
        if (current_offset_ >= page->GetSize()) {
          current_page_idx_++;
          current_offset_ = 0;
        }
        break;
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() -> Tuple {
      if (current_page_idx_ >= run_->pages_.size()) {
        return Tuple::Empty();
      }
      auto read_gaurd = run_->bpm_->ReadPage(run_->pages_[current_page_idx_]);
      auto intermediate_page = read_gaurd.As<IntermediateResultPage>();
      auto tuple_opt = intermediate_page->GetTupleAtOffset(current_offset_);
      return tuple_opt.value_or(Tuple::Empty());
    }

    auto GetAndIncrement() -> Tuple {
      while (current_page_idx_ < run_->pages_.size()) {
        auto read_guard = run_->bpm_->ReadPage(run_->pages_[current_page_idx_]);
        auto page = read_guard.As<IntermediateResultPage>();
        auto tuple_opt = page->GetTupleAtOffset(current_offset_);
        BUSTUB_ASSERT(tuple_opt.has_value(), "Page invariant violated during iteration");

        Tuple res = std::move(*tuple_opt);
        size_t advance = sizeof(RID) + sizeof(uint32_t) + res.GetLength();
        
        current_offset_ += advance;
        if (current_offset_ >= page->GetSize()) {
          current_page_idx_++;
          current_offset_ = 0;
        }
        return res;
      }
      return Tuple::Empty();
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      return this->current_page_idx_ == other.current_page_idx_ && this->current_offset_ == other.current_offset_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool {
      return this->current_page_idx_ != other.current_page_idx_ || this->current_offset_ != other.current_offset_;
    }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {
      Validate();
    }

    explicit Iterator(const MergeSortRun *run, size_t current_page_idx, size_t current_offset)
        : run_(run), current_page_idx_(current_page_idx), current_offset_(current_offset) {
      Validate();
    }

    void Validate() {
      while (current_page_idx_ < run_->pages_.size()) {
        auto read_guard = run_->bpm_->ReadPage(run_->pages_[current_page_idx_]);
        auto intermediate_page = read_guard.As<IntermediateResultPage>();
        auto tuple_opt = intermediate_page->GetTupleAtOffset(current_offset_);
        if (tuple_opt.has_value()) {
          return;
        }
        current_page_idx_++;
        current_offset_ = 0;
      }
    }

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_;

    size_t current_page_idx_{0};
    size_t current_offset_{0};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() -> Iterator { return Iterator(this); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() -> Iterator { return Iterator(this, pages_.size(), 0); }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:

  void InsertEntries(std::vector<SortEntry> &entries, std::vector<page_id_t> &pages);

  void MergeKRuns(size_t start_idx, size_t end_idx, std::vector<page_id_t> &merged_pages);

  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO(P3): You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<MergeSortRun> runs_;

  std::vector<MergeSortRun::Iterator> iterators_;

  struct HeapElement {
    size_t run_idx_;
    SortEntry entry_;
    HeapElement(SortEntry entry, size_t run_idx) : run_idx_(run_idx), entry_(std::move(entry)) {}
  };

  struct HeapComp {
    const TupleComparator *cmp_;
    explicit HeapComp(const TupleComparator *c) : cmp_(c) {}
    bool operator()(const HeapElement &a, const HeapElement &b) const {
      return (*cmp_)(b.entry_, a.entry_);
    }
  };

  std::priority_queue<HeapElement, std::vector<HeapElement>, HeapComp> min_heap_;

  bool is_finished_{false};
};

}  // namespace bustub
