//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

class DiskPartition {
 public:
  DiskPartition() = default;
  DiskPartition(BufferPoolManager *bpm, size_t max_page_num = 0) : bpm_(bpm), max_page_num_(max_page_num) {
    if (max_page_num_ == 0) {
      // fit the partition into a in memory hash table
      max_page_num_ = bpm_->Size() / 2;
    }
  }

  DiskPartition(const DiskPartition&) = delete;
  DiskPartition& operator=(const DiskPartition&) = delete;
  DiskPartition(DiskPartition &&) noexcept = default;
  DiskPartition& operator=(DiskPartition &&) noexcept = default;

  ~DiskPartition() {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
  }

  void Reset() {
    for (auto page_id : pages_) {
      bpm_->DeletePage(page_id);
    }
    pages_.clear();
  }

  void InsertTuple(const Tuple &tuple) {
    if (pages_.empty()) {
      auto new_page_id = bpm_->NewPage();
      pages_.push_back(new_page_id);
    }
    if (pages_.size() > max_page_num_) {
      throw std::runtime_error("Exceeded maximum number of pages for partition");
    }
    auto write_guard = bpm_->WritePage(static_cast<page_id_t>(pages_.back()));
    auto intermediate_page = write_guard.AsMut<IntermediateResultPage>();

    if (!intermediate_page->AppendTuple(tuple)) {
      write_guard.Drop();
      auto new_page_id = bpm_->NewPage();
      pages_.push_back(new_page_id);
      if (pages_.size() > max_page_num_) {
        throw std::runtime_error("Exceeded maximum number of pages for partition");
      }
      write_guard = bpm_->WritePage(static_cast<page_id_t>(new_page_id));
      intermediate_page = write_guard.AsMut<IntermediateResultPage>();
      if (!intermediate_page->AppendTuple(tuple)) {
        throw std::runtime_error("Tuple size exceeds page size");
      }
    }
  }

  /**
   * Write a batch of tuples into the partition in order to avoid frequent page writes.
   * @param tuple_batch The batch of tuples to be inserted
   */
  void InsertTupleBatch(const std::vector<Tuple> &tuple_batch) {
    if (pages_.empty()) {
      auto new_page_id = bpm_->NewPage();
      pages_.push_back(new_page_id);
    }
    if (pages_.size() > max_page_num_) {
      throw std::runtime_error("Exceeded maximum number of pages for partition");
    }
    auto write_guard = bpm_->WritePage(static_cast<page_id_t>(pages_.back()));
    auto intermediate_page = write_guard.AsMut<IntermediateResultPage>();

    for (const auto &tuple : tuple_batch) {
      if (!intermediate_page->AppendTuple(tuple)) {
        write_guard.Drop();
        auto new_page_id = bpm_->NewPage();
        pages_.push_back(new_page_id);
        if (pages_.size() > max_page_num_) {
          throw std::runtime_error("Exceeded maximum number of pages for partition");
        }
        write_guard = bpm_->WritePage(static_cast<page_id_t>(new_page_id));
        intermediate_page = write_guard.AsMut<IntermediateResultPage>();
        if (!intermediate_page->AppendTuple(tuple)) {
          throw std::runtime_error("Tuple size exceeds page size");
        }
      }
    }
  }

  class Iterator {
    friend class DiskPartition;

   public:
    Iterator() : partition_(nullptr) {};

    auto operator*() -> Tuple {
      if (current_page_idx_ >= partition_->pages_.size()) {
        return Tuple::Empty();
      }
      auto read_guard = partition_->bpm_->ReadPage(partition_->pages_[current_page_idx_]);
      auto intermediate_page = read_guard.As<IntermediateResultPage>();
      auto tuple_opt = intermediate_page->GetTupleAtOffset(current_offset_);
      return tuple_opt.value_or(Tuple::Empty());
    }

    auto operator++() -> Iterator & {
      while (current_page_idx_ < partition_->pages_.size()) {
        auto read_guard = partition_->bpm_->ReadPage(partition_->pages_[current_page_idx_]);
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
     * fetch the current tuple and advance the iterator
     * equals to auto __temp = *it; ++it; return __temp;
     */
    auto GetAndIncrement() -> Tuple {
      while (current_page_idx_ < partition_->pages_.size()) {
        auto read_guard = partition_->bpm_->ReadPage(partition_->pages_[current_page_idx_]);
        auto page = read_guard.As<IntermediateResultPage>();
        auto tuple_opt = page->GetTupleAtOffset(current_offset_);
        BUSTUB_ASSERT(tuple_opt.has_value(), "Page invariant violated during iteration");

        Tuple res = tuple_opt.value();
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

    auto operator!=(const Iterator &other) const -> bool {
      return current_page_idx_ != other.current_page_idx_ || current_offset_ != other.current_offset_;
    }

    auto operator==(const Iterator &other) const -> bool {
      return current_page_idx_ == other.current_page_idx_ && current_offset_ == other.current_offset_;
    }

   private:

    explicit Iterator(DiskPartition *parition) : partition_(parition) {
      Validate();      
    }
    
    explicit Iterator(DiskPartition *partition, size_t current_page_idx, size_t current_offset)
        : partition_(partition), current_page_idx_(current_page_idx), current_offset_(current_offset) {
      Validate();
    }

    void Validate() {
      while (current_page_idx_ < partition_->pages_.size()) {
        auto read_guard = partition_->bpm_->ReadPage(partition_->pages_[current_page_idx_]);
        auto intermediate_page = read_guard.As<IntermediateResultPage>();
        if (intermediate_page->GetSize() > current_offset_ && 
            std::cout << current_page_idx_ << ", " << current_offset_ << "\n",
            intermediate_page->GetTupleAtOffset(current_offset_).has_value()) {
          return;
        }
        current_page_idx_++;
        current_offset_ = 0;
      }
    }

    DiskPartition *partition_;
    size_t current_page_idx_{0};
    size_t current_offset_{0};
  };

  auto Begin() -> Iterator { return Iterator(this); }

  auto End() -> Iterator { return Iterator(this, pages_.size(), 0); }

  auto GetNumPages() const -> size_t { return pages_.size(); }

 private:
  BufferPoolManager *bpm_{nullptr};
  size_t max_page_num_{0};
  std::vector<page_id_t> pages_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto MakeLeftJoinKey(const Tuple &tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(&tuple, left_child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  auto MakeRightJoinKey(const Tuple &tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(&tuple, right_child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  auto GetPartitionId(const HashJoinKey &key) const -> size_t {
    std::hash<HashJoinKey> hasher;
    return hasher(key) % num_partitions_;
  }

  void BuildInMemoryHashTable(DiskPartition &partition_);

  auto ConstructTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple;

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_executor_;
  std::unique_ptr<AbstractExecutor> right_child_executor_;

  bool is_finished_{false};

  std::unordered_map<HashJoinKey, std::vector<Tuple>> in_memory_ht_;

  size_t num_partitions_;

  std::vector<DiskPartition> left_partitions_;
  std::vector<DiskPartition> right_partitions_;
  
  DiskPartition::Iterator partition_iterator_;
  DiskPartition::Iterator iterator_end_;

  bool is_left_build_{false};

  size_t current_partition_index_{0};
  
  Tuple current_probe_tuple_;
  std::vector<Tuple> *current_matching_tuples_{nullptr};
  size_t current_match_index_{0};
};

}  // namespace bustub
