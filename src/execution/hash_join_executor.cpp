//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  size_t pool_size = exec_ctx_->GetBufferPoolManager()->Size();
  if (pool_size > 3) {
    num_partitions_ = pool_size - 2;
  } else {
    num_partitions_ = 1;
  }

  // Reserve and initialize partitions now that num_partitions_ is known to avoid
  // reallocation (which would invalidate pointers held by iterators).
  left_partitions_.reserve(num_partitions_);
  right_partitions_.reserve(num_partitions_);
  for (size_t i = 0; i < num_partitions_; ++i) {
    left_partitions_.emplace_back(DiskPartition(exec_ctx_->GetBufferPoolManager()));
    right_partitions_.emplace_back(DiskPartition(exec_ctx_->GetBufferPoolManager()));
  }
}

void HashJoinExecutor::BuildInMemoryHashTable(DiskPartition &partition) {
  in_memory_ht_.clear();
  for (auto it = partition.Begin(); it != partition.End(); ++it) {
    const auto &tuple = *it;
    auto key = is_left_build_ ? MakeLeftJoinKey(tuple) : MakeRightJoinKey(tuple);
    in_memory_ht_[key].emplace_back(tuple);
  }
}

auto HashJoinExecutor::ConstructTuple(const Tuple *left_tuple, const Tuple *right_tuple) -> Tuple {
  std::vector<Value> values;
  auto left_count = left_child_executor_->GetOutputSchema().GetColumnCount();
  auto right_count = right_child_executor_->GetOutputSchema().GetColumnCount();
  values.reserve(left_count + right_count);

  for (uint32_t i = 0; i < left_count; i++) {
    values.emplace_back(left_tuple->GetValue(&left_child_executor_->GetOutputSchema(), i));
  }
  if (right_tuple != nullptr) {
    for (uint32_t i = 0; i < right_count; i++) {
      values.emplace_back(right_tuple->GetValue(&right_child_executor_->GetOutputSchema(), i));
    }
  } else {
    for (uint32_t i = 0; i < right_count; i++) {
      values.emplace_back(
          ValueFactory::GetNullValueByType(right_child_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
  }
  return Tuple(values, &plan_->OutputSchema());
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  auto header_size = sizeof(RID) + sizeof(uint32_t);

  left_child_executor_->Init();
  right_child_executor_->Init();

  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;
  std::vector<std::vector<Tuple>> tuple_buffers(num_partitions_);
  std::vector<uint32_t> buffer_size(num_partitions_, 0);

  for (auto &partition : left_partitions_) {
    partition.Reset();
  }
  for (auto &partition : right_partitions_) {
    partition.Reset();
  }

  while (left_child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuple_batch) {
      auto key = MakeLeftJoinKey(tuple);
      auto partition_id = GetPartitionId(key);

      tuple_buffers[partition_id].emplace_back(tuple);
      buffer_size[partition_id] += (tuple.GetLength() + header_size);
      if (buffer_size[partition_id] >= BUSTUB_PAGE_SIZE) {
        left_partitions_[partition_id].InsertTupleBatch(tuple_buffers[partition_id]);
        tuple_buffers[partition_id].clear();
        buffer_size[partition_id] = 0;
      }
    }
    tuple_batch.clear();
    rid_batch.clear();
  }

  for (size_t i = 0; i < num_partitions_; ++i) {
    if (!tuple_buffers[i].empty()) {
      left_partitions_[i].InsertTupleBatch(tuple_buffers[i]);
      tuple_buffers[i].clear();
    }
  }
  std::fill(buffer_size.begin(), buffer_size.end(), 0);

  while (right_child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : tuple_batch) {
      auto key = MakeRightJoinKey(tuple);
      auto partition_id = GetPartitionId(key);

      tuple_buffers[partition_id].emplace_back(tuple);
      buffer_size[partition_id] += (tuple.GetLength() + header_size);
      if (buffer_size[partition_id] >= BUSTUB_PAGE_SIZE) {
        right_partitions_[partition_id].InsertTupleBatch(tuple_buffers[partition_id]);
        tuple_buffers[partition_id].clear();
        buffer_size[partition_id] = 0;
      }
    }
    tuple_batch.clear();
    rid_batch.clear();
  }

  for (size_t i = 0; i < num_partitions_; ++i) {
    if (!tuple_buffers[i].empty()) {
      right_partitions_[i].InsertTupleBatch(tuple_buffers[i]);
      tuple_buffers[i].clear();
    }
  }

  // current partition index will increase by one during initialization
  current_partition_index_ = -1;
  in_memory_ht_.clear();
  current_matching_tuples_ = nullptr;
  current_match_index_ = 0;
  partition_iterator_ = iterator_end_ = DiskPartition::Iterator();
  is_finished_ = false;
}
/**
 * Yield the next tuple batch from the hash join.
 * @param[out] tuple_batch The next tuple batch produced by the hash join
 * @param[out] rid_batch The next tuple RID batch produced by the hash join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  static std::vector<Tuple> null_match_vec{Tuple()};

  tuple_batch->clear();
  tuple_batch->clear();

  if (is_finished_) {
    return false;
  }

  while (true) {
    if (current_matching_tuples_ != nullptr) {
      while (current_match_index_ < current_matching_tuples_->size()) {
        if (tuple_batch->size() >= batch_size) {
          return true;
        }
        const auto &build_tuple = (*current_matching_tuples_)[current_match_index_];

        Tuple output_tuple;
        if (is_left_build_) {
          output_tuple = ConstructTuple(&build_tuple, &current_probe_tuple_);
        } else {
          if (build_tuple.GetLength() == 0 && plan_->GetJoinType() == JoinType::LEFT) {
            output_tuple = ConstructTuple(&current_probe_tuple_, nullptr);
          } else {
            output_tuple = ConstructTuple(&current_probe_tuple_, &build_tuple);
          }
        }

        tuple_batch->emplace_back(output_tuple);
        rid_batch->emplace_back(output_tuple.GetRid());
        current_match_index_++;
      }
      current_matching_tuples_ = nullptr;
      current_match_index_ = 0;
    }

    if (partition_iterator_ == iterator_end_) {
      current_partition_index_++;
      // std::cout << current_partition_index_ << "/" << num_partitions_ << " partitions processed.\n";
      in_memory_ht_.clear();

      while (current_partition_index_ < num_partitions_) {
        auto &left_partition = left_partitions_[current_partition_index_];
        auto &right_partition = right_partitions_[current_partition_index_];

        if (left_partition.GetNumPages() < right_partition.GetNumPages() && plan_->GetJoinType() != JoinType::LEFT) {
          is_left_build_ = true;
          BuildInMemoryHashTable(left_partition);
          partition_iterator_ = right_partition.Begin();
          iterator_end_ = right_partition.End();
        } else {
          is_left_build_ = false;
          BuildInMemoryHashTable(right_partition);
          partition_iterator_ = left_partition.Begin();
          iterator_end_ = left_partition.End();
        }

        if (partition_iterator_ != iterator_end_) {
          break;
        }
        
        current_partition_index_++;
        // std::cout << current_partition_index_ << "/" << num_partitions_ << " partitions processed --.\n";
        in_memory_ht_.clear();
      }

      if (current_partition_index_ >= num_partitions_) {
        is_finished_ = true;
        return !tuple_batch->empty();
      }
    }

    current_probe_tuple_ = partition_iterator_.GetAndIncrement();

    if (current_probe_tuple_.GetLength() == 0) {
      // no more tuples in this partition
      partition_iterator_ = iterator_end_;
      continue;
    }

    auto key = is_left_build_ ? MakeRightJoinKey(current_probe_tuple_) : MakeLeftJoinKey(current_probe_tuple_);
    // std::cout << in_memory_ht_.size() << " keys in in-memory hash table.\n";
    auto it = in_memory_ht_.find(key);

    if (it != in_memory_ht_.end()) {
      // std::cout << "found matching key in in-memory hash table.\n"; 
      current_matching_tuples_ = &it->second;
      current_match_index_ = 0;
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      current_matching_tuples_ = &null_match_vec;
      current_match_index_ = 0;
    }
  }
}

}  // namespace bustub
