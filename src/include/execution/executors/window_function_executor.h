//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/external_merge_sort_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputting rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functions and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
 class SimpleWindowFunctionHashTable {
 public:
  /**
   * Construct a new SimpleWindowFunctionHashTable instance.
   * @param wf_type the types of window function
   */
  SimpleWindowFunctionHashTable(const WindowFunctionType wf_type)
      : wf_type_(wf_type) {}

  /** @return The initial aggregate value for this window function executor */
  auto GenerateInitialAggregateValue() -> Value {
    switch (wf_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        return ValueFactory::GetIntegerValue(0);
      case WindowFunctionType::Rank:
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      default:
        return Value(TypeId::INVALID);
    }
  }

  /**
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(Value *result, const Value &input) {
      // Handle COUNT(*) separately
    if (wf_type_ == WindowFunctionType::CountStarAggregate) {
      *result = result->Add(ValueFactory::GetIntegerValue(1));
      return;
    } 
    if (wf_type_ == WindowFunctionType::Rank) {
      position_++;
      if (last_input_.CompareEquals(input) != CmpBool::CmpTrue) {
        rank_ = position_;
        last_input_ = input;
      }
      
      *result = ValueFactory::GetIntegerValue(rank_);
      return;
    }
    if (input.IsNull()) {
      return;
    }
    switch (wf_type_) {
      case WindowFunctionType::CountAggregate:
        if (result->IsNull()) {
          *result = ValueFactory::GetIntegerValue(1);
        } else {
          *result = result->Add(ValueFactory::GetIntegerValue(1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (result->IsNull()) {
          *result = input;
        } else {
          *result = result->Add(input);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (result->IsNull()) {
          *result = input;
        } else {
          *result = result->Min(input);
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (result->IsNull()) {
          *result = input;
        } else {
          *result = result->Max(input);
        }
        break;
      default:
        break;
    }
  }

  void Init() {
    // Initialize the hash table with no entries.
    ht_.clear();
    // Insert a single entry with null group bys and initial aggregate values.
    AggregateKey empty_key;
    ht_.insert({empty_key, GenerateInitialAggregateValue()});
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  auto InsertCombine(const AggregateKey &agg_key, const Value &agg_val) -> Value {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
    return ht_[agg_key];
  }

  auto GetAggregateValue(const AggregateKey &agg_key) -> Value {
    if (ht_.count(agg_key) == 0) {
      return GenerateInitialAggregateValue();
    }
    return ht_[agg_key];
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, Value>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const Value & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, Value>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, Value> ht_{};
  /** The types of aggregations that we have */
  const WindowFunctionType wf_type_;
  /** The last value seen, used for RANK calculation */
  Value last_input_;
  /** The current rank, used for RANK calculation */
  size_t rank_{1};
  /** The current position, used for RANK calculation */
  size_t position_{0};
};

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:

  auto GetAggregateKey(const std::vector<AbstractExpressionRef> &partition_by, const Tuple *tuple) -> AggregateKey {
    std::vector<Value> values;
    for (const auto &expr : partition_by) {
      auto res = expr->Evaluate(tuple, child_executor_->GetOutputSchema());
      values.push_back(res);
    }
    return AggregateKey{values};
  };

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<std::unordered_map<AggregateKey, Value>> partitions;

  std::unordered_map<uint32_t, SimpleWindowFunctionHashTable> hts_;

  bool is_ordered_{false};
};
}  // namespace bustub
