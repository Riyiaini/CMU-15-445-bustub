//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "optimizer/optimizer_internal.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {


  
/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  const auto table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
  const auto indexes = catalog_.GetTableIndexes(table_info->name_);

  for (const auto &index : indexes) {
    std::vector<std::vector<AbstractExpressionRef>> point_lookups;
    // point lookup. 
    // eg., WHERE v = 1 ✔       WHERE 1 = v  ✔       WHERE v = 1 OR v = 2 ✔      
    //      WHERE v > 1 ✘       WHERE v = 1 AND v = 2 ✘
    if (MatchPointLookup(seq_scan_plan.filter_predicate_, index.get(), point_lookups)) {
      return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_, index->index_oid_,
                                                 seq_scan_plan.filter_predicate_, std::move(point_lookups),
                                                 std::vector<AbstractExpressionRef>(), true);
    }
    if (point_lookups.size() > 1) {
      return optimized_plan;
    }
  }

  std::vector<AbstractExpressionRef> predicates;
  Decompose(seq_scan_plan.filter_predicate_, predicates);

  for (const auto &index : indexes) {
    auto result = MatchPredWithIndex(predicates, index.get());
    // conflicting predicates, return empty index scan
    // eg., WHERE v > 2 AND v < 1
    if (result.is_conflict_) {
      return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_, index->index_oid_,
                                                 std::move(seq_scan_plan.filter_predicate_), std::vector<std::vector<AbstractExpressionRef>>(),
                                                 std::vector<AbstractExpressionRef>(), true);
    }
    // range scan, can only parse and conditions
    // eg., WHERE v > 1 ✔       WHERE v > 1 AND v <= 5 ✔     WHERE v = 1 AND v < 5 ✔
    //      WHERE v < 1 OR v > 3 ✘
    if (result.is_valid_) {
      std::vector<std::vector<AbstractExpressionRef>> range_lookup;
      range_lookup.emplace_back(std::move(result.lookup_start_));
      range_lookup.emplace_back(std::move(result.lookup_end_));
      return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_, index->index_oid_,
                                                 std::move(seq_scan_plan.filter_predicate_), std::move(range_lookup),
                                                 std::move(result.remaining_preds_), false);
    }
  }

  return optimized_plan;
}

}  // namespace bustub
