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

namespace bustub {


  
/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeOrderByAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    const auto filter_predicate = seq_scan_plan.filter_predicate_;
    if (filter_predicate == nullptr) {
      return optimized_plan;
    }
    const auto table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
    const auto indexes = catalog_.GetTableIndexes(table_info->name_);

    
    for (const auto &index : indexes) {

    }
  }

  return plan;
}

}  // namespace bustub
