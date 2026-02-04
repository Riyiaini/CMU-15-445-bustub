//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "optimizer/optimizer_internal.h"
#include "type/type_id.h"

namespace bustub {

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> child_plans;
  for (const auto &child : plan->GetChildren()) {
    child_plans.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(child_plans));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(nlj_plan.children_.size() == 2, "NLJ should have exactly two children");

    std::vector<AbstractExpressionRef> predicates;
    Decompose(nlj_plan.predicate_, predicates);

    std::vector<AbstractExpressionRef> left_key_expressions, right_key_expressions;

    for (const auto &pred : predicates) {
      const auto *expr = dynamic_cast<const ComparisonExpression *>(pred.get());
      if (expr == nullptr || expr->comp_type_ != ComparisonType::Equal) {
        return optimized_plan;
      }

      const auto &left_child = expr->GetChildAt(0);
      const auto &right_child = expr->GetChildAt(1);

      const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(left_child.get());
      const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(right_child.get());
      if (left_expr == nullptr || right_expr == nullptr) {
        return optimized_plan;
      }

      if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
        left_key_expressions.emplace_back(left_child);
        right_key_expressions.emplace_back(right_child);
      } else if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
        left_key_expressions.emplace_back(right_child);
        right_key_expressions.emplace_back(left_child);
      } else {
        return optimized_plan;
      }
    }

    return std::make_shared<HashJoinPlanNode>(
        nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
        std::move(left_key_expressions), std::move(right_key_expressions),
        nlj_plan.join_type_);
  }

  return optimized_plan;
}

}  // namespace bustub
