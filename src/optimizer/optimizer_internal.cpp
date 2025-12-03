//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer_internal.cpp
//
// Identification: src/optimizer/optimizer_internal.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer_internal.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "optimizer/optimizer.h"

namespace bustub {

void OptimizerHelperFunction() {}

void FlipComparisonType(ComparisonType &type) {
  switch (type)
  {
  case ComparisonType::Equal:
    break;
  case ComparisonType::GreaterThan:
    type = ComparisonType::LessThan;
    break;
  case ComparisonType::GreaterThanOrEqual:
    type = ComparisonType::LessThanOrEqual;
    break;
  case ComparisonType::LessThan:
    type = ComparisonType::GreaterThan;
    break;
  case ComparisonType::LessThanOrEqual:
    type = ComparisonType::GreaterThanOrEqual;
    break;
  default:
    break;
  }
}

auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool {
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(expr.get()); const_expr != nullptr) {
    return !const_expr->val_.CastAs(TypeId::INTEGER).GetAs<bool>();
  }
  return false;
}

void Decompose(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates) {
  const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    Decompose(expr->GetChildAt(0), predicates);
    Decompose(expr->GetChildAt(1), predicates);
  } else {
    predicates.push_back(expr);
  }
}

auto MatchPointLookup(const AbstractExpressionRef &expr, std::vector<std::shared_ptr<IndexInfo>> &indexes) -> bool {
  BUSTUB_ASSERT(expr != nullptr, "Expression can't be null");

  if (indexes.size() == 0) {
    return false;
  }
  const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::Or) {
    auto left_ok = MatchPointLookup(logic_expr->GetChildAt(0), indexes);
    auto right_ok = MatchPointLookup(logic_expr->GetChildAt(1), indexes);
    return left_ok && right_ok;
  }

  std::vector<AbstractExpressionRef> predicates;
  DecomposeAnd(expr, predicates);

  auto iter = indexes.begin();
  while (iter != indexes.end()) {
    auto match_result = MatchIndex(predicates, iter->get());
    if (match_result.is_valid && match_result.equal_count_ == (*iter)->index_->GetKeyAttrs().size()) {
      ++iter;
    } else {
      iter = indexes.erase(iter);
    }
  }

  return indexes.size() > 0;
}

auto MatchIndex(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index) -> MatchResult {

  auto key_attrs = index->index_->GetKeyAttrs();
  MatchResult result;
  result.remaining_preds = predicates;
  auto &remains = result.remaining_preds;

  for (size_t idx = 0; idx < key_attrs.size(); ++idx) {
    auto col_idx = key_attrs[idx];
    auto col_name = index->index_->GetKeySchema()->GetColumn(col_idx);
    bool found_matched = false;

    Value best_val;
    AbstractExpressionRef best_match = nullptr;

    auto iter = remains.begin();

    while (iter != remains.end()) {
      const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(iter->get());
      if (cmp_expr == nullptr || cmp_expr->comp_type_ == ComparisonType::NotEqual) {
        ++iter;
        continue;
      }
      auto comp_type = cmp_expr->comp_type_;
      auto left_child = cmp_expr->GetChildAt(0);
      auto right_child = cmp_expr->GetChildAt(1);
      bool is_flipped = false;

      const ColumnValueExpression *column_expr = nullptr;
      const ConstantValueExpression *constant_expr = nullptr;
      
      if (auto col = dynamic_cast<const ColumnValueExpression *>(left_child.get()); col != nullptr) {
        column_expr = col;
        constant_expr = dynamic_cast<const ConstantValueExpression *>(right_child.get());
      } else if (auto col = dynamic_cast<const ColumnValueExpression *>(right_child.get()); col != nullptr) {
        column_expr = col;
        constant_expr = dynamic_cast<const ConstantValueExpression *>(left_child.get());
        is_flipped = true;
        FlipComparisonType(comp_type);
      }
      if (column_expr == nullptr || constant_expr == nullptr) {
        ++iter;
        continue;
      }

      if (column_expr->GetColIdx() == col_idx) {
        bool should_update = false;

        if (comp_type == ComparisonType::Equal) {
          should_update = true;
        } else if (comp_type == ComparisonType::GreaterThan || comp_type == ComparisonType::GreaterThanOrEqual) {
          if (!found_matched || constant_expr->val_.CompareGreaterThan(best_val) == CmpBool::CmpTrue) {
            should_update = true;
          }
        }
        if (should_update) {
          found_matched = true;
          best_match = is_flipped ? left_child : right_child;
          best_val = constant_expr->val_;
          iter = remains.erase(iter);

          if (comp_type == ComparisonType::Equal) {
            result.equal_count_++;
            break;
          }
          continue;
        }
      }
      ++iter;
    }

    if (found_matched) {
      result.is_valid = true;
      result.index_matches.push_back(best_match);
    } else {
      break;
    }
  }
  return result;
}

}  // namespace bustub
