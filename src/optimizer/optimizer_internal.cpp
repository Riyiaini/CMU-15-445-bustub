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

// A normalized comparison where the column is always on the left and constant on the right.
struct NormalizedCmp {
  size_t col_idx;
  ComparisonType type;
  const ConstantValueExpression *constant;
  AbstractExpressionRef constant_expr_ref;  // keep original ref for lookup emission
};

// Normalize a predicate into (column op constant) form if possible.
static auto NormalizeCmp(const AbstractExpressionRef &pred) -> std::optional<NormalizedCmp> {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(pred.get());
  if (cmp_expr == nullptr || cmp_expr->comp_type_ == ComparisonType::NotEqual) {
    return std::nullopt;
  }

  auto comp_type = cmp_expr->comp_type_;
  auto left_child = cmp_expr->GetChildAt(0);
  auto right_child = cmp_expr->GetChildAt(1);

  const ColumnValueExpression *column_expr = nullptr;
  const ConstantValueExpression *constant_expr = nullptr;
  AbstractExpressionRef constant_ref = nullptr;

  if (auto col = dynamic_cast<const ColumnValueExpression *>(left_child.get()); col != nullptr) {
    column_expr = col;
    constant_expr = dynamic_cast<const ConstantValueExpression *>(right_child.get());
    constant_ref = right_child;
  } else if (auto col = dynamic_cast<const ColumnValueExpression *>(right_child.get()); col != nullptr) {
    column_expr = col;
    constant_expr = dynamic_cast<const ConstantValueExpression *>(left_child.get());
    constant_ref = left_child;
    FlipComparisonType(comp_type);
  }

  if (column_expr == nullptr || constant_expr == nullptr) {
    return std::nullopt;
  }

  NormalizedCmp nc{column_expr->GetColIdx(), comp_type, constant_expr, constant_ref};
  return nc;
}

struct Bounds {
  bool has_lower{false};
  Value lower_val{};

  bool has_upper{false};
  Value upper_val{};

  bool is_equal{false};
  AbstractExpressionRef lower_expr{nullptr};
  AbstractExpressionRef upper_expr{nullptr};
};

// Update bounds given a normalized comparison. Returns true if conflict detected.
static auto UpdateBounds(Bounds &b, const NormalizedCmp &nc) -> bool {
  switch (nc.type) {
    case ComparisonType::Equal: {
      // equal implies both bounds to same value
      b.is_equal = true;
      b.lower_expr = nc.constant_expr_ref;
      b.upper_expr = nc.constant_expr_ref;
      break;
    }
    case ComparisonType::GreaterThan:
    case ComparisonType::GreaterThanOrEqual: {
      if (!b.has_lower || nc.constant->val_.CompareGreaterThan(b.lower_val) == CmpBool::CmpTrue) {
        // check conflict with upper
        if (b.has_upper && nc.constant->val_.CompareGreaterThan(b.upper_val) == CmpBool::CmpTrue) {
          return true;
        }
        b.has_lower = true;
        b.lower_val = nc.constant->val_;
        b.lower_expr = nc.constant_expr_ref;
      }
      break;
    }
    case ComparisonType::LessThan:
    case ComparisonType::LessThanOrEqual: {
      if (!b.has_upper || nc.constant->val_.CompareLessThan(b.upper_val) == CmpBool::CmpTrue) {
        if (b.has_lower && nc.constant->val_.CompareLessThan(b.lower_val) == CmpBool::CmpTrue) {
          return true;
        }
        b.has_upper = true;
        b.upper_val = nc.constant->val_;
        b.upper_expr = nc.constant_expr_ref;
      }
      break;
    }
    default:
      break;
  }
  return false;
}

auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool {
  if (expr == nullptr) {
    return false;
  }
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(expr.get()); const_expr != nullptr) {
    return !const_expr->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>();
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

auto MatchPointLookup(const AbstractExpressionRef &expr, const IndexInfo *index, 
                      std::vector<std::vector<AbstractExpressionRef>> &point_lookups) -> bool {
  BUSTUB_ASSERT(expr != nullptr, "Expression can't be null");

  const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
  
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::Or) {
     return MatchPointLookup(logic_expr->GetChildAt(0), index, point_lookups) &&
            MatchPointLookup(logic_expr->GetChildAt(1), index, point_lookups);
  }

  std::vector<AbstractExpressionRef> predicates;
  Decompose(expr, predicates);

  std::vector<AbstractExpressionRef> index_match;
  if (FindEqualMatch(predicates, index, index_match)) {
    point_lookups.emplace_back(std::move(index_match));
    return true;
  }
  return false;
}

auto FindEqualMatch(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index,
                    std::vector<AbstractExpressionRef> &index_match) -> bool {
  auto key_attrs = index->index_->GetKeyAttrs();

  for (size_t idx = 0; idx < key_attrs.size(); ++idx) {
    auto col_idx = key_attrs[idx];
    bool find_match = false;
    bool is_flipped = false;

    for (const auto &pred : predicates) {
      const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(pred.get());
      if (cmp_expr == nullptr || cmp_expr->comp_type_ == ComparisonType::NotEqual) {
        continue;
      }
      auto comp_type = cmp_expr->comp_type_;
      auto left_child = cmp_expr->GetChildAt(0);
      auto right_child = cmp_expr->GetChildAt(1);

      const ColumnValueExpression *column_expr = nullptr;
      const ConstantValueExpression *constant_expr = nullptr;
      
      if (auto col = dynamic_cast<const ColumnValueExpression *>(left_child.get()); col != nullptr) {
        column_expr = col;
        constant_expr = dynamic_cast<const ConstantValueExpression *>(right_child.get());
      } else if (auto col = dynamic_cast<const ColumnValueExpression *>(right_child.get()); col != nullptr) {
        column_expr = col;
        constant_expr = dynamic_cast<const ConstantValueExpression *>(left_child.get());
        is_flipped = true;
      }
      if (column_expr == nullptr || constant_expr == nullptr) {
        continue;
      }

      if (column_expr->GetColIdx() == col_idx && comp_type == ComparisonType::Equal) {
        index_match.push_back(is_flipped ? left_child : right_child);
        find_match = true;
        break;
      }
    }

    if (!find_match) return false;
  }

  return true;
}

auto MatchPredWithIndex(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index_info) -> MatchResult {
  auto key_attrs = index_info->index_->GetKeyAttrs();
  MatchResult result;
  result.remaining_preds_ = predicates;

  // Normalize all usable comparisons and remember their indices for removal.
  std::vector<NormalizedCmp> normalized;
  normalized.reserve(predicates.size());
  for (const auto &p : predicates) {
    auto n = NormalizeCmp(p);
    if (n.has_value()) {
      normalized.emplace_back(n.value());
    }
  }

  // Track which predicates are consumed so we can reconstruct remaining later.
  std::vector<bool> consumed(predicates.size(), false);

  for (size_t ki = 0; ki < key_attrs.size(); ++ki) {
    auto col_idx = key_attrs[ki];
    Bounds b;

    for (size_t i = 0; i < normalized.size(); ++i) {
      const auto &nc = normalized[i];
      if (nc.col_idx != col_idx) { continue; }
      if (UpdateBounds(b, nc)) {
        result.is_conflict_ = true;
        b = Bounds{};
        break;
      }
      consumed[i] = true;
      if (b.is_equal) { break; }
    }

    // If no equal and no bounds for this key, stop processing further keys
    if (!b.is_equal && !b.has_lower && !b.has_upper) {
      break;
    }

    if (b.is_equal) {
      result.equal_count_++;
      result.is_valid_ = true;
      result.lookup_start_.push_back(b.lower_expr);
      result.lookup_end_.push_back(b.upper_expr);
    } else {
      result.is_valid_ = true;
      result.lookup_start_.push_back(b.has_lower ? b.lower_expr : nullptr);
      result.lookup_end_.push_back(b.has_upper ? b.upper_expr : nullptr);
    }
    if (result.is_conflict_) { break; }
  }

  // Rebuild remaining predicates
  std::vector<AbstractExpressionRef> new_remaining;
  new_remaining.reserve(result.remaining_preds_.size());
  for (size_t i = 0; i < result.remaining_preds_.size(); ++i) {
    if (!consumed[i]) { new_remaining.push_back(result.remaining_preds_[i]); }
  }
  result.remaining_preds_ = std::move(new_remaining);

  return result;
}

}  // namespace bustub
