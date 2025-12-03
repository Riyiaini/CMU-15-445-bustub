//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// optimizer_internal.h
//
// Identification: src/include/optimizer/optimizer_internal.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace bustub {

// Note: You can define your optimizer helper functions here
void OptimizerHelperFunction();

struct MatchResult {
    bool is_valid{false};
    /** Number of comparison expressions with equal type */
    int equal_count_{0};
    /** Constant expressions corresponding to index keys */
    std::vector<AbstractExpressionRef> index_matches;
    /** Remaining expressions to be used as filter predicates */
    std::vector<AbstractExpressionRef> remaining_preds;
};

auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool;

void DecomposeOr(const AbstractExpressionRef &expr, std::vector<std::vector<AbstractExpressionRef>> &predicates);

void DecomposeAnd(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates);

auto MatchIndex(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index) -> MatchResult;

}  // namespace bustub
