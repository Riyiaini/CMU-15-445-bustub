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
#include <vector>
#include "execution/expressions/abstract_expression.h"
#include "catalog/catalog.h"

namespace bustub {

// Note: You can define your optimizer helper functions here
void OptimizerHelperFunction();

struct MatchResult {
    /** Matches an index scan */
    bool is_valid_{false};
    /** Predicates conflict with each other, no output */
    bool is_conflict_{false};
    /** Number of comparison expressions with equal type */
    int equal_count_{0};
    /** Range of the index lookup; having the same value represents a point lookup  */
    std::vector<AbstractExpressionRef> lookup_start_;
    std::vector<AbstractExpressionRef> lookup_end_;
    /** Remaining expressions to be used as filter predicates */
    std::vector<AbstractExpressionRef> remaining_preds_;
};

auto IsPredicateFalse(const AbstractExpressionRef &expr) -> bool;

void Decompose(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &predicates);

auto MatchPointLookup(const AbstractExpressionRef &expr, const IndexInfo *index, 
                      std::vector<std::vector<AbstractExpressionRef>> &point_look_ups) -> bool;

auto FindEqualMatch(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index,
                    std::vector<AbstractExpressionRef> &index_match) -> bool;

auto MatchPredWithIndex(const std::vector<AbstractExpressionRef> &predicates, const IndexInfo *index) -> MatchResult;

}  // namespace bustub
