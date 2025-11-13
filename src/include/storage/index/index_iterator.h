//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
#define INVALID_INDEX -1

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT
  IndexIterator(std::shared_ptr<TracedBufferPoolManager> buffer_pool_manager, ReadPageGuard read_page_guard, int index_in_page);

  IndexIterator(const IndexIterator &that) = delete;
  auto operator=(const IndexIterator &that) -> IndexIterator & = delete;
  IndexIterator(IndexIterator &&that) noexcept;
  auto operator=(IndexIterator &&that) noexcept -> IndexIterator &;

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &that) const -> bool {
    if (index_in_page_ == INVALID_INDEX && that.index_in_page_ == INVALID_INDEX) {
      return true;
    } else if ( index_in_page_ == INVALID_INDEX || that.index_in_page_ == INVALID_INDEX) {
      return false;
    }
    return index_in_page_ == that.index_in_page_ && guard_.GetPageId() == that.guard_.GetPageId();
  }

  auto operator!=(const IndexIterator &that) const -> bool {
    if (index_in_page_ == INVALID_INDEX && that.index_in_page_ == INVALID_INDEX) {
      return false;
    } else if ( index_in_page_ == INVALID_INDEX || that.index_in_page_ == INVALID_INDEX) {
      return true;
    }
    return index_in_page_ != that.index_in_page_ || guard_.GetPageId() != that.guard_.GetPageId();
  }

  void SkipDeleted();

 private:

  std::shared_ptr<TracedBufferPoolManager> bpm_;
  ReadPageGuard guard_;
  int index_in_page_{INVALID_INDEX};
  
};

}  // namespace bustub
