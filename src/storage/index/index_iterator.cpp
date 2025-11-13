//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> buffer_pool_manager, ReadPageGuard read_page_guard, int index_in_page)
  : bpm_(buffer_pool_manager),
    guard_(std::move(read_page_guard)),
    index_in_page_(index_in_page) {}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(INDEXITERATOR_TYPE &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->guard_ = std::move(that.guard_);
  this->index_in_page_ = that.index_in_page_;

  that.index_in_page_ = INVALID_INDEX;
  that.bpm_ = nullptr;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator=(INDEXITERATOR_TYPE &&that) noexcept -> INDEXITERATOR_TYPE & {
  if (*this == that) {
    return *this;
  }

  this->bpm_ = that.bpm_;
  this->guard_ = std::move(that.guard_);
  this->index_in_page_ = that.index_in_page_;

  that.index_in_page_ = INVALID_INDEX;
  that.bpm_ = nullptr;
  return *this;
}
    
FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return index_in_page_ == INVALID_INDEX;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::SkipDeleted() {
  auto leaf_page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();

  while (true) {
    if (index_in_page_ >= leaf_page->GetSize()) {
      page_id_t next_page_id = leaf_page->GetNextPageId();
      if (next_page_id == INVALID_PAGE_ID) {
        index_in_page_ = INVALID_INDEX;
        guard_.Drop();
        return;
      }
      guard_ = std::move(bpm_->ReadPage(next_page_id));
      leaf_page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
      index_in_page_ = 0;
    }
    if (!leaf_page->InTombstones(index_in_page_)) {
      break;
    }
    index_in_page_++;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto leaf_page = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  return {leaf_page->KeyAt(index_in_page_), leaf_page->ValueAt(index_in_page_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {

  if (index_in_page_ == INVALID_INDEX) {
    return *this;
  }
  index_in_page_++;
  SkipDeleted();
  
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
