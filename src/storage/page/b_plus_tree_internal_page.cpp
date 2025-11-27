//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size + 1);
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndexByKey(const KeyType& key, KeyComparator comparator) const -> int {
  auto size = GetSize();
  auto it = std::lower_bound(key_array_ + 1, key_array_ + size, key,
                            [comparator](const KeyType &a, const KeyType &b) {
                              return comparator(a, b) < 0;
                            });
  int pos = static_cast<int>(it - key_array_);
  if (pos < size && comparator(*it, key) == 0) {
    return pos;
  }
  return pos - 1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                           KeyComparator comparator) {
  int size = GetSize();
  BUSTUB_ASSERT(size < GetMaxSize(), "page is full");
  int slot = GetIndexByKey(key, comparator) + 1;
  int move_size = size - slot;
  memmove(key_array_ + slot + 1, key_array_ + slot, move_size * sizeof(KeyType));
  memmove(page_id_array_ + slot + 1, page_id_array_ + slot, move_size * sizeof(ValueType));
  key_array_[slot] = key;
  page_id_array_[slot] = value;
  SetSize(size + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAt(int index) {
  int size = GetSize();
  BUSTUB_ASSERT(index > 0 && index < size, "invalid index for erase");
  int move_size = size - index - 1;
  memmove(key_array_ + index, key_array_ + index + 1, move_size * sizeof(KeyType));
  memmove(page_id_array_ + index, page_id_array_ + index + 1, move_size * sizeof(ValueType));
  SetSize(size - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *new_page) -> KeyType {
  int max_size = GetMaxSize();
  int min_size = GetMinSize();
  auto mid_key = key_array_[min_size];
  int move_keys = max_size - min_size - 1;
  int move_vals = max_size - min_size;
  memcpy(new_page->GetKeyArrayMut() + 1, key_array_ + min_size + 1, move_keys * sizeof(KeyType));
  memcpy(new_page->GetValueArrayMut(), page_id_array_ + min_size, move_vals * sizeof(ValueType));
  SetSize(min_size);
  new_page->SetSize(move_vals);
  return mid_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopFront() -> std::pair<KeyType, ValueType> {
  int size = GetSize();
  BUSTUB_ASSERT(size > 1, "page is empty");
  KeyType front_key = key_array_[1];
  ValueType front_value = page_id_array_[0];
  int move_keys = size - 2;
  int move_vals = size - 1;
  memmove(key_array_ + 1, key_array_ + 2, move_keys * sizeof(KeyType));
  memmove(page_id_array_, page_id_array_ + 1, move_vals * sizeof(ValueType));
  SetSize(size - 1);
  return {front_key, front_value};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushBack(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  BUSTUB_ASSERT(size < GetMaxSize(), "page is full");
  key_array_[size] = key;
  page_id_array_[size] = value;
  SetSize(size + 1);
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopBack() -> std::pair<KeyType, ValueType> {
  int size = GetSize();
  BUSTUB_ASSERT(size > 1, "page is empty");
  KeyType back_key = key_array_[size - 1];
  ValueType back_value = page_id_array_[size - 1];
  SetSize(size - 1);
  return {back_key, back_value};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushFront(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  BUSTUB_ASSERT(size < GetMaxSize(), "page is full");
  memmove(key_array_ + 2, key_array_ + 1, (size - 1) * sizeof(KeyType));
  memmove(page_id_array_ + 1, page_id_array_, size * sizeof(ValueType));
  key_array_[1] = key;
  page_id_array_[0] = value;
  SetSize(size + 1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
