//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <algorithm>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);

  SetNextPageId(INVALID_PAGE_ID);
  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombstones;
  for (size_t i = 0; i < num_tombstones_; ++i) {
    tombstones.push_back(key_array_[tombstones_[i]]);
  }
  return tombstones;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InTombstones(const KeyType &key, KeyComparator comparator) const -> bool {
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (comparator(KeyAt(tombstones_[i]), key) == 0) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InTombstones(const size_t index) const -> bool {
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] == index) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushTombstone(const size_t index) {

  tombstones_[num_tombstones_++] = index;
}


FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopTombstone() -> size_t {
  size_t idx = tombstones_[0];
  memmove(tombstones_, tombstones_ + 1, (num_tombstones_ - 1) * sizeof(size_t));
  --num_tombstones_;
  return idx;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopTombstoneAdjust() -> size_t {
  size_t idx = tombstones_[0];
  for (size_t i = 0; i < num_tombstones_ - 1; ++i) {
    size_t next_idx = tombstones_[i + 1];
    tombstones_[i] = next_idx > idx ? next_idx - 1 : next_idx;
  }
  --num_tombstones_;
  return idx;
}


FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveTombstoneByKey(const KeyType &key, KeyComparator comparator) -> bool {
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (comparator(KeyAt(tombstones_[i]), key) == 0) {
      // Shift left to remove tombstone
      memmove(tombstones_ + i, tombstones_ + i + 1, (num_tombstones_ - i - 1) * sizeof(size_t));
      --num_tombstones_;
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveTombstoneByIndex(size_t idx) -> bool {
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] == idx) {
      // Shift left to remove tombstone
      memmove(tombstones_ + i, tombstones_ + i + 1, (num_tombstones_ - i - 1) * sizeof(size_t));
      --num_tombstones_;
      return true;
    }
  }
  return false;
}

/**
 * @brief Helper method to adjust tombstone indexes before insertion or after deletion
 * @warning: must be called before insertion or after deletion
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AdjustTombstone(size_t idx, bool is_delete) {
  int shift = is_delete ? -1 : 1;
  for (size_t i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] >= idx) {
      tombstones_[i] += shift;
    }
  } 
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EvictTombstone() -> bool {
  if (num_tombstones_ == 0) {
    return false;
  }
  size_t idx = PopTombstone();
  size_t move_size = GetSize() - idx - 1;
  if (move_size > 0) {
    memmove(key_array_ + idx, key_array_ + idx + 1, move_size * sizeof(KeyType));
    memmove(rid_array_ + idx, rid_array_ + idx + 1, move_size * sizeof(ValueType));
    for (size_t i = 0; i < num_tombstones_; ++i) {
      if (tombstones_[i] >= idx) {
        tombstones_[i] -= 1;
      }
    } 
  }
  SetSize(GetSize() - 1);
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() -> std::pair<KeyType, ValueType> {
  int size = GetSize();
  BUSTUB_ASSERT(size > 0, "page is empty");
  KeyType front_key = key_array_[0];
  ValueType front_value = rid_array_[0];
  int move_size = size - 1;
  memmove(key_array_, key_array_ + 1, move_size * sizeof(KeyType));
  memmove(rid_array_, rid_array_ + 1, move_size * sizeof(ValueType));
  SetSize(size - 1);
  return {front_key, front_value};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  BUSTUB_ASSERT(size < GetMaxSize(), "page is full");
  key_array_[size] = key;
  rid_array_[size] = value;
  SetSize(size + 1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() -> std::pair<KeyType, ValueType> {
  int size = GetSize();
  BUSTUB_ASSERT(size > 0, "page is empty");
  KeyType back_key = key_array_[size - 1];
  ValueType back_value = rid_array_[size - 1];
  SetSize(size - 1);
  return {back_key, back_value};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  BUSTUB_ASSERT(size < GetMaxSize(), "page is full");
  memmove(key_array_ + 1, key_array_, size * sizeof(KeyType));
  memmove(rid_array_ + 1, rid_array_, size * sizeof(ValueType));
  key_array_[0] = key;
  rid_array_[0] = value;
  SetSize(size + 1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexByKey(const KeyType &key, KeyComparator comparator) const -> int {
  auto size = GetSize();
  auto it = std::lower_bound(key_array_, key_array_ + size, key,
                            [comparator](const KeyType &a, const KeyType &b) {
                              return comparator(a, b) < 0; 
                            });
  int pos = static_cast<int>(it - key_array_);
  if (pos < size && comparator(*it, key) == 0) {
    return pos;
  }
  return -pos - 1;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                       KeyComparator comparator) -> bool {
  int size = GetSize();
  BUSTUB_ASSERT(size < GetMaxSize(), "page is full");   
  if (size == 0) {
    key_array_[0] = key;
    rid_array_[0] = value;
    SetSize(1);
    return true;
  }
  if (RemoveTombstoneByKey(key, comparator)) {
    int slot = GetIndexByKey(key, comparator);
    BUSTUB_ASSERT(slot >= 0, "invalid slot for tombstone key");
    rid_array_[slot] = value;
    return true;
  }
  int slot = GetIndexByKey(key, comparator);
  if (slot >= 0) {
    return false;
  }
  slot = -slot - 1;
  AdjustTombstone(slot, false);
  memmove(key_array_ + slot + 1, key_array_ + slot, (size - slot) * sizeof(KeyType));
  memmove(rid_array_ + slot + 1, rid_array_ + slot, (size - slot) * sizeof(ValueType));
  key_array_[slot] = key;
  rid_array_[slot] = value;
  SetSize(size + 1);
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Erase(const KeyType &key, KeyComparator comparator) -> bool {
  if (InTombstones(key, comparator)) {
    return false;
  }
  int slot = GetIndexByKey(key, comparator);
  if (slot < 0) {
    return false;
  }
  if (num_tombstones_ < LEAF_PAGE_TOMB_CNT) {
    PushTombstone(slot);
    return false;
  }
  int erase_idx;
  if (LEAF_PAGE_TOMB_CNT > 0) {
    erase_idx = static_cast<int>(PopTombstoneAdjust());
    slot = slot > erase_idx ? slot - 1 : slot;
    PushTombstone(slot);
  } else {
    erase_idx = slot;
  }
  size_t move_size = GetSize() - erase_idx - 1;
  memmove(key_array_ + erase_idx, key_array_ + erase_idx + 1, move_size * sizeof(KeyType));
  memmove(rid_array_ + erase_idx, rid_array_ + erase_idx + 1, move_size * sizeof(ValueType));
  SetSize(GetSize() - 1);
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage *new_page) -> KeyType {
  int max_size = GetMaxSize();
  int min_size = GetMinSize();
  auto mid_key = key_array_[min_size];
  int right_count = max_size - min_size;
  memcpy(new_page->GetKeyArrayMut(), key_array_ + min_size, right_count * sizeof(KeyType));
  memcpy(new_page->GetValueArrayMut(), rid_array_ + min_size, right_count * sizeof(ValueType));
  SetSize(min_size);
  new_page->SetSize(right_count);
  for (size_t i = 0; i < num_tombstones_; ++i) {
    size_t tIndex = GetTombstoneAt(i);
    if (tIndex >= static_cast<size_t>(min_size)) {
      new_page->PushTombstone(tIndex - min_size);
      RemoveTombstoneByIndex(tIndex);
      --i;
    }
  }
  return mid_key;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
