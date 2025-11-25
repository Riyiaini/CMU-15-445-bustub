//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.h
//
// Identification: src/include/storage/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"


namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_DEFAULT_TOMB_CNT 0
#define LEAF_PAGE_TOMB_CNT ((NumTombs < 0) ? LEAF_PAGE_DEFAULT_TOMB_CNT : NumTombs)
#define LEAF_PAGE_SLOT_CNT                                                                               \
  ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE - sizeof(size_t) - (LEAF_PAGE_TOMB_CNT * sizeof(size_t))) / \
   (sizeof(KeyType) + sizeof(ValueType)))  // NOLINT

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf pages also contain a fixed buffer of "tombstone" indexes for entries
 * that have been deleted.
 *
 * Leaf page format (keys are stored in order, tomb order is up to you):
 *  --------------------
 * | HEADER | TOMB_SIZE | (where TOMB_SIZE is num_tombstones_)
 *  --------------------
 *  -----------------------------------
 * | TOMB(0) | TOMB(1) | ... | TOMB(k) |
 *  -----------------------------------
 *  ---------------------------------
 * | KEY(1) | KEY(2) | ... | KEY(n) |
 *  ---------------------------------
 *  ---------------------------------
 * | RID(1) | RID(2) | ... | RID(n) |
 *  ---------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  -----------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  -----------------------------------------------
 *  -----------------
 * | NextPageId (4) |
 *  -----------------
 */
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  void Init(int max_size = LEAF_PAGE_SLOT_CNT);

  // Tombstone methods
  auto GetTombstones() const -> std::vector<KeyType>;
  auto InTombstones(const KeyType &key, KeyComparator comparator) const -> bool;
  auto InTombstones(const size_t index) const -> bool;
  auto GetNumTombstones() const -> size_t { return num_tombstones_; }
  void SetNumTombstones(size_t num_tombs) { num_tombstones_ = num_tombs; }
  void PushTombstone(const size_t index);
  auto PopTombstone() -> size_t;
  auto PopTombstoneAdjust() -> size_t;
  auto RemoveTombstoneByKey(const KeyType &key, KeyComparator comparator) -> bool;
  auto RemoveTombstoneByIndex(size_t idx) -> bool;
  auto GetTombstoneAt(size_t idx) const -> size_t { return tombstones_[idx]; }
  auto GetTombstoneKeyAt(size_t idx) const -> KeyType { return key_array_[tombstones_[idx]]; }
  void AdjustTombstone(size_t idx, bool is_delete);
  auto EvictTombstone() -> bool;
  
  
  // Helper methods
  auto GetNextPageId() const -> page_id_t { return next_page_id_; }
  void SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }
  auto KeyAt(int index) const -> KeyType { return key_array_[index]; }
  auto ValueAt(int index) const -> ValueType { return rid_array_[index]; }
  void SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }
  void SetValueAt(int index, const ValueType &value) { rid_array_[index] = value; }
  auto GetKeyArrayMut() -> KeyType* { return key_array_; }
  auto GetValueArrayMut() -> ValueType* { return rid_array_; }
  auto GetValueArray() const -> const ValueType* { return rid_array_; }

  auto PopFront() -> std::pair<KeyType, ValueType>;
  void PushBack(const KeyType &key, const ValueType &value);
  auto PopBack() -> std::pair<KeyType, ValueType>;
  void PushFront(const KeyType &key, const ValueType &value);
  auto GetIndexByKey(const KeyType &key, KeyComparator comparator) const -> int;
  auto Insert(const KeyType &key, const ValueType &value, KeyComparator comparator) -> bool;
  auto Erase(const KeyType &key, KeyComparator comparator) -> bool;
  auto Split(BPlusTreeLeafPage *new_page) -> KeyType;

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(tombkey1, tombkey2, ...|key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    auto tombs = GetTombstones();
    for (size_t i = 0; i < tombs.size(); i++) {
      kstr.append(std::to_string(tombs[i].ToString()));
      if ((i + 1) < tombs.size()) {
        kstr.append(",");
      }
    }

    kstr.append("|");

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  size_t num_tombstones_;
  // Fixed-size tombstone buffer (indexes into key_array_ / rid_array_).
  size_t tombstones_[LEAF_PAGE_TOMB_CNT];
  // Array members for page data.
  KeyType key_array_[LEAF_PAGE_SLOT_CNT];
  ValueType rid_array_[LEAF_PAGE_SLOT_CNT];
  // (Spring 2025) Feel free to add more fields and helper functions below if needed
};

}  // namespace bustub
