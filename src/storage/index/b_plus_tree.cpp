//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  page_id_t root_page_id = GetRootPageId();
  return root_page_id == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;
  result->clear();

  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }
  int next_page_id = root_page_id;

  while (true) {
    auto r_guard = bpm_->ReadPage(next_page_id);
    const BPlusTreePage *page = r_guard.As<BPlusTreePage>();

    if (!page->IsLeafPage()) {
      const InternalPage *inter_page = r_guard.As<InternalPage>();
      int idx = search(page, false, key);
      next_page_id = inter_page->ValueAt(idx);

    } else {
      const LeafPage *leaf_page = r_guard.As<LeafPage>();
      if (leaf_page->InTombstones(key, comparator_)) {
        return false;
      }
      int idx = search(page, true, key);
      if (idx >= 0) {
        return false;
      }
      auto vals = leaf_page->GetValueArray();
      result->push_back(vals[-idx - 1]);
      return true;
    }
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;

  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto &header_guard = ctx.header_page_.value();
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  page_id_t root_page_id = header_page->root_page_id_;

  if (root_page_id == INVALID_PAGE_ID) {
    // allocate new page
    root_page_id = bpm_->NewPage();
    auto write_gaurd = bpm_->WritePage(root_page_id);
    // insert key and value in the new page
    auto root_page = write_gaurd.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    auto rkeys = root_page->GetKeyArrayMut();
    auto rvals = root_page->GetValueArrayMut();
    rkeys[0] = key;
    rvals[0] = value;
    root_page->SetSize(1);
    // update header_page's root_page_id_
    header_page->root_page_id_ = root_page_id;
    return true;
  }

  ctx.root_page_id_ = root_page_id;

  page_id_t next_page_id = root_page_id;
  auto &write_set = ctx.write_set_;

  while (true) {
    write_set.emplace_back(bpm_->WritePage(next_page_id));
    auto &guard = write_set.back();
    const BPlusTreePage *page = guard.As<BPlusTreePage>();

    if (!page->IsLeafPage()) {
      const InternalPage *inter_page = guard.As<InternalPage>();

      // split an internal node when number of values reaches max_size before insertion
      // else release all the page guards before this page
      if (inter_page->GetSize() < inter_page->GetMaxSize()) {
        write_set.erase(write_set.begin(), write_set.end() - 1);
        if (ctx.header_page_.has_value()) {
          ctx.header_page_.reset();
        }
      }

      int idx = search(page, false, key);
      next_page_id = inter_page->ValueAt(idx);

    } else {
      LeafPage *leaf_page = guard.AsMut<LeafPage>();
      auto lkeys = leaf_page->GetKeyArrayMut();
      auto lvals = leaf_page->GetValueArrayMut();

      // split a leaf node when the number of values reaches max_size after insertion
      if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
        write_set.erase(write_set.begin(), write_set.end() - 1);
        if (ctx.header_page_.has_value()) {
          ctx.header_page_.reset();
        }
      }

      if (leaf_page->RemoveTombstone(key, comparator_)) {
        // key exists in tombstone, reuse the slot
        int idx = -search(page, true, key) - 1;
        lvals[idx] = value;
        return true;
      }

      int idx = search(page, true, key);
      if (idx < 0) {
        // key already exists
        return false;
      }

      int lsz = leaf_page->GetSize();
      memmove(lkeys + idx + 1, lkeys + idx, (lsz - idx) * sizeof(KeyType));
      memmove(lvals + idx + 1, lvals + idx, (lsz - idx) * sizeof(ValueType));
      lkeys[idx] = key;
      lvals[idx] = value;
      leaf_page->SetSize(lsz + 1);

      if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
        Split(ctx);
      }

      return true;
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;

  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto &header_guard = ctx.header_page_.value();
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  page_id_t root_page_id = header_page->root_page_id_;

  if (root_page_id == INVALID_PAGE_ID) {
    // tree is empty
    return;
  }
  ctx.root_page_id_ = root_page_id;

  page_id_t next_page_id = root_page_id;
  auto &write_set = ctx.write_set_;
  auto &child_index_set = ctx.child_index_set_;

  while (true) {
    write_set.emplace_back(bpm_->WritePage(next_page_id));
    auto &guard = write_set.back();
    auto page = guard.As<BPlusTreePage>();

    if (!page->IsLeafPage()) {
      InternalPage *inter_page = guard.AsMut<InternalPage>();

      if (inter_page->GetSize() > inter_page->GetMinSize()) {
        // upper pages won't be affected by merging
        write_set.erase(write_set.begin(), write_set.end() - 1);
        child_index_set.clear();
        if (ctx.header_page_.has_value()) {
          ctx.header_page_.reset();
        }
      }

      int idx = search(page, false, key);
      child_index_set.push_back(idx);
      next_page_id = inter_page->ValueAt(idx);

    } else {
      LeafPage *leaf_page = guard.AsMut<LeafPage>();

      if (leaf_page->GetSize() > leaf_page->GetMinSize()) {
        write_set.erase(write_set.begin(), write_set.end() - 1);
        child_index_set.clear();
        if (ctx.header_page_.has_value()) {
          ctx.header_page_.reset();
        }
      }

      if (leaf_page->InTombstones(key, comparator_)) {
        // key already deleted
        return;
      }

      int idx = -search(page, true, key) - 1;
      if (idx < 0) {
        // page doesn't exists
        return;
      }

      if (leaf_page->GetNumTombstones() < LEAF_PAGE_TOMB_CNT) {
        leaf_page->PushTombstone(idx);
        return;
      }

      int erase_idx;
      if (LEAF_PAGE_TOMB_CNT == 0) {
        erase_idx = idx;
      } else {
        erase_idx = static_cast<int>(leaf_page->PopTombstoneAdjust());
        idx = idx > erase_idx ? idx - 1 : idx;
        leaf_page->PushTombstone(idx);
      }

      size_t lsz = leaf_page->GetSize();

      auto lkeys = leaf_page->GetKeyArrayMut();
      auto lvals = leaf_page->GetValueArrayMut();
      size_t move_size = lsz - erase_idx - 1;
      memmove(lkeys + erase_idx, lkeys + erase_idx + 1, move_size * sizeof(KeyType));
      memmove(lvals + erase_idx, lvals + erase_idx + 1, move_size * sizeof(ValueType));

      leaf_page->SetSize(lsz - 1);

      if (leaf_page->GetSize() < leaf_page->GetMinSize() && write_set.size() > 1) {
        Merge(ctx);
      } else if (ctx.IsRootPage(guard.GetPageId()) && leaf_page->GetSize() == 0) {
        // tree becomes empty
        header_page->root_page_id_ = INVALID_PAGE_ID;
        page_id_t old_root_id = guard.GetPageId();
        write_set.pop_back();
        bpm_->DeletePage(old_root_id);
      }
      return;
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(Context &ctx) {
  auto &write_set = ctx.write_set_;
  KeyType insert_key;
  page_id_t insert_val;
  bool at_leaf = true;

  while (!write_set.empty()) {
    auto &guard = write_set.back();

    if (at_leaf) {
      LeafPage *old_page = guard.AsMut<LeafPage>();

      if (old_page->GetSize() != old_page->GetMaxSize()) {
        throw std::runtime_error("Split error: leaf page is not full");
      }

      size_t max_size = old_page->GetMaxSize();
      size_t min_size = old_page->GetMinSize();

      // get new page
      page_id_t new_page_id = bpm_->NewPage();
      auto new_guard = bpm_->WritePage(new_page_id);
      LeafPage *new_page = new_guard.AsMut<LeafPage>();
      new_page->Init(leaf_max_size_);

      KeyType mid_key = old_page->KeyAt(min_size);

      size_t right_count = max_size - min_size;
      // copy the key and value to new page
      auto okeys = old_page->GetKeyArrayMut();
      auto ovals = old_page->GetValueArrayMut();
      auto nkeys = new_page->GetKeyArrayMut();
      auto nvals = new_page->GetValueArrayMut();

      memcpy(nkeys, okeys + min_size, right_count * sizeof(KeyType));
      memcpy(nvals, ovals + min_size, right_count * sizeof(ValueType));

      old_page->SetSize(static_cast<int>(min_size));
      new_page->SetSize(static_cast<int>(right_count));

      // copy the tombstone
      for (size_t i = 0; i < old_page->GetNumTombstones(); ++i) {
        size_t tIndex = old_page->GetTombstoneAt(i);
        if (tIndex >= static_cast<size_t>(min_size)) {
          new_page->PushTombstone(tIndex - min_size);
        }
      }

      // get the insert key and value for next interation
      insert_key = mid_key;
      insert_val = new_page_id;
      // update next page id;
      new_page->SetNextPageId(old_page->GetNextPageId());
      old_page->SetNextPageId(new_page_id);

      at_leaf = false;
    } else {
      InternalPage *old_page = guard.AsMut<InternalPage>();

      if (old_page->GetSize() < old_page->GetMaxSize()) {
        // insert in the old_page
        auto ikeys = old_page->GetKeyArrayMut();
        auto ivals = old_page->GetValueArrayMut();
        int osz = old_page->GetSize();
        int idx = search(guard.AsMut<BPlusTreePage>(), false, insert_key) + 1;
        memmove(ikeys + idx + 1, ikeys + idx, (osz - idx) * sizeof(ikeys[0]));
        memmove(ivals + idx + 1, ivals + idx, (osz - idx) * sizeof(ivals[0]));
        ikeys[idx] = insert_key;
        ivals[idx] = insert_val;
        old_page->SetSize(osz + 1);
        return;
      }

      size_t max_size = old_page->GetMaxSize();
      size_t min_size = old_page->GetMinSize();
      // get new page
      page_id_t new_page_id = bpm_->NewPage();
      auto new_guard = bpm_->WritePage(new_page_id);
      InternalPage *new_page = new_guard.AsMut<InternalPage>();
      new_page->Init(internal_max_size_);

      KeyType mid_key = old_page->KeyAt(static_cast<int>(min_size));

      int idx = search(guard.AsMut<BPlusTreePage>(), false, insert_key) + 1;

      if (idx <= static_cast<int>(min_size)) {
        size_t right_count = max_size - min_size - 1;  // drop the middle key
        // move half of the nodes to new page
        auto okeys = old_page->GetKeyArrayMut();
        auto ovals = old_page->GetValueArrayMut();
        auto nkeys = new_page->GetKeyArrayMut();
        auto nvals = new_page->GetValueArrayMut();
        memcpy(nkeys + 1, okeys + min_size + 1, right_count * sizeof(KeyType));
        memcpy(nvals, ovals + min_size, (right_count + 1) * sizeof(page_id_t));
        // insert the new key in old page
        memmove(okeys + idx + 1, okeys + idx, (min_size - idx) * sizeof(okeys[0]));
        memmove(ovals + idx + 1, ovals + idx, (min_size - idx) * sizeof(ovals[0]));
        okeys[idx] = insert_key;
        ovals[idx] = insert_val;

        old_page->SetSize(static_cast<int>(min_size + 1));
        new_page->SetSize(static_cast<int>(right_count + 1));

      } else {
        size_t left_count = static_cast<size_t>(idx) - min_size - 1;
        size_t right_count = max_size - static_cast<size_t>(idx);

        // copy right half part of the page and insert new node
        auto okeys = old_page->GetKeyArrayMut();
        auto ovals = old_page->GetValueArrayMut();
        auto nkeys = new_page->GetKeyArrayMut();
        auto nvals = new_page->GetValueArrayMut();
        memcpy(nkeys + 1, okeys + min_size + 1, left_count * sizeof(KeyType));
        memcpy(nvals, ovals + min_size, (left_count + 1) * sizeof(page_id_t));
        nkeys[left_count + 1] = insert_key;
        nvals[left_count + 1] = insert_val;
        memcpy(nkeys + 1 + left_count + 1, okeys + idx, right_count * sizeof(KeyType));
        memcpy(nvals + 1 + left_count + 1, ovals + idx, right_count * sizeof(page_id_t));

        old_page->SetSize(static_cast<int>(min_size));
        new_page->SetSize(static_cast<int>(left_count + right_count + 2));
      }

      insert_key = mid_key;
      insert_val = new_page_id;
    }
    write_set.pop_back();
  }

  if (!ctx.header_page_.has_value()) {
    throw std::runtime_error("Split error: spliting reaches root page who doesn't have value");
  }

  auto &header_guard = ctx.header_page_.value();
  auto head_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  page_id_t new_page_id = bpm_->NewPage();
  auto new_guard = bpm_->WritePage(new_page_id);
  InternalPage *new_page = new_guard.AsMut<InternalPage>();

  new_page->Init(internal_max_size_);
  auto rkeys = new_page->GetKeyArrayMut();
  auto rvals = new_page->GetValueArrayMut();
  rkeys[1] = insert_key;
  rvals[0] = ctx.root_page_id_;
  rvals[1] = insert_val;
  new_page->SetSize(2);

  head_page->root_page_id_ = new_page_id;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(Context &ctx) {
  auto &write_set = ctx.write_set_;
  auto &child_index_set = ctx.child_index_set_;

  // We'll repair underfull nodes bottom-up: start at leaf, then climb parents until balanced or root.
  if (write_set.size() <= 1) {
    return;  // nothing to merge
  }

  bool at_leaf = true;

  while (write_set.size() > 1) {
    auto &node_guard = write_set.back();
    auto &parent_guard = write_set[write_set.size() - 2];
    auto parent_page = parent_guard.AsMut<InternalPage>();
    auto parent_vals = parent_page->GetValueArrayMut();
    int child_idx = child_index_set.back();

    if (at_leaf) {
      auto leaf = node_guard.AsMut<LeafPage>();
      auto lkeys = leaf->GetKeyArrayMut();
      auto lvals = leaf->GetValueArrayMut();
      int min_size = leaf->GetMinSize();

      // Try to fix underflow by borrow/merge until leaf is not underfull.
      while (leaf->GetSize() < min_size) {
        // Try borrow from right sibling
        if (child_idx < parent_page->GetSize() - 1) {
          auto right_id = parent_vals[child_idx + 1];
          auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
          auto right = right_guard.AsMut<LeafPage>();
          auto rkeys = right->GetKeyArrayMut();
          auto rvals = right->GetValueArrayMut();
          if (right->GetSize() > right->GetMinSize()) {
            // Move first entry from right to end of leaf
            KeyType borrow_key = right->KeyAt(0);
            ValueType borrow_val = rvals[0];
            int lsz = leaf->GetSize();
            lkeys[lsz] = borrow_key;
            lvals[lsz] = borrow_val;
            leaf->SetSize(lsz + 1);
            // Shift left
            int rsz = right->GetSize();
            memmove(rkeys, rkeys + 1, (rsz - 1) * sizeof(KeyType));
            memmove(rvals, rvals + 1, (rsz - 1) * sizeof(ValueType));
            right->SetSize(rsz - 1);
            // Update parent guide key to new first key of right
            parent_page->SetKeyAt(child_idx + 1, right->KeyAt(0));

            // Tombstone transfer if necessary
            if (right->RemoveTombstone(0)) {
              if (leaf->GetNumTombstones() == LEAF_PAGE_TOMB_CNT) {
                int pos = leaf->PopTombstoneAdjust();
                int csz2 = leaf->GetSize();
                int mv = csz2 - (pos + 1);
                memmove(lkeys + pos, lkeys + pos + 1, mv * sizeof(KeyType));
                memmove(lvals + pos, lvals + pos + 1, mv * sizeof(ValueType));
                leaf->SetSize(csz2 - 1);
              }
              // lsz is the largest index in leaf, no need to adjust
              leaf->PushTombstone(lsz);
            }

            right->AdjustTombstone(0, true);  // must be called after remove
            continue;                   // re-check underflow
          }
        }

        // Try borrow from left sibling
        if (child_idx > 0) {
          auto left_id = parent_vals[child_idx - 1];
          auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
          auto left = left_guard.AsMut<LeafPage>();
          auto lfvals = left->GetValueArrayMut();
          if (left->GetSize() > left->GetMinSize()) {
            // Move last entry from left to beginning of leaf
            int lsz = left->GetSize();
            KeyType borrow_key = left->KeyAt(lsz - 1);
            ValueType borrow_val = lfvals[lsz - 1];
            int csz = leaf->GetSize();
            memmove(lkeys + 1, lkeys, csz * sizeof(KeyType));
            memmove(lvals + 1, lvals, csz * sizeof(ValueType));
            lkeys[0] = borrow_key;
            lvals[0] = borrow_val;
            leaf->SetSize(csz + 1);
            left->SetSize(lsz - 1);
            // Update parent guide key at idx to borrowed key
            parent_page->SetKeyAt(child_idx, borrow_key);

            // Tombstone transfer if necessary
            if (left->RemoveTombstone(lsz - 1)) {
              if (leaf->GetNumTombstones() == LEAF_PAGE_TOMB_CNT) {
                int pos = leaf->PopTombstoneAdjust();
                int csz2 = leaf->GetSize();
                int mv = csz2 - (pos + 1);
                memmove(lkeys + pos, lkeys + pos + 1, mv * sizeof(KeyType));
                memmove(lvals + pos, lvals + pos + 1, mv * sizeof(ValueType));
                leaf->SetSize(csz2 - 1);
              }
              leaf->PushTombstone(0);
            }

            left->AdjustTombstone(lsz - 1, true);  // must be called after remove
            continue;                        // re-check underflow
          }
        }

        // Merge with a sibling
        if (child_idx < parent_page->GetSize() - 1) {
          // Merge current leaf with right sibling
          auto right_id = parent_vals[child_idx + 1];
          auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
          auto right = right_guard.AsMut<LeafPage>();
          if (leaf->GetSize() + right->GetSize() > leaf->GetMaxSize()) {
            throw std::runtime_error("merge size too large");
          }
          MergeLeafPage(parent_page, leaf, right, child_idx + 1);
          page_id_t drop_page_id = right_guard.GetPageId();
          right_guard.Drop();
          bpm_->DeletePage(drop_page_id);
        } else if (child_idx > 0) {
          // Merge with left sibling: move everything into left, delete current
          auto left_id = parent_vals[child_idx - 1];
          auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
          auto left = left_guard.AsMut<LeafPage>();
          if (leaf->GetSize() + left->GetSize() > leaf->GetMaxSize()) {
            throw std::runtime_error("merge size too large");
          }
          MergeLeafPage(parent_page, left, leaf, child_idx);
          page_id_t drop_page_id = node_guard.GetPageId();
          node_guard.Drop();
          bpm_->DeletePage(drop_page_id);
          leaf = left;
          // After merging into left, the child at child_idx is removed; current path climbs anyway.
        } else {
          // Single child in parent but underfull; will be handled when climbing.
          break;
        }
      }

      at_leaf = false;  // next iteration handles internal node
    } else {
      auto inter = node_guard.AsMut<InternalPage>();
      auto ikeys = inter->GetKeyArrayMut();
      auto ivals = inter->GetValueArrayMut();
      int min_size = inter->GetMinSize();

      while (inter->GetSize() < min_size) {
        if (child_idx < parent_page->GetSize() - 1) {
          // Borrow from right internal sibling
          auto right_id = parent_vals[child_idx + 1];
          auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
          auto right = right_guard.AsMut<InternalPage>();
          auto rkeys = right->GetKeyArrayMut();
          auto rvals = right->GetValueArrayMut();
          if (right->GetSize() > right->GetMinSize()) {
            // Pull down parent key and bring up right's first key
            KeyType borrow_key = parent_page->KeyAt(child_idx + 1);
            page_id_t borrow_val = rvals[0];
            int isz = inter->GetSize();
            ikeys[isz] = borrow_key;
            ivals[isz] = borrow_val;
            inter->SetSize(isz + 1);
            // Move right's first key up to parent and shift right
            parent_page->SetKeyAt(child_idx + 1, right->KeyAt(1));
            int rsz = right->GetSize();
            memmove(rkeys, rkeys + 1, (rsz - 1) * sizeof(KeyType));
            memmove(rvals, rvals + 1, (rsz) * sizeof(page_id_t));
            right->SetSize(rsz - 1);
            continue;
          }
        }

        if (child_idx > 0) {
          // Borrow from left internal sibling
          auto left_id = parent_vals[child_idx - 1];
          auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
          auto left = left_guard.AsMut<InternalPage>();
          auto lvals2 = left->GetValueArrayMut();
          if (left->GetSize() > left->GetMinSize()) {
            KeyType borrow_key = parent_page->KeyAt(child_idx);
            int lsz = left->GetSize();
            page_id_t borrow_val = lvals2[lsz - 1];
            int isz = inter->GetSize();
            memmove(ikeys + 1, ikeys, isz * sizeof(KeyType));
            memmove(ivals + 1, ivals, (isz + 1) * sizeof(page_id_t));
            ikeys[0] = borrow_key;
            ivals[0] = borrow_val;
            inter->SetSize(isz + 1);
            // Move left's last key up to parent
            parent_page->SetKeyAt(child_idx, left->KeyAt(lsz - 1));
            left->SetSize(lsz - 1);
            continue;
          }
        }

        // Merge
        if (child_idx < parent_page->GetSize() - 1) {
          auto right_id = parent_vals[child_idx + 1];
          auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
          auto right = right_guard.AsMut<InternalPage>();
          MergeInternalPage(parent_page, inter, right, child_idx + 1);
          page_id_t drop_page_id = right_guard.GetPageId();
          right_guard.Drop();
          bpm_->DeletePage(drop_page_id);
        } else if (child_idx > 0) {
          auto left_id = parent_vals[child_idx - 1];
          auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
          auto left = left_guard.AsMut<InternalPage>();
          MergeInternalPage(parent_page, left, inter, child_idx);
          page_id_t drop_page_id = node_guard.GetPageId();
          node_guard.Drop();
          bpm_->DeletePage(drop_page_id);
          inter = left;
        } else {
          break;
        }
      }
    }

    getchar();
    // If parent now OK, we can stop; otherwise climb up
    if (parent_page->GetSize() >= parent_page->GetMinSize()) {
      child_index_set.clear();
      write_set.clear();
      return;
    }

    child_index_set.pop_back();
    write_set.pop_back();
  }

  auto &node_guard = write_set.back();

  if (ctx.IsRootPage(node_guard.GetPageId())) {
    auto root_page = node_guard.AsMut<BPlusTreePage>();
    if (root_page->GetSize() == 0 && root_page->IsLeafPage()) {
      // tree is now empty
      auto header_guard = std::move(ctx.header_page_).value();
      auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = INVALID_PAGE_ID;
      page_id_t drop_page_id = node_guard.GetPageId();
      node_guard.Drop();
      bpm_->DeletePage(drop_page_id);
    } else if (root_page->GetSize() == 1 && !root_page->IsLeafPage()) {
      // promote single child as new root
      auto header_guard = std::move(ctx.header_page_).value();
      auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = node_guard.As<InternalPage>()->ValueAt(0);
      page_id_t drop_page_id = node_guard.GetPageId();
      node_guard.Drop();
      bpm_->DeletePage(drop_page_id);
    }
    return;
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeafPage(InternalPage *parent, LeafPage *left, LeafPage *right, int index) {
  // Move all keys and values from the right page to the left page

  int left_size = left->GetSize();
  int right_size = right->GetSize();
  auto pkeys = parent->GetKeyArrayMut();
  auto pvals = parent->GetValueArrayMut();
  auto lkeys = left->GetKeyArrayMut();
  auto lvals = left->GetValueArrayMut();
  auto rkeys = right->GetKeyArrayMut();
  auto rvals = right->GetValueArrayMut();

  // delete parent's guide key and right page id
  int psize = parent->GetSize();
  int move_size = psize - (index + 1);
  memmove(pkeys + index, pkeys + index + 1, move_size * sizeof(KeyType));
  memmove(pvals + index, pvals + index + 1, move_size * sizeof(page_id_t));
  parent->SetSize(psize - 1);
  // merge right page to left page
  memcpy(lkeys + left_size, rkeys, right_size * sizeof(KeyType));
  memcpy(lvals + left_size, rvals, right_size * sizeof(ValueType));
  left->SetSize(left_size + right_size);
  left->SetNextPageId(right->GetNextPageId());
  // merge tombstones
  size_t left_tb_size = left->GetNumTombstones();
  size_t right_tb_size = right->GetNumTombstones();
  int overflow = static_cast<int>(left_tb_size + right_tb_size) - LEAF_PAGE_TOMB_CNT;
  while (overflow > 0) {
    size_t pos = left->PopTombstoneAdjust();
    int lsz = left->GetSize();
    int mv = lsz - (pos + 1);
    memmove(lkeys + pos, lkeys + pos + 1, mv * sizeof(KeyType));
    memmove(lvals + pos, lvals + pos + 1, mv * sizeof(ValueType));
    left->SetSize(lsz - 1);
    overflow--;
  }

  for (size_t i = 0; i < right_tb_size; ++i) {
    size_t tIndex = right->GetTombstoneAt(i);
    left->PushTombstone(tIndex + left_size);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternalPage(InternalPage *parent, InternalPage *left, InternalPage *right, int index) {
  // Move all keys and values from the right page to the left page

  int left_size = left->GetSize();
  int right_size = right->GetSize();
  auto pkeys = parent->GetKeyArrayMut();
  auto pvals = parent->GetValueArrayMut();
  auto lkeys = left->GetKeyArrayMut();
  auto lvals = left->GetValueArrayMut();
  auto rkeys = right->GetKeyArrayMut();
  auto rvals = right->GetValueArrayMut();

  KeyType parent_key = parent->KeyAt(index);
  // delete parent's guide key and right page id
  int psize = parent->GetSize();
  int move_size = psize - (index + 1);
  memmove(pkeys + index, pkeys + index + 1, move_size * sizeof(KeyType));
  memmove(pvals + index, pvals + index + 1, move_size * sizeof(page_id_t));
  parent->SetSize(psize - 1);
  // insert parent's guide key to left page
  lkeys[left_size] = parent_key;
  // merge right page to left page
  memcpy(lkeys + left_size + 1, rkeys, (right_size - 1) * sizeof(KeyType));
  memcpy(lvals + left_size, rvals, right_size * sizeof(page_id_t));
  left->SetSize(left_size + right_size);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::search(const BPlusTreePage *page, bool isLeaf, KeyType key) -> int {
  int cmp;
  if (isLeaf) {
    const LeafPage *leaf_page = reinterpret_cast<const LeafPage *>(page);
    if (leaf_page->GetSize() <= 16) {
      for (int idx = 0; idx < leaf_page->GetSize(); ++idx) {
        cmp = comparator_(leaf_page->KeyAt(idx), key);
        if (cmp == 0) {
          return -idx - 1;  // for deletion
        }

        if (cmp > 0) {
          return idx;
        }
      }
      return leaf_page->GetSize();
    } else {
      int left = 0, right = leaf_page->GetSize(), mid = (left + right) / 2;
      while (left < right) {
        cmp = comparator_(leaf_page->KeyAt(mid), key);

        if (cmp == 0) {
          return -mid - 1;  // for deletion
        } else if (cmp < 0) {
          left = mid + 1;
        } else {
          right = mid;
        }
        mid = (left + right) / 2;
      }
      return mid;
    }
  } else {
    const InternalPage *inter_page = reinterpret_cast<const InternalPage *>(page);
    if (inter_page->GetSize() <= 16) {
      int idx;
      for (idx = 1; idx < inter_page->GetSize(); ++idx) {
        if (comparator_(inter_page->KeyAt(idx), key) > 0) {
          break;
        }
      }
      return idx - 1;
    } else {
      int left = 1, right = inter_page->GetSize(), mid = (left + right) / 2;
      while (left < right) {
        cmp = comparator_(inter_page->KeyAt(mid), key);

        if (cmp == 0) {
          return mid;
        } else if (cmp < 0) {
          left = mid + 1;
        } else {
          right = mid;
        }
        mid = (left + right) / 2;
      }
      return mid - 1;
    }
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
