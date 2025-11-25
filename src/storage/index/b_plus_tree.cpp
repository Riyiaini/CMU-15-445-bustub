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
    auto page = r_guard.As<BPlusTreePage>();
    
    if (page->IsLeafPage()) {
      auto leaf_page = r_guard.As<LeafPage>();
      if (leaf_page->InTombstones(key, comparator_)) {
        return false;
      }
      int idx = leaf_page->GetIndexByKey(key, comparator_);
      if (idx < 0) {
        return false;
      }
      result->push_back(leaf_page->ValueAt(idx));
      return true;
    }
    auto inter_page = r_guard.As<InternalPage>();
    int idx = inter_page->GetIndexByKey(key, comparator_);
    next_page_id = inter_page->ValueAt(idx);
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
    root_page_id = bpm_->NewPage();
    auto write_gaurd = bpm_->WritePage(root_page_id);
    auto root_page = write_gaurd.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->Insert(key, value, comparator_);
    header_page->root_page_id_ = root_page_id;
    return true;
  }
  ctx.root_page_id_ = root_page_id;
  page_id_t next_page_id = root_page_id;
  auto &write_set = ctx.write_set_;

  write_set.emplace_back(bpm_->WritePage(next_page_id));
  while (true) { 
    auto &guard = write_set.back();
    auto page = guard.As<BPlusTreePage>();

    if (page->IsLeafPage()) {
      break;
    }
    auto inter_page = guard.As<InternalPage>();
    if (inter_page->GetSize() < inter_page->GetMaxSize()) {
     ctx.Drop();
    }
    int slot = inter_page->GetIndexByKey(key, comparator_);
    next_page_id = inter_page->ValueAt(slot);
    write_set.emplace_back(bpm_->WritePage(next_page_id));
  }

  auto &guard = write_set.back();
  auto leaf_page = guard.AsMut<LeafPage>();
  if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
    ctx.Drop();
  }
  if (!leaf_page->Insert(key, value, comparator_)) {
    return false;
  }
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    ResolveOverflow(ctx);
  }
  return true;
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
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto &header_guard = ctx.header_page_.value();
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return;
  }
  ctx.root_page_id_ = root_page_id;
  page_id_t next_page_id = root_page_id;
  auto &write_set = ctx.write_set_;
  auto &child_index_set = ctx.child_index_set_;

  write_set.push_back(bpm_->WritePage(next_page_id));
  while (true) {
    auto &guard = write_set.back();
    auto page = guard.As<BPlusTreePage>();
    
    if (page->IsLeafPage()) {
      break;
    }
    auto inter_page = guard.AsMut<InternalPage>();
    if ((guard.GetPageId() != root_page_id && inter_page->GetSize() > inter_page->GetMinSize()) ||
        (guard.GetPageId() == root_page_id && inter_page->GetSize() > 2)) {
      ctx.Drop();
    }
    int slot = inter_page->GetIndexByKey(key, comparator_);
    child_index_set.push_back(slot);
    next_page_id = inter_page->ValueAt(slot);
    write_set.push_back(bpm_->WritePage(next_page_id));
  }

  auto &guard = write_set.back();
  auto leaf_page = guard.AsMut<LeafPage>();
  if ((guard.GetPageId() != root_page_id && leaf_page->GetSize() > leaf_page->GetMinSize()) ||
      (guard.GetPageId() == root_page_id && leaf_page->GetSize() > 1)) {
    ctx.Drop();
  }
  if (!leaf_page->Erase(key, comparator_)) {
    return;
  }
  if (leaf_page->GetSize() < leaf_page->GetMinSize() && write_set.size() > 1) {
    ResolveUnderflow(ctx);
  } else if (ctx.IsRootPage(guard.GetPageId()) && leaf_page->GetSize() == 0) {
    header_page->root_page_id_ = INVALID_PAGE_ID;
    page_id_t old_root_id = guard.GetPageId();
    write_set.pop_back();
    bpm_->DeletePage(old_root_id);
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto guard = FindLeftmostLeafPage();
  if (!guard.has_value()) {
    return End();
  }

  auto it = INDEXITERATOR_TYPE(bpm_, std::move(guard.value()), 0);
  it.SkipDeleted();
  return it;
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto guard = FindLeafPage(key);
  if (!guard.has_value()) {
    return End();
  }

  auto page_guard = std::move(guard.value());
  auto leaf_page = page_guard.template As<LeafPage>();
  int idx = leaf_page->GetIndexByKey(key, comparator_);

  idx = idx >= 0 ? idx : -idx - 1;

  INDEXITERATOR_TYPE it = INDEXITERATOR_TYPE(bpm_, std::move(page_guard), idx);
  it.SkipDeleted();
  return it;
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
}

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
void BPLUSTREE_TYPE::ResolveOverflow(Context &ctx) {
  auto &write_set = ctx.write_set_;
  auto &guard = write_set.back();
  auto leaf_page = guard.AsMut<LeafPage>();
  BUSTUB_ASSERT(leaf_page->GetSize() == leaf_page->GetMaxSize(), "Leaf page is not full when splitting");

  auto new_page_id = bpm_->NewPage();
  auto new_guard = bpm_->WritePage(new_page_id);
  auto new_page = new_guard.AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);
  KeyType insert_key = leaf_page->Split(new_page);
  page_id_t insert_val = new_page_id;
  new_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  write_set.pop_back();

  while (!write_set.empty()) {
    auto &guard = write_set.back();
    auto old_page = guard.AsMut<InternalPage>();
    if (old_page->GetSize() < old_page->GetMaxSize()) {
      old_page->Insert(insert_key, insert_val, comparator_);
      return;
    }
    page_id_t new_page_id = bpm_->NewPage();
    auto new_guard = bpm_->WritePage(new_page_id);
    auto new_page = new_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    KeyType mid_key = old_page->Split(new_page);
    if (comparator_(insert_key, mid_key) < 0) {
      old_page->Insert(insert_key, insert_val, comparator_);
    } else {
      new_page->Insert(insert_key, insert_val, comparator_);
    }
    insert_key = mid_key;
    insert_val = new_page_id;
    write_set.pop_back();
  }

  page_id_t new_root_id = bpm_->NewPage();
  auto new_root_guard = bpm_->WritePage(new_root_id);
  auto new_root_page = new_root_guard.AsMut<InternalPage>();
  new_root_page->Init(internal_max_size_);
  new_root_page->SetKeyAt(1, insert_key);
  new_root_page->SetValueAt(0, ctx.root_page_id_);
  new_root_page->SetValueAt(1, insert_val);
  new_root_page->SetSize(2);

  BUSTUB_ASSERT(ctx.header_page_.has_value(), "Header page guard should be held when splitting root");
  auto &header_guard = ctx.header_page_.value();
  auto head_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  head_page->root_page_id_ = new_root_id;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ResolveUnderflow(Context &ctx) {
  auto &write_set = ctx.write_set_;
  auto &child_index_set = ctx.child_index_set_;

  {
    auto &leaf_guard = write_set.back();
    auto &parent_guard = write_set[write_set.size() - 2];
    auto parent_page = parent_guard.AsMut<InternalPage>();
    int child_idx = child_index_set.back();

    while (true) {
      auto res = CoelesceOrRedistributeLeaf(parent_page, child_idx, leaf_guard);
      if (res.fixed) break;
      if (res.merged && res.merged_into_left) child_idx -= 1;
    }
    if (ctx.IsRootPage(parent_guard.GetPageId())) {
      if (parent_page->GetSize() <= 1) {
        auto header_guard = std::move(ctx.header_page_).value();
        auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_ = leaf_guard.GetPageId();
        auto drop_page_id = header_guard.GetPageId();
        bpm_->DeletePage(drop_page_id);
      }
      ctx.Clear();
      return;
    }

    write_set.pop_back();
    child_index_set.pop_back();
  }

  while (write_set.size() > 1) {
    auto &inter_guard = write_set.back();
    auto &parent_guard = write_set[write_set.size() - 2];
    auto parent_page = parent_guard.AsMut<InternalPage>();
    auto child_idx = child_index_set.back();

    while (true) {
      auto res = CoelesceOrRedistributeInternal(parent_page, child_idx, inter_guard);
      if (res.fixed) break;
      if (res.merged && res.merged_into_left) child_idx -= 1;
    }

    if (!ctx.IsRootPage(parent_guard.GetPageId())) {
      if (parent_page->GetSize() >= parent_page->GetMinSize()) {
        ctx.Clear();
        return;
      }
    } else {
      if (parent_page->GetSize() <= 1) {
        auto header_guard = std::move(ctx.header_page_).value();
        auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_ = inter_guard.GetPageId();
        page_id_t drop_page_id = parent_guard.GetPageId();
        parent_guard.Drop();
        bpm_->DeletePage(drop_page_id);
      }
      ctx.Clear();
      return;
    }

    child_index_set.pop_back();
    write_set.pop_back();
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoelesceOrRedistributeLeaf(InternalPage *parent, int child_idx,
                                        WritePageGuard &node_guard) -> RebalanceResult {
  // Implementation left as an exercise
  auto leaf_page = node_guard.AsMut<LeafPage>();
  auto result = RebalanceResult{};
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    result.fixed = true;
    return result;
  }
  auto right_bound = parent->GetSize() - 1;
  if (child_idx < right_bound) {
    auto right_id = parent->ValueAt(child_idx + 1);
    auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
    auto right_page = right_guard.AsMut<LeafPage>();
    if (right_page->GetSize() > right_page->GetMinSize()) {
      // Redistribute from right sibling
      auto [borrow_key, borrow_val] = right_page->PopFront();
      leaf_page->PushBack(borrow_key, borrow_val);
      parent->SetKeyAt(child_idx + 1, right_page->KeyAt(0));

      result.fixed = true;
      if (right_page->RemoveTombstoneByIndex(0)) {
        if (leaf_page->GetNumTombstones() == LEAF_PAGE_TOMB_CNT) {
          leaf_page->EvictTombstone();
          // After evicting, size of leaf_page decreases by 1 and is less than min size.
          result.fixed = false;
        }
        leaf_page->PushTombstone(leaf_page->GetSize() - 1);
      }
      right_page->AdjustTombstone(0, true);
      result.merged = false;
      return result;            
    }
  }
  if (child_idx > 0) {
    auto left_id = parent->ValueAt(child_idx - 1);
    auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
    auto left_page = left_guard.AsMut<LeafPage>();
    if (left_page->GetSize() > left_page->GetMinSize()) {
      // Redistribute from left sibling
      auto [borrow_key, borrow_val] = left_page->PopBack();
      leaf_page->PushFront(borrow_key, borrow_val);
      parent->SetKeyAt(child_idx, borrow_key);

      result.fixed = true;
      leaf_page->AdjustTombstone(0, false);
      if (left_page->RemoveTombstoneByIndex(left_page->GetSize() - 1)) {
        if (leaf_page->GetNumTombstones() == LEAF_PAGE_TOMB_CNT) {
          leaf_page->EvictTombstone();
          result.fixed = false;
        }
        leaf_page->PushTombstone(0);
      }
      result.merged = false;
      return result;                      
    }
  }
  // Merge
  if (child_idx < right_bound) {
    auto right_id = parent->ValueAt(child_idx + 1);
    auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
    auto right_page = right_guard.AsMut<LeafPage>();
    MergeLeafPage(parent, leaf_page, right_page, child_idx + 1);
    page_id_t drop_page_id = right_guard.GetPageId();
    right_guard.Drop();
    bpm_->DeletePage(drop_page_id);
    result.fixed = leaf_page->GetSize() >= leaf_page->GetMinSize();
    result.merged_into_left = false;
  } else if (child_idx > 0) {
    auto left_id = parent->ValueAt(child_idx - 1);
    auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
    auto left_page = left_guard.AsMut<LeafPage>();
    MergeLeafPage(parent, left_page, leaf_page, child_idx);
    page_id_t drop_page_id = node_guard.GetPageId();
    bpm_->DeletePage(drop_page_id);
    node_guard = std::move(left_guard);
    result.fixed = left_page->GetSize() >= left_page->GetMinSize();
    result.merged_into_left = true;
  } else {
    BUSTUB_ASSERT(false, "Cannot merge leaf page without siblings");
  }
  result.merged = true;
  return result;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CoelesceOrRedistributeInternal(InternalPage *parent, int child_idx,
                                        WritePageGuard &node_guard) -> RebalanceResult {
  auto inter_page = node_guard.AsMut<InternalPage>();
  auto result = RebalanceResult{};
  if (inter_page->GetSize() >= inter_page->GetMinSize()) {
    result.fixed = true;
    return result;
  }
  auto right_bound = parent->GetSize() - 1;
  if (child_idx < right_bound) {
    auto right_id = parent->ValueAt(child_idx + 1);
    auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
    auto right_page = right_guard.AsMut<InternalPage>();
    if (right_page->GetSize() > right_page->GetMinSize()) {
      // Redistribute from right sibling
      auto parent_key = parent->KeyAt(child_idx + 1);
      auto [borrow_key, borrow_val] = right_page->PopFront();
      inter_page->PushBack(parent_key, borrow_val);
      parent->SetKeyAt(child_idx + 1, borrow_key);

      result.fixed = true;
      result.merged = false;
      return result;            
    }
  }
  if (child_idx > 0) {
    auto left_id = parent->ValueAt(child_idx - 1);
    auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
    auto left_page = left_guard.AsMut<InternalPage>();
    if (left_page->GetSize() > left_page->GetMinSize()) {
      // Redistribute from left sibling
      auto parent_key = parent->KeyAt(child_idx);
      auto [borrow_key, borrow_val] = left_page->PopBack();
      inter_page->PushFront(parent_key, borrow_val);
      parent->SetKeyAt(child_idx, borrow_key);

      result.fixed = true;
      result.merged = false;
      return result;                      
    }
  }
  // Merge
  if (child_idx < right_bound) {
    auto right_id = parent->ValueAt(child_idx + 1);
    auto right_guard = bpm_->WritePage(static_cast<page_id_t>(right_id));
    auto right_page = right_guard.AsMut<InternalPage>();
    MergeInternalPage(parent, inter_page, right_page, child_idx + 1);
    page_id_t drop_page_id = right_guard.GetPageId();
    right_guard.Drop();
    bpm_->DeletePage(drop_page_id);
    result.fixed = inter_page->GetSize() >= inter_page->GetMinSize();
    result.merged_into_left = false;
  } else if (child_idx > 0) {
    auto left_id = parent->ValueAt(child_idx - 1);
    auto left_guard = bpm_->WritePage(static_cast<page_id_t>(left_id));
    auto left_page = left_guard.AsMut<InternalPage>();
    MergeInternalPage(parent, left_page, inter_page, child_idx);
    page_id_t drop_page_id = node_guard.GetPageId();
    node_guard = std::move(left_guard);
    bpm_->DeletePage(drop_page_id);
    result.fixed = left_page->GetSize() >= left_page->GetMinSize();
    result.merged_into_left = true;
  } else {
    BUSTUB_ASSERT(false, "Cannot merge internal page without siblings");
  }
  result.merged = true;
  return result;
}


FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeafPage(InternalPage *parent, LeafPage *left, LeafPage *right, int index) {

  int left_size = left->GetSize();
  int right_size = right->GetSize();
  auto pkeys = parent->GetKeyArrayMut();
  auto pvals = parent->GetValueArrayMut();
  auto lkeys = left->GetKeyArrayMut();
  auto lvals = left->GetValueArrayMut();
  auto rkeys = right->GetKeyArrayMut();
  auto rvals = right->GetValueArrayMut();

  int psize = parent->GetSize();
  int move_size = psize - (index + 1);
  memmove(pkeys + index, pkeys + index + 1, move_size * sizeof(KeyType));
  memmove(pvals + index, pvals + index + 1, move_size * sizeof(page_id_t));
  parent->SetSize(psize - 1);
  memcpy(lkeys + left_size, rkeys, right_size * sizeof(KeyType));
  memcpy(lvals + left_size, rvals, right_size * sizeof(ValueType));
  left->SetSize(left_size + right_size);
  left->SetNextPageId(right->GetNextPageId());
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

  int left_size = left->GetSize();
  int right_size = right->GetSize();
  auto pkeys = parent->GetKeyArrayMut();
  auto pvals = parent->GetValueArrayMut();
  auto lkeys = left->GetKeyArrayMut();
  auto lvals = left->GetValueArrayMut();
  auto rkeys = right->GetKeyArrayMut();
  auto rvals = right->GetValueArrayMut();

  KeyType parent_key = parent->KeyAt(index);
  int psize = parent->GetSize();
  int move_size = psize - (index + 1);
  memmove(pkeys + index, pkeys + index + 1, move_size * sizeof(KeyType));
  memmove(pvals + index, pvals + index + 1, move_size * sizeof(page_id_t));
  parent->SetSize(psize - 1);
  lkeys[left_size] = parent_key;
  memcpy(lkeys + left_size + 1, rkeys + 1, (right_size - 1) * sizeof(KeyType));
  memcpy(lvals + left_size, rvals, right_size * sizeof(page_id_t));
  left->SetSize(left_size + right_size);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> std::optional<ReadPageGuard> {
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;

  if (root_page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  auto cur_guard = bpm_->ReadPage(root_page_id);
  while (true) {
    auto page = cur_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto inter_page = cur_guard.As<InternalPage>();
    int slot = inter_page->GetIndexByKey(key, comparator_);
    cur_guard = bpm_->ReadPage(inter_page->ValueAt(slot));
  }
  return cur_guard;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftmostLeafPage() -> std::optional<ReadPageGuard> {
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto root_page_id = header_page->root_page_id_;

  if (root_page_id == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  auto cur_guard = bpm_->ReadPage(root_page_id);
  while (true) {
    auto page = cur_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      break;
    }
    auto inter_page = cur_guard.As<InternalPage>();
    cur_guard = bpm_->ReadPage(inter_page->ValueAt(0));
  }
  return cur_guard;
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
