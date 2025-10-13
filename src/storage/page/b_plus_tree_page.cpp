//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool {
    return this->page_type_ == IndexPageType::LEAF_PAGE;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {
    this->page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int {
    return this->size_;
}
void BPlusTreePage::SetSize(int size) {
    this->size_ = size;
}
void BPlusTreePage::ChangeSizeBy(int amount) {
    this->size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int {
    return this->max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
    this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
auto BPlusTreePage::GetMinSize() const -> int {
    return static_cast<int>((max_size_ + 1) / 2);
}

}  // namespace bustub
