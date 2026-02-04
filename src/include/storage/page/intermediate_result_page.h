#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include <iostream>

#include "storage/table/tuple.h"

static constexpr size_t HEADER_SIZE = 8;
#define INTERMEDIATE_RESULT_PAGE_DATA_SIZE (BUSTUB_PAGE_SIZE - HEADER_SIZE)

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  explicit IntermediateResultPage() : size_(0) {}

  auto GetSize() const -> size_t { return size_; }

  auto GetTupleAtOffset(size_t offset) const -> std::optional<Tuple> {
    if (offset + sizeof(RID) + sizeof(uint32_t) > size_) {
      return std::nullopt;
    }

    RID rid = *reinterpret_cast<const RID *>(data_ + offset);
    Tuple tuple(rid);
    auto size = *reinterpret_cast<const uint32_t *>(data_ + offset + sizeof(RID));
    if (offset + sizeof(RID) + sizeof(uint32_t) + size > size_) {
      std::cerr << "Invalid tuple size read from IntermediateResultPage: offset=" << offset << " size_field=" << size
                << " page_used=" << size_ << "\n";
      return std::nullopt;
    }
    tuple.DeserializeFrom(data_ + offset + sizeof(RID));
    return tuple;
  }

  auto AppendTuple(const Tuple &tuple) -> bool {
    size_t needed = sizeof(RID) + sizeof(uint32_t) + tuple.GetLength();
    if (size_ + needed > INTERMEDIATE_RESULT_PAGE_DATA_SIZE) {
      return false;
    }
    auto rid = tuple.GetRid();
    memcpy(data_ + size_, &rid, sizeof(RID));
    tuple.SerializeTo(data_ + size_ + sizeof(RID));
    /* printf("Appended tuple at address %p with RID(page_id=%d, slot_num=%d), length=%u\n", data_ + size_, rid.GetPageId(),
           rid.GetSlotNum(), tuple.GetLength()); */
    size_ += needed;
    return true;
  }

 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  size_t size_;

  char data_[INTERMEDIATE_RESULT_PAGE_DATA_SIZE];
};

}  // namespace bustub
