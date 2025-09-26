// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

#include <chrono>
#include <algorithm>

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> { 

  std::unique_lock<std::mutex> lock(latch_);
  std::optional<frame_id_t> evict_frame;

  if (Size() == 0) {
    return evict_frame;
  }  

  if ((mru_.size() < mru_target_size_ && !mru_.empty()) || mfu_.empty()) {
    // evict from the most frequently used list.
    evict_frame = EvictFrom(ArcStatus::MFU);
    if (!evict_frame.has_value()) {
      evict_frame = EvictFrom(ArcStatus::MRU);
    }
  } 
  else {
    // evict from the most recently used list.
    evict_frame = EvictFrom(ArcStatus::MRU);
    if (!evict_frame.has_value()) {
      evict_frame = EvictFrom(ArcStatus::MFU);
    }
  }

  return evict_frame;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {

  // auto start = std::chrono::high_resolution_clock::now();

  std::unique_lock<std::mutex> lock(latch_);
  std::shared_ptr<FrameStatus> frame;

  if (auto it = alive_map_.find(frame_id); it != alive_map_.end()) {
    // page in the alive list, cache hit
    auto &frame = it->second;

    if (frame->arc_status_ == ArcStatus::MRU) {
      frame->arc_status_ = ArcStatus::MFU;
      auto pit = mru_pos_.find(frame_id);
      mru_.erase(pit->second);
    } 
    else {
      auto pit = mfu_pos_.find(frame_id);
      mfu_.erase(pit->second);
    }

    mfu_.push_back(frame_id);
    mfu_pos_[frame_id] = std::prev(mfu_.end());
    return;
  }
  if (auto it = ghost_map_.find(page_id); it != ghost_map_.end()) {
    // page in the ghost list
    auto frame = std::move(it->second);
    ghost_map_.erase(page_id);

    if (frame->arc_status_ == ArcStatus::MRU_GHOST) {
      // increase mru target size
      size_t increase = mru_ghost_.size() >= mfu_ghost_.size() ? 1 : mfu_ghost_.size() / mru_ghost_.size();
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + increase);
      // erase page in mru_ghost_
      auto pos = mru_ghost_pos_.find(page_id);
      mru_ghost_.erase(pos->second);
      mru_ghost_pos_.erase(pos);
    } 
    else {
      // decrease mfu target size
      size_t decrease = mfu_ghost_.size() >= mru_ghost_.size() ? 1 : mru_ghost_.size() / mfu_ghost_.size();
      mru_target_size_ = (mru_target_size_ > decrease) ? (mru_target_size_ - decrease) : 0;
      // erase page in mfu_ghost_
      auto pos = mfu_ghost_pos_.find(page_id);
      mfu_ghost_.erase(pos->second);
      mfu_ghost_pos_.erase(pos);
    }
    // add page to mfu
    mfu_.push_back(frame_id);
    mfu_pos_[frame_id] = std::prev(mfu_.end());

    frame->arc_status_ = ArcStatus::MFU;
    frame->frame_id_ = frame_id;
    frame->evictable_ = false;
    alive_map_[frame_id] = frame;
    return;
  }
  // page isn't in the replacer
  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    // evict from mru_ghost_
    EvictFromGhost(ArcStatus::MRU_GHOST);
  }
  else if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
    // evict from mfu_ghost_
    EvictFromGhost(ArcStatus::MFU_GHOST);
  } 
  // add page to the back of mru_
  mru_.push_back(frame_id);
  mru_pos_[frame_id] = std::prev(mru_.end());
  alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

  // auto start = std::chrono::high_resolution_clock::now();
  std::unique_lock<std::mutex> lock(latch_);

  if (auto it = alive_map_.find(frame_id); it != alive_map_.end()) {

    auto frame = it->second;
    if (frame->evictable_ != set_evictable) {
      if (set_evictable) {
          curr_size_++;
      } else {
          curr_size_--;
      }
      frame->evictable_ = set_evictable;
    }
  }
  else {
    throw std::invalid_argument("Frame not found");
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {

  std::unique_lock<std::mutex> lock(latch_);

  if (alive_map_.find(frame_id) != alive_map_.end()) {
    auto frame = alive_map_[frame_id];

    if (frame->evictable_) {
      if (frame->arc_status_ == ArcStatus::MRU) {
        frame->arc_status_ = ArcStatus::MRU_GHOST;
        auto it = mru_pos_.find(frame_id);
        mru_.erase(it->second);
        mru_ghost_.push_back(frame->page_id_);
        mru_ghost_pos_[frame->page_id_] = std::prev(mru_ghost_.end());
      } 
      else {
        frame->arc_status_ = ArcStatus::MFU_GHOST;
        auto it = mfu_pos_.find(frame_id);
        mfu_.erase(it->second);
        mfu_ghost_.push_back(frame->page_id_);
        mfu_ghost_pos_[frame->page_id_] = std::prev(mfu_ghost_.end());
      }

      alive_map_.erase(frame_id);
      ghost_map_[frame->page_id_] = frame;
      curr_size_--;
    } 
    else {
      throw std::invalid_argument("frame not valid");
    }
  } 
  else {
    throw std::invalid_argument("frame not found");
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t { return curr_size_; }

auto ArcReplacer::EvictFrom(ArcStatus type) -> std::optional<frame_id_t> {

  page_id_t evt_page_id;
  frame_id_t evt_frame_id;
  std::shared_ptr<FrameStatus> evt_frame;

  if (type != ArcStatus::MRU && type != ArcStatus::MFU) {
    std::cerr << "EvictFromGhost: error type" << std::endl;
    std::abort();
  }

  auto &list = (type == ArcStatus::MRU) ? mru_ : mfu_;
  auto &list_pos = (type == ArcStatus::MRU) ? mru_pos_ : mfu_pos_;
  auto &ghost_list = (type == ArcStatus::MRU) ? mru_ghost_ : mfu_ghost_;
  auto &ghost_list_pos = (type == ArcStatus::MRU) ? mru_ghost_pos_ : mfu_ghost_pos_;

  auto it = std::find_if(list.begin(), list.end(), [this](frame_id_t id) {
    return alive_map_[id]->evictable_;
  });

  if (it == list.end()) {
    return std::nullopt;
  }

  evt_frame_id = *it;
  evt_frame = alive_map_[evt_frame_id];
  alive_map_.erase(evt_frame_id);
  list.erase(it);
  list_pos.erase(evt_frame_id);
  
  evt_frame->evictable_ = false;
  evt_page_id = evt_frame->page_id_;
  evt_frame->arc_status_ = (type == ArcStatus::MRU) ? ArcStatus::MRU_GHOST : ArcStatus::MFU_GHOST;
  // update the ghost_list and ghost_map_
  ghost_list.push_back(evt_page_id);
  ghost_list_pos[evt_page_id] = std::prev(ghost_list.end());
  ghost_map_[evt_page_id] = evt_frame;
  curr_size_--;
  return evt_frame_id;
}

auto ArcReplacer::EvictFromGhost(ArcStatus type) -> bool {
  page_id_t evt_page_id;

  if (type != ArcStatus::MRU_GHOST && type != ArcStatus::MFU_GHOST) {
    std::cerr << "EvictFromGhost: error type" << std::endl;
    std::abort();
  }

  if (type == ArcStatus::MRU_GHOST) {
    if (mru_ghost_.empty()) {
      return false;
    }
    evt_page_id = mru_ghost_.front();
    mru_ghost_.pop_front();
    ghost_map_.erase(evt_page_id); 
    return true;
  } else {
    if (mfu_ghost_.empty()) {
      return false;
    }
    evt_page_id = mfu_ghost_.front();
    mfu_ghost_.pop_front();
    ghost_map_.erase(evt_page_id);
    return true;
  }
}

}  // namespace bustub