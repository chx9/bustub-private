//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  rwlatch_.WLock();
  size_t mi_n1 = SIZE_MAX;
  size_t mi_n2 = SIZE_MAX;
  frame_id_t f1 = -1;
  frame_id_t f2 = -1;
  for (auto &it : evictable_frame_) {
    auto t_list = it.second;
    auto front = t_list.front();
    if (t_list.size() == k_ && front < mi_n1) {  // k historical references
      mi_n1 = front;
      f1 = it.first;
    } else if (t_list.size() != k_ && front < mi_n2) {  // less than k historical references
      mi_n2 = front;
      f2 = it.first;
    }
  }

  if (mi_n1 == SIZE_MAX && mi_n2 == SIZE_MAX) {
    rwlatch_.WUnlock();
    return false;
  }

  if (mi_n2 == SIZE_MAX) {
    *frame_id = f1;
  } else {
    *frame_id = f2;
  }

  evictable_frame_.erase(*frame_id);
  curr_size_--;
  rwlatch_.WUnlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  rwlatch_.WLock();
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "larger than replacer_size_");

  if (evictable_frame_.find(frame_id) != evictable_frame_.end()) {
    evictable_frame_[frame_id].emplace_back(current_timestamp_++);
    if (evictable_frame_[frame_id].size() > k_) {
      evictable_frame_[frame_id].pop_front();
    }
  } else if (non_evictable_frame_.find(frame_id) != non_evictable_frame_.end()) {
    non_evictable_frame_[frame_id].emplace_back(current_timestamp_++);
    if (non_evictable_frame_[frame_id].size() > k_) {
      non_evictable_frame_[frame_id].pop_front();
    }
  } else {
    non_evictable_frame_[frame_id] = std::list<size_t>(1, current_timestamp_++);
  }
  rwlatch_.WUnlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  rwlatch_.WLock();
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "larger than replacer_size_");

  if (evictable_frame_.find(frame_id) != evictable_frame_.end() && (!set_evictable)) {
    curr_size_--;
    non_evictable_frame_[frame_id] = evictable_frame_[frame_id];
    evictable_frame_.erase(frame_id);
  } else if (non_evictable_frame_.find(frame_id) != non_evictable_frame_.end() && (set_evictable)) {
    curr_size_++;
    evictable_frame_[frame_id] = non_evictable_frame_[frame_id];
    non_evictable_frame_.erase(frame_id);
  }
  rwlatch_.WUnlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  rwlatch_.WLock();
  BUSTUB_ASSERT(non_evictable_frame_.find(frame_id) == non_evictable_frame_.end(),
                "Remove is called on a non-evictable frame");
  if (evictable_frame_.find(frame_id) != evictable_frame_.end()) {
    evictable_frame_.erase(frame_id);
    curr_size_--;
  }
  rwlatch_.WUnlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
