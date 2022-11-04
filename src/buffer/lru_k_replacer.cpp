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
FrameInfo::FrameInfo(size_t k, frame_id_t frame_id) : k_{k}, frame_id_{frame_id}, accesses_(0) {}
auto FrameInfo::HasK() -> bool { return accesses_.size() == k_; }
auto FrameInfo::IsEvictable() -> bool { return is_evictable_; }
auto FrameInfo::SetEvictable(bool set_evictable) -> void { is_evictable_ = set_evictable; }
auto FrameInfo::GetBack() -> size_t { return accesses_.back(); }
auto FrameInfo::PopFront() -> void { accesses_.pop_front(); }
auto FrameInfo::PushBack(size_t access) -> void { accesses_.push_back(access); }
auto FrameInfo::GetSize() -> size_t { return accesses_.size(); }
auto FrameInfo::GetId() -> frame_id_t { return frame_id_; }
auto FrameInfo::GetFront() -> size_t { return accesses_.front(); }
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_{num_frames}, k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_size_ == 0) {
    latch_.unlock();
    return false;
  }
  auto k_it_evict = cache_.end();
  auto none_k_it_evict = cache_.end();
  auto it = cache_.begin();
  while (it != cache_.end()) {
    if (!it->second.IsEvictable()) {
      it++;
      continue;
    }
    if (it->second.HasK()) {
      if (k_it_evict == cache_.end() || it->second.GetFront() < k_it_evict->second.GetFront()) {
        k_it_evict = it;
      }
    } else {
      if (none_k_it_evict == cache_.end() || it->second.GetFront() < none_k_it_evict->second.GetFront()) {
        none_k_it_evict = it;
      }
    }
    it++;
  }
  if (none_k_it_evict != cache_.end()) {
    *frame_id = none_k_it_evict->first;
    cache_.erase(none_k_it_evict);
    curr_size_--;
    latch_.unlock();
    return true;
  }
  if (k_it_evict != cache_.end()) {
    *frame_id = k_it_evict->first;
    cache_.erase(k_it_evict);
    curr_size_--;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "invalid frame id");
  if (cache_.find(frame_id) == cache_.end()) {
    cache_[frame_id] = FrameInfo(k_, frame_id);
  }
  auto &frame_info = cache_[frame_id];
  frame_info.PushBack(current_timestamp_);
  current_timestamp_++;
  if (frame_info.GetSize() > k_) {
    frame_info.PopFront();
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "invalid frame id");
  auto it = cache_.find(frame_id);
  if (it == cache_.end()) {
    latch_.unlock();
    return;
  }
  bool before = it->second.IsEvictable();
  if (!before && set_evictable) {
    curr_size_++;
  } else if (before && !set_evictable) {
    curr_size_--;
  }
  it->second.SetEvictable(set_evictable);
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  auto it = cache_.find(frame_id);
  if (it != cache_.end()) {
    BUSTUB_ASSERT(it->second.IsEvictable(), "Remove is called on a non-evictable frame");
    cache_.erase(it);
    curr_size_--;
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

// auto Compare(FrameInfo *f1, FrameInfo *f2) -> bool { return f1->GetBack() < f2->GetBack(); }
auto Compare::operator()(FrameInfo *f1, FrameInfo *f2) -> bool { return f1->GetFront() > f2->GetFront(); }
}  // namespace bustub
