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
  rw_latch_.WLock();
  if (curr_size_ == 0) {
    rw_latch_.WUnlock();
    return false;
  }
  std::priority_queue<FrameInfo *, std::vector<FrameInfo *>, Compare> k_evictables;
  std::priority_queue<FrameInfo *, std::vector<FrameInfo *>, Compare> none_k_evitables;
  for (auto &it : cache_) {
    if (!it.second.IsEvictable()) {
      continue;
    }
    if (it.second.HasK()) {
      k_evictables.push(&it.second);
    } else {
      none_k_evitables.push(&it.second);
    }
  }
  if (!none_k_evitables.empty()) {
    auto id = none_k_evitables.top()->GetId();
    *frame_id = id;
    cache_.erase(id);
    curr_size_--;
    rw_latch_.WUnlock();
    return true;
  }
  auto id = k_evictables.top()->GetId();
  *frame_id = id;
  cache_.erase(id);
  curr_size_--;
  rw_latch_.WUnlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  rw_latch_.WLock();
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
  rw_latch_.WUnlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  rw_latch_.WLock();
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "invalid frame id");
  auto it = cache_.find(frame_id);
  if (it == cache_.end()) {
    rw_latch_.WUnlock();
    return;
  }
  bool before = it->second.IsEvictable();
  if (!before && set_evictable) {
    curr_size_++;
  } else if (before && !set_evictable) {
    curr_size_--;
  }
  it->second.SetEvictable(set_evictable);
  rw_latch_.WUnlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  rw_latch_.WLock();
  auto it = cache_.find(frame_id);
  if (it != cache_.end()) {
    BUSTUB_ASSERT(it->second.IsEvictable(), "Remove is called on a non-evictable frame");
    cache_.erase(it);
    curr_size_--;
  }
  rw_latch_.WUnlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

// auto Compare(FrameInfo *f1, FrameInfo *f2) -> bool { return f1->GetBack() < f2->GetBack(); }
auto Compare::operator()(FrameInfo *f1, FrameInfo *f2) -> bool { return f1->GetFront() > f2->GetFront(); }
}  // namespace bustub
