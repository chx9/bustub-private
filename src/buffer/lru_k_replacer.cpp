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
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_{num_frames}, k_(k) {}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (curr_size_ == 0) {
    return false;
  };
  std::vector<FrameInfo> k_evictables;
  std::vector<FrameInfo> none_k_evictables;
  for (auto &it : cache_) {
    if (!it.second.IsEvictable()) {
      continue;
    }
    if (it.second.HasK()) {
      k_evictables.push_back(it.second);
    } else {
      none_k_evictables.push_back(it.second);
    }
  }
  // LOG_INFO("%d %d\n", (int)k_evictables.size(), (int)none_k_evictables.size());
  if (!none_k_evictables.empty()) {
    std::sort(none_k_evictables.begin(), none_k_evictables.end(), Compare);
    for(size_t i=0;i<none_k_evictables.size();i++){
    }
    cache_.erase(none_k_evictables[0].GetId());
    *frame_id = none_k_evictables[0].GetId();
    curr_size_--;
    return true;
  }
  std::sort(k_evictables.begin(), k_evictables.end(), Compare);
  cache_.erase(k_evictables[0].GetId());
  *frame_id = k_evictables[0].GetId();
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (cache_.find(frame_id) == cache_.end()) {
    cache_[frame_id] = FrameInfo(k_, frame_id);
  }
  auto& frame_info = cache_[frame_id];
  frame_info.PushBack(current_timestamp_);
  current_timestamp_++;
  if (frame_info.GetSize() > k_) {
    frame_info.PopFront();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  bool before = cache_[frame_id].IsEvictable();
  if(!before && set_evictable){
    curr_size_++;
  }else if(before && !set_evictable){
    curr_size_--;
  }
  cache_[frame_id].SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) { 
    auto it = cache_.find(frame_id);
    if(it!=cache_.end()){
        cache_.erase(it); 
        curr_size_--;
    }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }


auto Compare(FrameInfo f1, FrameInfo f2) -> bool {return f1.GetBack() < f2.GetBack();}
}  // namespace bustub
