//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/extendible_hash_table.h"
#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  size_t index = IndexOf(key);
  if (!(dir_[index]->Insert(key, value))) {
    if (GetLocalDepth(index) == GetGlobalDepth()) {
      // 1.1. If the local depth of the bucket is equal to the global depth,
      // increment the global depth
      global_depth_++;
      // 1.2 double the size of the directory.
      size_t ds = dir_.size();
      for (size_t i = 0; i < ds; i++) {
        dir_.emplace_back(std::shared_ptr<Bucket>(dir_[i]));
      }
    }
    auto cur_bucket = dir_[index];
    //  2. Increment the local depth of the bucket.
    dir_[index]->IncrementDepth();
    // 3. split
    int local_depth = dir_[index]->GetDepth();
    int mask = (1 << (local_depth - 1));
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, local_depth);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, local_depth);
    auto items = dir_[index]->GetItems();
    for (auto const &item : items) {
      auto hash_key = std::hash<K>{}(item.first);
      if (hash_key & mask) {
        one_bucket->Insert(item.first, item.second);
      } else {
        zero_bucket->Insert(item.first, item.second);
      }
    }
    auto sz = dir_.size();
    for (size_t i = 0; i < sz; i++) {
      if (dir_[i] != cur_bucket) {
        continue;
      }
      if (static_cast<bool>(i & mask)) {
        dir_[i] = one_bucket;
      } else {
        dir_[i] = zero_bucket;
      }
    }
    num_buckets_++;
    Insert(key, value);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = list_.begin();
  while (it != list_.end() && it->first != key) {
    it++;
  }
  if (it != list_.end()) {
    value = it->second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  while (it != list_.end() && it->first != key) {
    it++;
  }
  if (it == list_.end()) {
    return false;
  }
  list_.erase(it);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = list_.begin();
  while (it != list_.end() && it->first != key) {
    it++;
  }
  if (it != list_.end()) {
    it->second = value;
    return true;
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
