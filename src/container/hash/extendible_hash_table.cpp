

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

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Find the value associated with the given key.
 *
 * Use IndexOf(key) to find the directory index the key hashes to.
 *
 * @param key The key to be searched.
 * @param[out] value The value associated with the key.
 * @return True if the key is found, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  rif_latch_.lock();
  size_t index = IndexOf(key);
  bool success = dir_[index]->Find(key, value);
  rif_latch_.unlock();
  return success;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Given the key, remove the corresponding key-value pair in the hash table.
 * Shrink & Combination is not required for this project
 * @param key The key to be deleted.
 * @return True if the key exists, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  rif_latch_.lock();
  size_t index = IndexOf(key);
  bool success = dir_[index]->Remove(key);
  rif_latch_.unlock();
  return success;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Insert the given key-value pair into the hash table.
 * If a key already exists, the value should be updated.
 * If the bucket is full and can't be inserted, do the following steps before retrying:
 *    1. If the local depth of the bucket is equal to the global depth,
 *        increment the global depth and double the size of the directory.
 *    2. Increment the local depth of the bucket.
 *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
 *
 * @param key The key to be inserted.
 * @param value The value to be inserted.
 */
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  rif_latch_.lock();
  size_t index = IndexOf(key);
  while (!(dir_[index]->Insert(key, value))) {
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
    index = IndexOf(key);
  }
  rif_latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Find the value associated with the given key in the bucket.
 * @param key The key to be searched.
 * @param[out] value The value associated with the key.
 * @return True if the key is found, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  latch_.lock();
  auto it = list_.begin();
  while (it != list_.end() && it->first != key) {
    it++;
  }
  if (it != list_.end()) {
    value = it->second;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Given the key, remove the corresponding key-value pair in the bucket.
 * @param key The key to be deleted.
 * @return True if the key exists, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  latch_.lock();
  auto it = list_.begin();
  while (it != list_.end() && it->first != key) {
    it++;
  }
  if (it == list_.end()) {
    latch_.unlock();
    return false;
  }
  list_.erase(it);
  latch_.unlock();
  return true;
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief Insert the given key-value pair into the bucket.
 *      1. If a key already exists, the value should be updated.
 *      2. If the bucket is full, do nothing and return false.
 * @param key The key to be inserted.
 * @param value The value to be inserted.
 * @return True if the key-value pair is inserted, false otherwise.
 */
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  latch_.lock();
  auto it = list_.begin();
  while (it != list_.end() && it->first != key) {
    it++;
  }
  if (it != list_.end()) {
    it->second = value;
    latch_.unlock();
    return true;
  }
  if (IsFull()) {
    latch_.unlock();
    return false;
  }
  list_.emplace_back(std::make_pair(key, value));
  latch_.unlock();
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
