//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // from free list, always find from the free list first
    frame_id = free_list_.back();
    free_list_.pop_back();

    // replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    auto new_page_id = AllocatePage();
    *page_id = new_page_id;

    // page_table
    page_table_->Insert(new_page_id, frame_id);
    // reset set meta data
    pages_[frame_id].ResetMemory();
    pages_[frame_id].pin_count_++;
    pages_[frame_id].page_id_ = new_page_id;
    return &pages_[frame_id];
  }

  bool success = replacer_->Evict(&frame_id);
  if (success) {
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].is_dirty_) {
      // if has a dirty page, write it back to the disk
      FlushPgImp(evict_page_id);
    }
    // reset the memory and meta data for the new page
    pages_[frame_id].ResetMemory();
    // page_table
    page_table_->Remove(evict_page_id);
    // replacer
    replacer_->Remove(frame_id);

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    auto new_page_id = AllocatePage();
    // page table
    page_table_->Insert(new_page_id, frame_id);

    pages_[frame_id].page_id_ = new_page_id;
    pages_[frame_id].pin_count_++;

    *page_id = new_page_id;
    return &pages_[frame_id];
  }
  // set page_id to null if all frames currently in use and not evictable
  page_id = nullptr;
  return nullptr;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
    frame_id_t frame_id;
    bool success = page_table_->Find(page_id, frame_id);
    if(success){
      return &pages_[frame_id];
    }

    // from free list 
    if(!free_list_.empty()){
      frame_id = free_list_.back();
      free_list_.pop_back();

      // replacer
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);

      // page_table
      page_table_->Insert(page_id, frame_id);

      // reset meta data
      pages_[frame_id].ResetMemory();

      // read and copy data
      char* page_data = nullptr;
      disk_manager_->ReadPage(page_id, page_data);
      memcpy(pages_[frame_id].data_, page_data, BUSTUB_PAGE_SIZE);

      pages_[frame_id].pin_count_++;
      pages_[frame_id].page_id_ = page_id;

      return &pages_[frame_id];
    }
    success = replacer_->Evict(&frame_id);
    if(success){
      page_id_t evict_page_id = pages_[frame_id].GetPageId();
      if(pages_[frame_id].is_dirty_){
        // if has a dirty page, write it back to the disk
        FlushPgImp(evict_page_id);
      }
      // reset the memory and meta data for the new page
      pages_[frame_id].ResetMemory();
      // page_table
      page_table_->Remove(evict_page_id);
      // replacer
      replacer_->Remove(frame_id);

      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      page_table_->Insert(page_id, frame_id);
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].pin_count_++;

      // read and copy data
      char* page_data = nullptr;
      disk_manager_->ReadPage(page_id, page_data);
      memcpy(pages_[frame_id].data_, page_data, BUSTUB_PAGE_SIZE);
      return &pages_[frame_id];
    }
    return nullptr;
 }

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  bool success = page_table_->Find(page_id, frame_id);
  // If page_id is not in the buffer pool or its pin count is already 0, return false
  if (!success || pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  // Decrement the pin count of a page.
  pages_[frame_id].pin_count_--;
  // If the pin count reaches 0, the frame should be evictable by the replacer.
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Evict(&frame_id);
  }
  // Also, set the dirty flag on the page to indicate if the page was modified.
  pages_[frame_id].is_dirty_ = true;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id;
  bool success = page_table_->Find(page_id, frame_id);
  if (!success) {
    return false;
  }
  // Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  // Unset the dirty flag of the page after flushing.
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    FlushPgImp(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  bool success = page_table_->Find(page_id, frame_id);
  // If page_id is not in the buffer pool, do nothing and return true
  if (!success) {
    return true;
  }
  // If the page is pinned and cannot be deleted, return false immediately
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  // delete from the page table;
  page_table_->Remove(page_id);
  // stop tracking the frame in the replacer
  replacer_->Remove(frame_id);
  // add the frame back to the free list.
  free_list_.emplace_back(frame_id);
  // Finally, you should call DeallocatePage() to imitate freeing the page on the disk.
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
