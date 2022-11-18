//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/hash_table_page_defs.h"
namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  return array_[index].second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PairAt(int index) -> const MappingType & { return array_[index]; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) -> void { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> bool {
  int sz = GetSize();
  // find the right position
  int index = 0;
  while (index < sz && comparator(KeyAt(index), key) < 0) {
    index++;
  }
  // duplicate key not allowed
  if (index != sz && comparator(KeyAt(index), key) == 0) {
    return false;
  }
  // insertion
  for (int i = sz; i > index; i--) {
    // move(i, i-1);
    array_[i] = array_[i - 1];
  }
  array_[index] = {key, value};
  IncreaseSize(1);
  return true;
  //
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitInto(BPlusTreeLeafPage *new_leaf_page_ptr) -> KeyType {
  int sz = GetSize();
  int pos = sz / 2;
  for (int i = sz / 2; i < sz; i++) {
    new_leaf_page_ptr->array_[i - pos] = array_[i];
  }
  //
  new_leaf_page_ptr->SetSize((sz + 1) / 2);
  SetSize(sz / 2);
  //
  new_leaf_page_ptr->SetNextPageId(GetNextPageId());
  SetNextPageId(new_leaf_page_ptr->GetPageId());
  return new_leaf_page_ptr->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  int sz = GetSize();
  for (int i = 0; i < sz; i++) {
    if (comparator(key, KeyAt(i)) == 0) {
      // delete this key;
      for (int j = i; j < sz - 1; j++) {
        array_[j] = array_[j + 1];
      }
      IncreaseSize(-1);
      return true;
    }
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::StealFrom(BPlusTreeLeafPage *brother_page_ptr, bool &is_left) {
  if (is_left) {
    auto stolen_item = brother_page_ptr->array_[brother_page_ptr->GetSize() - 1];
    IncreaseSize(-1);
    int i = GetSize();
    while (i > 0) {
      array_[i] = array_[i - 1];
      i--;
    }
    array_[0] = stolen_item;
    IncreaseSize(1);
  } else {
    // right
    auto stolen_item = brother_page_ptr->array_[0];
    IncreaseSize(-1);
    int sz = brother_page_ptr->GetSize();
    int i = 0;
    while (i < sz) {
      brother_page_ptr->array_[i] = brother_page_ptr->array_[i + 1];
      i++;
    }
    array_[GetSize()] = stolen_item;
    IncreaseSize(1);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ConcatWith(BPlusTreeLeafPage *leaf_page_ptr) {
  int sz = GetSize();
  int leaf_size = leaf_page_ptr->GetSize();
  for (int i = 0; i < leaf_size; i++) {
    array_[i + sz] = leaf_page_ptr->array_[i];
  }
  IncreaseSize(leaf_size);

  leaf_page_ptr->SetSize(0);
  SetNextPageId(leaf_page_ptr->GetNextPageId());
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
