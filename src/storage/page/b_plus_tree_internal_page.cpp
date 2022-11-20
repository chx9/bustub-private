//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const page_id_t &page_id,
                                            const KeyComparator &comparator) {
  int sz = GetSize();
  int i;
  for (i = 1; i <= sz; i++) {
    if (comparator(KeyAt(i), key) > 0) {
      break;
    }
  }

  // insert
  for (int j = sz + 1; j > i; j--) {
    array_[j] = array_[j - 1];
  }
  array_[i] = {key, page_id};
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitInto(BPlusTreeInternalPage *new_internal_page_ptr) -> KeyType {
  int sz = GetSize();
  int mid = sz / 2 + 1;
  new_internal_page_ptr->SetValueAt(0, ValueAt(mid));
  for (int i = mid + 1; i <= sz; i++) {
    new_internal_page_ptr->array_[i - mid] = array_[i];
  }
  new_internal_page_ptr->IncreaseSize(sz - mid);
  SetSize(mid - 1);

  return KeyAt(mid);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetAdjacentBrother(const KeyType &key, bool &is_left,
                                                        const KeyComparator &comparator) -> std::pair<int, page_id_t> {
  int sz = GetSize();
  int i = sz;
  while (i > 0) {
    if (comparator(KeyAt(i), key) <= 0) {
      break;
    }
    i--;
  }

  // 即当前节点是通过valueAt(0)指向的，所以没有左边的兄弟，只有右边的兄弟
  if (i == 0) {
    is_left = false;
    return {i + 1, ValueAt(i + 1)};
  }

  // 那就是有左边节点，优先那左边节点，事实上，这里要返回一个key，如果brother在左边
  is_left = true;
  return {i, ValueAt(i - 1)};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
  int sz = GetSize();
  while (index < sz) {
    array_[index] = array_[index + 1];
    index++;
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFromLeft(BPlusTreeInternalPage *brother_page_ptr,
                                                   BPlusTreeInternalPage *parent_page_ptr, int index,

                                                   BufferPoolManager *buffer_pool_manager_) {
  int i = GetSize() + 1;
  while (i > 0) {
    array_[i] = array_[i - 1];
    i--;
  }
  SetKeyAt(1, parent_page_ptr->KeyAt(index));
  parent_page_ptr->SetKeyAt(index, brother_page_ptr->KeyAt(brother_page_ptr->GetSize()));
  SetValueAt(0, brother_page_ptr->ValueAt(brother_page_ptr->GetSize()));

  IncreaseSize(1);
  brother_page_ptr->IncreaseSize(-1);

  page_id_t child_page_id = ValueAt(0);
  auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
  auto child_page_ptr = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
  child_page_ptr->SetParentPageId(GetPageId());
  buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFromRight(BPlusTreeInternalPage *brother_page_ptr,
                                                    BPlusTreeInternalPage *parent_page_ptr, int index,
                                                    BufferPoolManager *buffer_pool_manager_) {
  SetKeyAt(GetSize() + 1, parent_page_ptr->KeyAt(index));
  parent_page_ptr->SetKeyAt(index, brother_page_ptr->KeyAt(1));
  SetValueAt(GetSize() + 1, brother_page_ptr->ValueAt(0));

  int i = 0;
  int brother_sz = brother_page_ptr->GetSize();
  while (i < brother_sz) {
    brother_page_ptr->array_[i] = brother_page_ptr->array_[i + 1];
    i++;
  }
  IncreaseSize(1);
  brother_page_ptr->IncreaseSize(-1);

  page_id_t child_page_id = ValueAt(GetSize());
  auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
  auto child_page_ptr = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
  child_page_ptr->SetParentPageId(GetPageId());
  buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ConcatWith(BPlusTreeInternalPage *brother_page_ptr, const KeyType &key,
                                                BufferPoolManager *buffer_pool_manager) {
  int sz = GetSize();
  brother_page_ptr->SetKeyAt(0, key);
  int internal_sz = brother_page_ptr->GetSize();
  for (int i = 0; i <= internal_sz; i++) {
    array_[i + sz + 1] = brother_page_ptr->array_[i];
    auto child_page = buffer_pool_manager->FetchPage(brother_page_ptr->ValueAt(i));
    auto child_page_ptr = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
    child_page_ptr->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_ptr->GetPageId(), true);
  }
  IncreaseSize(internal_sz + 1);
  brother_page_ptr->SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
