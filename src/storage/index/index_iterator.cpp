/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *buffer_pool_manager)
    : page_id_(page_id), index_(index), buffer_pool_manager_(buffer_pool_manager) {
  auto leaf_page = buffer_pool_manager_->FetchPage(page_id_);
  leaf_page_ptr_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { buffer_pool_manager_->UnpinPage(page_id_, false); }  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_ptr_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page_ptr_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_ptr_->PairAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  int sz = leaf_page_ptr_->GetSize();
  index_++;
  if (index_ == sz) {
    if (leaf_page_ptr_->GetNextPageId() == INVALID_PAGE_ID) {
      return *this;
    }
    page_id_ = leaf_page_ptr_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page_ptr_->GetNextPageId(), false);
    auto leaf_page = buffer_pool_manager_->FetchPage(page_id_);
    leaf_page_ptr_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
