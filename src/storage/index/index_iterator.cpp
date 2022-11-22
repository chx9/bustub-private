/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *buffer_pool_manager)
    : index_(index), page_id_(page_id), buffer_pool_manager_(buffer_pool_manager) {
  if (page_id_ != INVALID_PAGE_ID) {
    Page *leaf_page = buffer_pool_manager_->FetchPage(page_id_);
    leaf_page_ptr_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(leaf_page->GetData());
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_id_ != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool {
  return leaf_page_ptr_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page_ptr_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return leaf_page_ptr_->PairAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  if (index_ == leaf_page_ptr_->GetSize() && leaf_page_ptr_->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_ = leaf_page_ptr_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page_ptr_->GetPageId(), false);
    Page *leaf_page = buffer_pool_manager_->FetchPage(page_id_);
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
