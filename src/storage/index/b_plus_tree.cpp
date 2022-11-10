#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
  page_id_t page_id = root_page_id_;
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  while (!internal_page_ptr->IsLeafPage()) {
    int i = 1;
    int sz = internal_page_ptr->GetSize();
    while (i <= sz && comparator_(internal_page_ptr->KeyAt(i), key) <= 0) {
      i++;
    }
    page_id = internal_page_ptr->ValueAt(i - 1);
    // UnpinPage
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  }
  return reinterpret_cast<LeafPage *>(internal_page_ptr);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  auto leaf_page_ptr = FindLeaf(key);
  int sz = leaf_page_ptr->GetSize();
  for (int i = 0; i < sz; i++) {
    if (comparator_(leaf_page_ptr->KeyAt(i), key) == 0) {
      result->emplace_back(leaf_page_ptr->ValueAt(i));
      buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_ptr;
  if (IsEmpty()) {
    // create leaf node, and set it to root;
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    UpdateRootPageId(1);
    leaf_page_ptr = reinterpret_cast<LeafPage *>(root_page->GetData());
    leaf_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf_page_ptr->InsertKeyValue(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  leaf_page_ptr = FindLeaf(key);
  // insert
  if (!leaf_page_ptr->InsertKeyValue(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
    return false;
  }
  if (leaf_page_ptr->GetSize() == leaf_page_ptr->GetMaxSize()) {
    // 1. split leaf;
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_leaf_page_ptr = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf_page_ptr->Init(new_page_id, leaf_page_ptr->GetParentPageId(), leaf_max_size_);
    auto mid_key = leaf_page_ptr->SplitInto(new_leaf_page_ptr);
    // 2. if root is leaf, split and update root page id;
    if (leaf_page_ptr->IsRootPage()) {
      // 2. create new internal page, set to root_page_id
      auto page = buffer_pool_manager_->NewPage(&root_page_id_);
      UpdateRootPageId(0);
      auto new_internal_page_ptr = reinterpret_cast<InternalPage *>(page->GetData());
      new_internal_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      //
      new_internal_page_ptr->SetValueAt(0, leaf_page_ptr->GetPageId());
      new_internal_page_ptr->SetKeyAt(1, mid_key);
      new_internal_page_ptr->SetValueAt(1, new_leaf_page_ptr->GetPageId());
      new_internal_page_ptr->IncreaseSize(1);
      //
      leaf_page_ptr->SetParentPageId(root_page_id_);
      new_leaf_page_ptr->SetParentPageId(root_page_id_);

      // unpin
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->UnpinPage(new_internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
      return true;
    }

    buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
    InsertIntoInternal(leaf_page_ptr->GetParentPageId(), mid_key, new_leaf_page_ptr->GetPageId());
    // unpin
  }
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternal(page_id_t parent_page_id, const KeyType &key, const page_id_t &value) {
  auto internal_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(internal_page->GetData());
  internal_page_ptr->InsertKeyValue(key, value, comparator_);

  if (internal_page_ptr->GetSize() == internal_page_ptr->GetMaxSize()) {
    // if full, split
    page_id_t new_internal_page_id;
    auto new_internal_page = buffer_pool_manager_->NewPage(&new_internal_page_id);
    auto new_internal_page_ptr = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal_page_ptr->Init(new_internal_page_id, internal_page_ptr->GetParentPageId(), internal_max_size_);
    auto mid_key = internal_page_ptr->SplitInto(new_internal_page_ptr);

    // redistribute new_internal;
    int sz = new_internal_page_ptr->GetSize();
    for (int i = 0; i <= sz; i++) {
      auto child_page = buffer_pool_manager_->FetchPage(new_internal_page_ptr->ValueAt(i));
      auto child_page_ptr = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_page_ptr->SetParentPageId(new_internal_page_ptr->GetPageId());
      buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
    }
    if (internal_page_ptr->IsRootPage()) {
      auto new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
      UpdateRootPageId(0);
      auto root_page_ptr = reinterpret_cast<InternalPage *>(new_root_page->GetData());
      root_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      root_page_ptr->SetValueAt(0, internal_page_ptr->GetPageId());
      root_page_ptr->SetKeyAt(1, mid_key);
      root_page_ptr->SetValueAt(1, new_internal_page_ptr->GetPageId());
      root_page_ptr->SetSize(1);
      internal_page_ptr->SetParentPageId(root_page_id_);
      new_internal_page_ptr->SetParentPageId(root_page_id_);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      return;
    }
    page_id_t leaf_page_parent_page_id = internal_page_ptr->GetParentPageId();
    page_id_t new_leaf_page_page_id = new_internal_page_ptr->GetPageId();
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_internal_page_ptr->GetPageId(), true);
    InsertIntoInternal(leaf_page_parent_page_id, mid_key, new_leaf_page_page_id);
  }
  buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  page_id_t page_id = root_page_id_;
  auto page_ptr = buffer_pool_manager_->FetchPage(page_id);
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  while (!internal_page_ptr->IsLeafPage()) {
    page_id = internal_page_ptr->ValueAt(0);
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), false);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  }
  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf_page_ptr = FindLeaf(key);
  int index = 0;
  while (comparator_(leaf_page_ptr->KeyAt(index), key) != 0) {
    index++;
  }
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
  return INDEXITERATOR_TYPE(leaf_page_ptr->GetPageId(), index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  page_id_t page_id = root_page_id_;
  auto page_ptr = buffer_pool_manager_->FetchPage(page_id);
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  while (!internal_page_ptr->IsLeafPage()) {
    page_id = internal_page_ptr->ValueAt(internal_page_ptr->GetSize());
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), false);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  }
  return INDEXITERATOR_TYPE(page_id, internal_page_ptr->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
