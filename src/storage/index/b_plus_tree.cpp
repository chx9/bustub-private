#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
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
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (transaction == nullptr) {
    transaction = new Transaction(0);
  }
  auto leaf_page_ptr = FindLeafPage(key, false, transaction);
  if (leaf_page_ptr == nullptr) {
    return false;
  }
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool is_shared, Transaction *transaction) -> LeafPage * {
  LockRootPageId(is_shared);
  if (IsEmpty()) {
    return nullptr;
  }
  return nullptr;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
//   // 空的话返回false
//   if (IsEmpty()) {
//     return false;
//   }

//   // 根据key找到对应的leafpage
//   page_id_t page_id = root_page_id_;
//   // FetchPage + RLatch
//   Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
//   page_ptr->RLatch();
//   // 一开始没法判断是leaf还是internal,所以都转换成internal
//   auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());

//   // 如果是leaf,那就直接转换成leaf然后返回
//   while (!internal_page_ptr->IsLeafPage()) {
//     int i = 1;
//     int sz = internal_page_ptr->GetSize();
//     while (i <= sz && comparator_(internal_page_ptr->KeyAt(i), key) <= 0) {
//       i++;
//     }
//     page_id = internal_page_ptr->ValueAt(i - 1);
//     // RUnlatch + UnpinPage
//     page_ptr->RUnlatch();
//     buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
//     // FetchPage + RLatch
//     page_ptr = buffer_pool_manager_->FetchPage(page_id);
//     page_ptr->RLatch();

//     internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
//   }

//   auto leaf_page_ptr = reinterpret_cast<LeafPage *>(internal_page_ptr);
//   // 在leafpage中根据key找到value,O(n)复杂度,遍历完如果没找到,那就最后返回false
//   int sz = leaf_page_ptr->GetSize();
//   for (int i = 0; i < sz; i++) {
//     if (comparator_(leaf_page_ptr->KeyAt(i), key) == 0) {
//       result->emplace_back(leaf_page_ptr->ValueAt(i));
//       // RUnlatch + UnpinPage
//       page_ptr->RUnlatch();
//       buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
//       return true;
//     }
//   }
//   page_ptr->RUnlatch();
//   buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
//   return false;
// }
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Transaction *transaction) -> LeafPage * {
  // 因为前面判断了是否为空,所以root_page_id肯定是有值的,因此可以根据root_page_id_拿到page
  page_id_t page_id = root_page_id_;
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  // 一开始没法判断是leaf还是internal,所以都转换成internal
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());

  // 如果是leaf,那就直接转换成leaf然后返回
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
  // 如果为空，就要创建新的节点
  if (IsEmpty()) {
    // 从buffer_pool_manager_种new一个出来，并且对root_page_id_赋值
    Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    // 更新headerpage种的root_page_id
    UpdateRootPageId(1);
    // 当B+树是空的时候，生成的肯定是叶子节点，进行强制转换reinterpret_cast
    auto leaf_page_ptr = reinterpret_cast<LeafPage *>(root_page->GetData());
    // 初始化叶子节点，page_id是root_page_id_，parent_id为INVALID_PAGE_ID，max_size根据b+的成员来
    leaf_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    // 将key插入到这个leaf的第一个位置
    leaf_page_ptr->Insert(key, value, comparator_);
    // Unpin
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  // 如果不为空，就要去找到合适插入的位置
  // 因为前面判断了是否为空,所以root_page_id肯定是有值的,因此可以根据root_page_id_拿到page
  page_id_t page_id = root_page_id_;
  // FetchPage + WLatch + AddIntoPageSet
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  page_ptr->WLatch();
  transaction->AddIntoPageSet(page_ptr);

  // 一开始没法判断是leaf还是internal,所以都转换成internal
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());

  // 如果是leaf,那就直接转换成leaf然后返回
  while (!internal_page_ptr->IsLeafPage()) {
    int i = 1;
    int sz = internal_page_ptr->GetSize();
    while (i <= sz && comparator_(internal_page_ptr->KeyAt(i), key) <= 0) {
      i++;
    }
    page_id = internal_page_ptr->ValueAt(i - 1);

    // FetchPage + WLatch + AddIntoPageSet
    auto child_page_ptr = buffer_pool_manager_->FetchPage(page_id);
    child_page_ptr->WLatch();
    transaction->AddIntoPageSet(child_page_ptr);
    auto internal_child_page_ptr = reinterpret_cast<InternalPage *>(child_page_ptr->GetData());

    // if safe, then Pop + WUnlatch + UnpinPage
    if (internal_child_page_ptr->GetSize() < internal_child_page_ptr->GetMaxSize() - 1) {
      auto page_set = transaction->GetPageSet();
      while (page_set->size() > 1) {
        auto release_page_ptr = page_set->front();
        page_set->pop_front();
        release_page_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(release_page_ptr->GetPageId(), false);
      }
    }
    internal_page_ptr = internal_child_page_ptr;
    page_ptr = child_page_ptr;
  }

  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(internal_page_ptr);

  // 如果只有在key是重复的情况下，会返回false，因此，当返回false的时候插入失败，返回false
  if (!leaf_page_ptr->Insert(key, value, comparator_)) {
    // 释放WUnlatch所有的锁，pop掉transaction中的deque，并进行unpin
    auto page_set = transaction->GetPageSet();
    while (!page_set->empty()) {
      auto release_page_ptr = page_set->front();
      page_set->pop_front();
      release_page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(release_page_ptr->GetPageId(), false);
    }
    return false;
  }
  // 判断叶子节点是不是满了，满了的话就要分裂，然后将分裂出来的节点插入到父亲节点，如果没有父亲节点，就要创建一个父亲节点，然后将原来的leaf和新的leaf的page_id，都插进去。
  if (leaf_page_ptr->GetSize() == leaf_page_ptr->GetMaxSize()) {
    // 创建一个新的leaf，init
    page_id_t new_leaf_page_id;
    Page *new_leaf_page = buffer_pool_manager_->NewPage(&new_leaf_page_id);
    auto new_leaf_page_ptr = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
    new_leaf_page_ptr->Init(new_leaf_page_id, leaf_page_ptr->GetParentPageId(), leaf_max_size_);
    // 分裂叶子节点
    KeyType mid_key = leaf_page_ptr->SplitInto(new_leaf_page_ptr);

    // 如果当前叶子节点本身就是根节点， 那么就要创建父亲节点
    if (leaf_page_ptr->IsRootPage()) {
      // 此时，b+树的根节点的root_page_id_就转换成创建的新的internal的page_id
      page_id_t new_root_page_id;
      Page *root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
      // 这里创建的是一个父亲节点，那父亲节点一定是internalpage，因此强制转换过去
      auto root_page_ptr = reinterpret_cast<InternalPage *>(root_page->GetData());
      // 初始化，root_page_id，INVALID_PAGE_ID, internal_max_size_
      root_page_ptr->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      // 插入internal_page中,第0个位置的value是当前叶子节点的pageid,第一个位置的key是新分裂出来的leafpage的array的第一个key,第一个位置的value是新分裂的leafpage的page_id
      root_page_ptr->SetValueAt(0, leaf_page_ptr->GetPageId());
      root_page_ptr->SetKeyAt(1, mid_key);
      root_page_ptr->SetValueAt(1, new_leaf_page_ptr->GetPageId());
      root_page_ptr->IncreaseSize(1);
      // 当前leafpage的parent_page_id都是新创建的internal_page_id
      leaf_page_ptr->SetParentPageId(root_page_ptr->GetPageId());
      new_leaf_page_ptr->SetParentPageId(root_page_ptr->GetPageId());
      // 重置root_page_id
      root_page_id_ = new_root_page_id;
      // 然后更新headerpage中该b+树的page_id。
      UpdateRootPageId(0);
      // Pop + WUnlatch + UnpinPage
      transaction->GetPageSet()->pop_front();
      page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      // 新建的root和new_leaf因为是局部的,所以unpin掉
      buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(root_page_ptr->GetPageId(), true);

      return true;
    }
    // page_id_t leaf_page_parent_page_id = leaf_page_ptr->GetParentPageId();
    page_id_t new_leaf_page_page_id = new_leaf_page_ptr->GetPageId();

    // leaf插入成功了，也safe了，因此可以给leaf解锁了，也就是page_set的back
    auto page_set = transaction->GetPageSet();
    auto release_page_ptr = page_set->back();
    page_set->pop_back();
    release_page_ptr->WUnlatch();
    // release_page_ptr就是leaf_page_ptr
    buffer_pool_manager_->UnpinPage(release_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
    InsertIntoInternal(mid_key, new_leaf_page_page_id, transaction);
  } else {
    // 释放page_ptr的锁，pop掉page_ptr中的deque，并进行unpin
    transaction->GetPageSet()->pop_front();
    page_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternal(const KeyType &key, const page_id_t &value, Transaction *transaction) {
  // 获取要插入的parent_page，是一个internalpage
  auto internal_page = transaction->GetPageSet()->back();
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(internal_page->GetData());
  // 插入
  internal_page_ptr->Insert(key, value, comparator_);

  // 如果满了的话,就分裂.没有的话直接返回internal_page的page_id
  if (internal_page_ptr->GetSize() == internal_page_ptr->GetMaxSize()) {
    // 创建一个新的internal，init
    page_id_t new_internal_page_id;
    Page *new_internal_page = buffer_pool_manager_->NewPage(&new_internal_page_id);
    auto new_internal_page_ptr = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal_page_ptr->Init(new_internal_page_id, internal_page_ptr->GetParentPageId(), internal_max_size_);
    // 分裂internal节点，得到需要插入该internal父亲节点的key
    KeyType mid_key = internal_page_ptr->SplitInto(new_internal_page_ptr);

    // *****************更新new_internal_page_ptr的所有孩子节点的parent_page_id
    int sz = new_internal_page_ptr->GetSize();
    for (int i = 0; i <= sz; i++) {
      Page *child_page = buffer_pool_manager_->FetchPage(new_internal_page_ptr->ValueAt(i));
      child_page->WLatch();
      auto child_page_ptr = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_page_ptr->SetParentPageId(new_internal_page_ptr->GetPageId());
      // Unpin
      child_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);
    }
    // *****************

    // 如果当前internal节点本身是根节点， 那么就要创建父亲节点
    if (internal_page_ptr->IsRootPage()) {
      // 此时，b+树的根节点的root_page_id_就转换成创建的新的internal的page_id
      page_id_t new_root_page_id;
      Page *root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
      // 这里创建的是一个父亲节点，那父亲节点一定是internalpage，因此强制转换过去
      auto root_page_ptr = reinterpret_cast<InternalPage *>(root_page->GetData());
      // 初始化，root_page_id，INVALID_PAGE_ID, internal_max_size_
      root_page_ptr->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      // 插入internal_page中,第0个位置的value是当前internal节点的pageid,第一个位置的key是mid_key,第一个位置的value是新分裂的internal的page_id
      root_page_ptr->SetValueAt(0, internal_page_ptr->GetPageId());
      root_page_ptr->SetKeyAt(1, mid_key);
      root_page_ptr->SetValueAt(1, new_internal_page_ptr->GetPageId());
      root_page_ptr->IncreaseSize(1);
      // 当前leafpage的parent_page_id都是新创建的internal_page_id
      internal_page_ptr->SetParentPageId(root_page_ptr->GetPageId());
      new_internal_page_ptr->SetParentPageId(root_page_ptr->GetPageId());
      root_page_id_ = new_root_page_id;
      // 然后更新headerpage中该b+树的page_id。
      UpdateRootPageId(0);
      // Pop + WUnlatch + UnpinPage
      transaction->GetPageSet()->pop_front();
      internal_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
      // 新建的root和new_leaf因为是局部的,所以unpin掉
      buffer_pool_manager_->UnpinPage(new_internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(root_page_ptr->GetPageId(), true);
    } else {
      // 递归调用插入
      // page_id_t leaf_page_parent_page_id = internal_page_ptr->GetParentPageId();
      page_id_t new_internal_page_id = new_internal_page_ptr->GetPageId();

      // internal插入成功了，也safe了，因此可以给internal解锁了，也就是page_set的back
      auto page_set = transaction->GetPageSet();
      auto release_page_ptr = page_set->back();
      page_set->pop_back();
      release_page_ptr->WUnlatch();
      // release_page_ptr就是internal_page_ptr
      buffer_pool_manager_->UnpinPage(release_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_internal_page_ptr->GetPageId(), true);
      InsertIntoInternal(mid_key, new_internal_page_id, transaction);
    }
  } else {
    // Pop + WUnlatch + UnpinPage
    transaction->GetPageSet()->pop_front();
    internal_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
  }
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
INDEX_TEMPLATE_ARGUMENTS void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 空的话直接返回
  if (IsEmpty()) {
    return;
  }
  // 向下递归，找到对应leaf
  LeafPage *leaf_page_ptr = FindLeaf(key);
  if (!leaf_page_ptr->Remove(key, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
    return;
  }
  // 如果是根节点，leafpage
  if (leaf_page_ptr->IsRootPage()) {
    if (leaf_page_ptr->GetSize() == 0) {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    } else {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    return;
  }
  // 判断删掉对应key的leafpage的size是否小于一半
  if (leaf_page_ptr->GetSize() < leaf_page_ptr->GetMinSize()) {
    page_id_t parent_page_id = leaf_page_ptr->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_page_ptr = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // 遍历找到了key
    // brother_page_id, is_left, 自己(兄弟)在parent中夹着的key对应的index后续要用    ()V K V K V K V
    // 如果我是leaf：如果是偷，那么就是直接改，如果是合并，那么就是直接删除。
    // 如果是是internal：如果是偷，那么也需要改，如果是合并，那要把对应的index移到下面来
    page_id_t brother_page_id;
    bool is_left = true;
    int index;
    std::tie(index, brother_page_id) =
        parent_page_ptr->GetAdjacentBrother(leaf_page_ptr->KeyAt(0), is_left, comparator_);
    Page *brother_page = buffer_pool_manager_->FetchPage(brother_page_id);

    auto *brother_page_ptr = reinterpret_cast<LeafPage *>(brother_page->GetData());
    // 能Steal
    if (brother_page_ptr->GetSize() > brother_page_ptr->GetMinSize()) {
      leaf_page_ptr->StealFrom(brother_page_ptr, is_left);
      // 更新parent中对应index的key，根据is_left来传leaf或者brother的keyAt(0)
      parent_page_ptr->SetKeyAt(index, is_left ? leaf_page_ptr->KeyAt(0) : brother_page_ptr->KeyAt(0));

      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(brother_page_ptr->GetPageId(), true);
      return;
    }
    // 合并 leaf在右，brother在左
    if (!is_left) {
      std::swap(leaf_page_ptr, brother_page_ptr);
    }
    // index 删除
    parent_page_ptr->RemoveAt(index);
    brother_page_ptr->ConcatWith(leaf_page_ptr);
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
    buffer_pool_manager_->DeletePage(leaf_page_ptr->GetPageId());
    buffer_pool_manager_->UnpinPage(brother_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);

    CheckParent(parent_page_id);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CheckParent(page_id_t internal_page_id) {
  Page *internal_page = buffer_pool_manager_->FetchPage(internal_page_id);
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(internal_page->GetData());
  if (internal_page_ptr->IsRootPage()) {
    if (internal_page_ptr->GetSize() == 0) {
      page_id_t new_root_page_id = internal_page_ptr->ValueAt(0);
      Page *new_root_page = buffer_pool_manager_->FetchPage(new_root_page_id);
      auto new_root_page_ptr = reinterpret_cast<InternalPage *>(new_root_page->GetData());
      new_root_page_ptr->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);

      root_page_id_ = new_root_page_id;
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), false);
      buffer_pool_manager_->DeletePage(internal_page_ptr->GetPageId());
    } else {
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), false);
    }
    return;
  }

  if (internal_page_ptr->GetSize() < internal_page_ptr->GetMinSize()) {
    page_id_t parent_page_id = internal_page_ptr->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_page_ptr = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // 找到brother
    page_id_t brother_page_id;
    bool is_left = true;
    int index;
    std::tie(index, brother_page_id) =
        parent_page_ptr->GetAdjacentBrother(internal_page_ptr->KeyAt(1), is_left, comparator_);
    Page *brother_page = buffer_pool_manager_->FetchPage(brother_page_id);
    auto *brother_page_ptr = reinterpret_cast<InternalPage *>(brother_page->GetData());

    // brother很胖
    if (brother_page_ptr->GetSize() > brother_page_ptr->GetMinSize()) {
      if (is_left) {
        internal_page_ptr->StealFromLeft(brother_page_ptr, parent_page_ptr, index, buffer_pool_manager_);
      } else {
        internal_page_ptr->StealFromRight(brother_page_ptr, parent_page_ptr, index, buffer_pool_manager_);
      }

      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(brother_page_ptr->GetPageId(), true);
      return;
    }
    // 合并 leaf在右，brother在左
    if (!is_left) {
      std::swap(internal_page_ptr, brother_page_ptr);
    }

    // index 下移
    brother_page_ptr->ConcatWith(internal_page_ptr, parent_page_ptr->KeyAt(index), buffer_pool_manager_);
    parent_page_ptr->RemoveAt(index);
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
    buffer_pool_manager_->DeletePage(internal_page_ptr->GetPageId());
    buffer_pool_manager_->UnpinPage(brother_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);

    CheckParent(parent_page_id);
  } else {
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockRootPageId(bool is_shared) {
  if (is_shared) {
    latch_.RLock();
  } else {
    latch_.WUnlock();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockRootPageId(bool is_shared) {
  if (is_shared) {
    latch_.RUnlock();
  } else {
    latch_.WUnlock();
  }
}
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
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  // 一开始没法判断是leaf还是internal,所以都转换成internal
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());

  // 如果是leaf,那就直接转换成leaf然后返回
  while (!internal_page_ptr->IsLeafPage()) {
    page_id = internal_page_ptr->ValueAt(0);
    // UnpinPage
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  }
  page_id = internal_page_ptr->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);

  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  LeafPage *leaf_page_ptr = FindLeaf(key);
  int sz = leaf_page_ptr->GetSize();
  int i;
  for (i = 0; i < sz; i++) {
    if (comparator_(leaf_page_ptr->KeyAt(i), key) == 0) {
      break;
    }
  }
  page_id_t page_id = leaf_page_ptr->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);

  return INDEXITERATOR_TYPE(page_id, i, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  page_id_t page_id = root_page_id_;
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  // 一开始没法判断是leaf还是internal,所以都转换成internal
  auto internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());

  // 如果是leaf,那就直接转换成leaf然后返回
  while (!internal_page_ptr->IsLeafPage()) {
    page_id = internal_page_ptr->ValueAt(internal_page_ptr->GetSize());
    // UnpinPage
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    internal_page_ptr = reinterpret_cast<InternalPage *>(page_ptr->GetData());
  }
  page_id = internal_page_ptr->GetPageId();
  int sz = internal_page_ptr->GetSize();
  buffer_pool_manager_->UnpinPage(page_id, false);

  return INDEXITERATOR_TYPE(page_id, sz, buffer_pool_manager_);
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
