//===----------------------------------------------------------------------===//
//
//                    BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  HashTableDirectoryPage *dir_page;
  directory_latch_.lock();

  // if directory page is not created yet
  if (directory_page_id_ == INVALID_PAGE_ID) {
    // create a new directory page
    page_id_t new_page_id_directory;
    Page *page_directory = buffer_pool_manager_->NewPage(&new_page_id_directory);
    assert(page_directory != nullptr);
    directory_page_id_ = new_page_id_directory;
    dir_page = reinterpret_cast<HashTableDirectoryPage *>(page_directory->GetData());
    dir_page->SetPageId(new_page_id_directory);

    // create a new bucket page and put into directory page
    page_id_t new_page_id_bucket;
    Page *page_bucket = buffer_pool_manager_->NewPage(&new_page_id_bucket);
    assert(page_bucket != nullptr);
    dir_page->SetBucketPageId(0, new_page_id_bucket);

    // unpin those two pages in buffer pool manager
    assert(buffer_pool_manager_->UnpinPage(new_page_id_directory, true));
    assert(buffer_pool_manager_->UnpinPage(new_page_id_bucket, true));
  }
  directory_latch_.unlock();

  // if directory page already exists
  assert(directory_page_id_ != INVALID_PAGE_ID);
  Page *page_directory = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page_directory != nullptr);
  dir_page = reinterpret_cast<HashTableDirectoryPage *>(page_directory->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> Page * {
  Page *page_bucket = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(page_bucket != nullptr);
  return page_bucket;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchHashBucketPage(Page *page) -> HASH_TABLE_BUCKET_TYPE * {
  assert(page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchHashBucketPage(bucket_page);

  bool has_get_value = bucket->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();
  return has_get_value;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchHashBucketPage(bucket_page);

  if (!bucket->IsFull()) {
    bool has_insert = bucket->Insert(key, value, comparator_);
    bucket_page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return has_insert;
  }

  bucket_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  if (split_bucket_depth >= MAX_BUCKET_DEPTH) {
    //    LOG_DEBUG("Directory page slots are full, can not split more bucket page.")
    assert((buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false)));
    table_latch_.WUnlock();
    return false;
  }

  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  dir_page->IncrLocalDepth(split_bucket_index);

  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  Page *split_bucket_page = FetchBucketPage(split_bucket_page_id);
  split_bucket_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *split_bucket = FetchHashBucketPage(split_bucket_page);
  uint32_t pre_array_size = split_bucket->NumReadable();
  MappingType *pre_array = split_bucket->GetArrayCopy();
  split_bucket->Reset();

  page_id_t new_bucket_page_id;
  Page *new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  assert(new_bucket_page != nullptr);
  new_bucket_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *new_bucket = FetchHashBucketPage(new_bucket_page);
  uint32_t new_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);
  dir_page->SetLocalDepth(new_bucket_index, dir_page->GetGlobalDepth());
  dir_page->SetBucketPageId(new_bucket_index, new_bucket_page_id);

  for (uint32_t i = 0; i < pre_array_size; i++) {
    uint32_t target_bucket_index = Hash(pre_array[i].first) & dir_page->GetLocalDepthMask(split_bucket_index);
    page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
    assert(target_bucket_page_id == split_bucket_page_id || target_bucket_page_id == new_bucket_page_id);
    if (target_bucket_page_id == split_bucket_page_id) {
      assert(split_bucket->Insert(pre_array[i].first, pre_array[i].second, comparator_));
    } else {
      assert(new_bucket->Insert(pre_array[i].first, pre_array[i].second, comparator_));
    }
  }
  delete[] pre_array;

  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= diff; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, split_bucket_page_id);
  }
  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, split_bucket_page_id);
  }
  for (uint32_t i = new_bucket_index; i >= diff; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(new_bucket_index));
    dir_page->SetBucketPageId(i, new_bucket_page_id);
  }
  for (uint32_t i = new_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(new_bucket_index));
    dir_page->SetBucketPageId(i, new_bucket_page_id);
  }

  new_bucket_page->WUnlatch();
  split_bucket_page->WUnlatch();

  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchHashBucketPage(bucket_page);

  bool has_remove = bucket->Remove(key, value, comparator_);
  if (!has_remove) {
    bucket_page->WUnlatch();
    table_latch_.WUnlock();
    return has_remove;
  }

  bool bucket_is_empty = bucket->IsEmpty();

  bucket_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  table_latch_.WUnlock();
  if (bucket_is_empty) {
    Merge(transaction, bucket_index);
  }
  return has_remove;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  auto dir_page_data = FetchDirectoryPage();

  for (uint32_t i = 0;; i++) {
    // 目录的大小可能会变小，所以每次都要判断bucket是不是超过了
    if (i >= dir_page_data->Size()) {
      break;
    }
    auto old_local_depth = dir_page_data->GetLocalDepth(i);
    auto bucket_page_id = dir_page_data->GetBucketPageId(i);
    auto bucket_page = FetchBucketPage(bucket_page_id);
    auto bucket_page_data = FetchHashBucketPage(bucket_page);
    bucket_page->RLatch();

    if (old_local_depth > 1 && bucket_page_data->IsEmpty()) {  // 原bucket空了
      auto split_bucket_idx = dir_page_data->GetSplitImageIndex(i);
      if (dir_page_data->GetLocalDepth(split_bucket_idx) == old_local_depth) {
        dir_page_data->DecrLocalDepth(i);
        dir_page_data->DecrLocalDepth(split_bucket_idx);
        dir_page_data->SetBucketPageId(
            i, dir_page_data->GetBucketPageId(split_bucket_idx));  // 将分割bucket的pageID赋给原bucket
        auto new_bucket_page_id = dir_page_data->GetBucketPageId(i);

        for (uint32_t j = 0; j < dir_page_data->Size();
             ++j) {  // 遍历找到原pageID和分割pageID对应的bucketIdx，将他们的pageID都设置为分割pageID
          if (j == i || j == split_bucket_idx) {
            continue;
          }
          auto cur_bucket_page_id = dir_page_data->GetBucketPageId(j);
          if (cur_bucket_page_id == bucket_page_id || cur_bucket_page_id == new_bucket_page_id) {
            dir_page_data->SetLocalDepth(j, dir_page_data->GetLocalDepth(i));
            dir_page_data->SetBucketPageId(j, new_bucket_page_id);
          }
        }
        // 这样操作完后，无论分割bucket还是原bucket，它们的page都是分割后的page
      }
      if (dir_page_data->CanShrink()) {
        dir_page_data->DecrGlobalDepth();
      }
    }
    bucket_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);

  table_latch_.WUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, uint32_t bucket_index) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  uint32_t split_bucket_index = dir_page->GetSplitImageIndex(bucket_index);

  uint32_t bucket_local_depth = dir_page->GetLocalDepth(bucket_index);
  if (bucket_local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  uint32_t split_bucket_local_depth = dir_page->GetLocalDepth(split_bucket_index);
  if (bucket_local_depth != split_bucket_local_depth) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  Page *bucket_page = FetchBucketPage(bucket_index);
  bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchHashBucketPage(bucket_page);
  if (!bucket->IsEmpty()) {
    bucket_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  bucket_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(bucket_page_id));

  page_id_t split_bucket_page_id = dir_page->GetBucketPageId(split_bucket_index);
  dir_page->SetBucketPageId(bucket_index, split_bucket_page_id);
  dir_page->DecrLocalDepth(bucket_index);
  dir_page->DecrLocalDepth(split_bucket_index);
  assert(dir_page->GetLocalDepth(bucket_index) == dir_page->GetLocalDepth(split_bucket_index));

  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t i_bucket_page_id = dir_page->GetBucketPageId(i);
    if (i_bucket_page_id == bucket_page_id || i_bucket_page_id == split_bucket_page_id) {
      dir_page->SetBucketPageId(i, split_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    }
  }

  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
