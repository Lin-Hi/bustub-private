//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool has_find_value = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      has_find_value = true;
    }
  }
  return has_find_value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  int64_t available = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && array_[i].second == value) {
        return false;
      }
    } else if (available == -1) {
      available = i;
    }
  }

  if (available == -1) {
    return false;
  }

  array_[available] = MappingType(key, value);
  SetOccupied(available);
  SetReadable(available);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, KeyAt(i)) == 0 && value == ValueAt(i)) {
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint32_t num_index = bucket_idx / (8 * sizeof(char));
  uint32_t bit_index = bucket_idx % (8 * sizeof(char));
  uint8_t c = static_cast<uint8_t>(readable_[num_index]);
  c = c & (~(1 << bit_index));
  readable_[num_index] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t num_index = bucket_idx / (8 * sizeof(char));
  uint32_t bit_index = bucket_idx % (8 * sizeof(char));
  uint8_t num = static_cast<uint8_t>(occupied_[num_index]);
  return ((num >> bit_index) & static_cast<uint8_t>(1)) == 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t num_index = bucket_idx / (8 * sizeof(char));
  uint32_t bit_index = bucket_idx % (8 * sizeof(char));
  uint8_t c = static_cast<uint8_t>(occupied_[num_index]);
  c = c | (1 << bit_index);
  occupied_[num_index] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t num_index = bucket_idx / (8 * sizeof(char));
  uint32_t bit_index = bucket_idx % (8 * sizeof(char));
  uint8_t num = static_cast<uint8_t>(readable_[num_index]);
  //  return ((num >> bit_index) & 1) > 0;
  return (num & (1 << bit_index)) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t num_index = bucket_idx / (8 * sizeof(char));
  uint32_t bit_index = bucket_idx % (8 * sizeof(char));
  readable_[num_index] = static_cast<char>(readable_[num_index] | (1 << bit_index));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  uint8_t all_readable = 0b11111111;
  size_t quotient = BUCKET_ARRAY_SIZE / (8 * sizeof(char));
  size_t reminder = BUCKET_ARRAY_SIZE % (8 * sizeof(char));

  for (size_t i = 0; i < quotient; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    if (c != all_readable) {
      return false;
    }
  }

  uint8_t c = static_cast<uint8_t>(readable_[quotient]);
  for (size_t i = 0; i < reminder; i++) {
    if ((c & static_cast<uint8_t>(1)) != 1) {
      return false;
    }
    c = c >> 1;
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t ans = 0;
  size_t quotient = BUCKET_ARRAY_SIZE / (8 * sizeof(char));
  size_t reminder = BUCKET_ARRAY_SIZE % (8 * sizeof(char));

  for (size_t i = 0; i < quotient; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    for (ulong j = 0; j < (8 * sizeof(char)); j++) {
      if ((c & static_cast<uint8_t>(1)) == 1) {
        ans++;
      }
      c = c >> 1;
    }
  }

  uint8_t c = static_cast<uint8_t>(readable_[quotient]);
  for (ulong j = 0; j < reminder; j++) {
    if ((c & static_cast<uint8_t>(1)) == 1) {
      ans++;
    }
    c = c >> 1;
  }

  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  uint8_t all_empty = 0b0;
  size_t quotient = BUCKET_ARRAY_SIZE / (8 * sizeof(char));
  size_t reminder = BUCKET_ARRAY_SIZE % (8 * sizeof(char));

  for (size_t i = 0; i < quotient; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    if (c != all_empty) {
      return false;
    }
  }

  uint8_t c = static_cast<uint8_t>(readable_[quotient]);
  for (size_t i = 0; i < reminder; i++) {
    if ((c & static_cast<uint8_t>(1)) != 0) {
      return false;
    }
    c = c >> 1;
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetArrayCopy() -> MappingType * {
  uint32_t size = NumReadable();
  MappingType *new_array = new MappingType[size];
  for (uint32_t i = 0, j = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      new_array[j] = array_[i];
      j++;
    }
  }
  return new_array;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::Reset() {
  memset(array_, 0, sizeof(array_));
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
