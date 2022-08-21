//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <typeinfo>

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (list_.empty()) {
    //    LOG_DEBUG("There's no frame in LRUReplacer when victimized.");
    return false;
  }

  if (typeid(*frame_id) != typeid(frame_id_t)) {
    //    LOG_DEBUG("Pointed variable of input pointer is not frame_id_t type.");
    return false;
  }
  if (INT32_MIN > *frame_id || *frame_id > INT32_MAX) {
    //    LOG_DEBUG("Pointed variable of input pointer is out of int32_t boundary.");
    return false;
  }

  *frame_id = list_.back();
  list_.pop_back();
  map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (map_.count(frame_id) == 0) {
    //    LOG_DEBUG("Pinned frame is not in LRUReplacer.");
    return;
  }

  list_.erase(map_[frame_id]);
  map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (map_.count(frame_id) != 0) {
    //    LOG_DEBUG("Unpinned frame is already in LRUReplacer.");
    return;
  }

  if (list_.size() >= max_size_) {
    //    LOG_DEBUG("LRUReplacer is already full in unpin operation.");
    return;
  }

  list_.push_front(frame_id);
  map_.insert(std::make_pair(frame_id, list_.begin()));
}

auto LRUReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return static_cast<size_t>(list_.size());
}

}  // namespace bustub
