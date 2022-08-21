//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances; i++) {
    auto instance = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    buffer_pools_.push_back(instance);
  }

  this->num_instances_ = num_instances;
  this->pool_size_ = pool_size;
  start_index_ = 0;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete buffer_pools_[i];
  }
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  size_t total = 0;
  for (size_t i = 0; i < num_instances_; i++) {
    total += buffer_pools_[i]->GetPoolSize();
  }
  return total;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManager * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pools_[page_id % num_instances_];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bpmi = buffer_pools_[page_id % num_instances_];
  return bpmi->FetchPage(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bpmi = buffer_pools_[page_id % num_instances_];
  return bpmi->UnpinPage(page_id, is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  if (page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("Input cannot be INVALID_PAGE_ID.");
  }

  BufferPoolManagerInstance *bpmi = buffer_pools_[page_id % num_instances_];
  return bpmi->FlushPage(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  size_t start = start_index_ % num_instances_;
  start_index_++;
  size_t idx = start;
  Page *result = nullptr;
  while (true) {
    result = buffer_pools_[idx]->NewPage(page_id);
    if (result != nullptr) {
      return result;
    }
    idx = (idx + 1) % num_instances_;
    if (idx == start) {
      return nullptr;
    }
  }
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *bpmi = buffer_pools_[page_id % num_instances_];
  return bpmi->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    buffer_pools_[i]->FlushAllPages();
  }
}

}  // namespace bustub
