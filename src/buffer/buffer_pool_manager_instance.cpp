//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FindEmptyFrameid(frame_id_t *frame) -> bool {
  if (!free_list_.empty()) {
    *frame = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (!replacer_->Evict(frame)) {
    return false;
  }
  frame_id_t select_frame_id = *frame;
  if (pages_[select_frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[select_frame_id].page_id_, pages_[select_frame_id].data_);
  }
  page_table_->Remove(pages_[select_frame_id].page_id_);
  pages_[select_frame_id].page_id_ = INVALID_PAGE_ID;
  return true;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t free_frame_id;
  if (!FindEmptyFrameid(&free_frame_id)) {
    return nullptr;
  }

  page_id_t new_page = AllocatePage();
  *page_id = new_page;
  page_table_->Insert(new_page, free_frame_id);
  pages_[free_frame_id].page_id_ = new_page;
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].is_dirty_ = false;
  pages_[free_frame_id].ResetMemory();
  replacer_->SetEvictable(free_frame_id, false);
  replacer_->RecordAccess(free_frame_id);

  return &pages_[free_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }

  frame_id_t get_frame_id;
  if (page_table_->Find(page_id, get_frame_id)) {
    pages_[get_frame_id].pin_count_++;
    pages_[get_frame_id].page_id_ = page_id;
    replacer_->SetEvictable(get_frame_id, false);
    replacer_->RecordAccess(get_frame_id);

    return &pages_[get_frame_id];
  }
  frame_id_t free_frame_id;
  if (!FindEmptyFrameid(&free_frame_id)) {
    return nullptr;
  }

  page_table_->Insert(page_id, free_frame_id);

  pages_[free_frame_id].page_id_ = page_id;
  pages_[free_frame_id].pin_count_ = 1;
  pages_[free_frame_id].is_dirty_ = false;
  pages_[free_frame_id].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[free_frame_id].data_);
  replacer_->SetEvictable(free_frame_id, false);
  replacer_->RecordAccess(free_frame_id);

  return &pages_[free_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
  }

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].is_dirty_ && pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if(pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();

  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
