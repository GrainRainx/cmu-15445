//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <exception>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::HistoryGetFrame(frame_id_t frame_id) -> std::list<frame_id_t>::iterator {
  for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
    if ((*it) == frame_id) {
      return it;
    }
  }
  return history_list_.end();
}

auto LRUKReplacer::CacheGetFrame(frame_id_t frame_id) -> std::list<frame_id_t>::iterator {
  for (auto it = cache_list_.begin(); it != cache_list_.end(); it++) {
    if ((*it) == frame_id) {
      return it;
    }
  }
  return cache_list_.end();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      *frame_id = frame;
      auto de_iter = HistoryGetFrame(frame);
      history_list_.erase(de_iter);
      curr_size_--;
      is_evictable_[frame] = false;
      return true;
    }
  }

  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      *frame_id = frame;
      auto de_iter = CacheGetFrame(frame);
      cache_list_.erase(de_iter);
      curr_size_--;
      is_evictable_[frame] = false;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  std::scoped_lock<std::mutex> lock(latch_);
  access_count_[frame_id]++;
  if (access_count_[frame_id] == k_) {
    auto dele_frame = HistoryGetFrame(frame_id);
    if (dele_frame != history_list_.end()) {
      history_list_.erase(dele_frame);
    }
    cache_list_.push_front(frame_id);
  } else if (access_count_[frame_id] > k_) {
    auto dele_frame = CacheGetFrame(frame_id);
    if (dele_frame != cache_list_.end()) {
      cache_list_.erase(dele_frame);
    }
    cache_list_.push_front(frame_id);
  } else {
    auto dele_frame = HistoryGetFrame(frame_id);
    if (dele_frame != history_list_.end()) {
      history_list_.erase(dele_frame);
    }
    history_list_.push_front(frame_id);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  std::scoped_lock<std::mutex> lock(latch_);
  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (access_count_[frame_id] == 0) {
    return;
  }
  if (!is_evictable_[frame_id]) {
    throw std::exception();
    return;
  }
  if (access_count_[frame_id] >= k_) {
    auto de_iterator = CacheGetFrame(frame_id);
    cache_list_.erase(de_iterator);
  } else {
    auto de_iterator = HistoryGetFrame(frame_id);
    history_list_.erase(de_iterator);
  }
  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
