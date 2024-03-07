//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  std::shared_ptr<Bucket> new_bucket(new Bucket(bucket_size_, 0));
  dir_.push_back(new_bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  size_t index = IndexOf(key);
  bool flag = false;
  latch_.lock();
  flag = dir_[index]->Find(key, value);
  latch_.unlock();
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  size_t index = IndexOf(key);
  bool flag = false;
  latch_.lock();
  flag = dir_[index]->Remove(key);
  latch_.unlock();
  return flag;
}

auto Lowmask(int number, int n) -> int {
  int mask = (1 << n) - 1;
  return number & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  num_buckets_++;
  bucket->IncrementDepth();
  int new_depth = bucket->GetDepth();
  std::shared_ptr<Bucket> new_bucket(new Bucket(bucket_size_, new_depth));

  int pre_bucket_mask = Lowmask(std::hash<K>()(bucket->GetItems().begin()->first), new_depth - 1);
  for (auto it = (bucket->GetItems().begin()); it != (bucket->GetItems().end());) {
    if (pre_bucket_mask != Lowmask(std::hash<K>()((*it).first), new_depth)) {
      new_bucket->Insert(it->first, it->second);
      it = bucket->GetItems().erase(it);
    } else {
      it++;
    }
  }

  for (int i = 0; i < (1 << global_depth_); i++) {
    if (Lowmask(i, new_depth - 1) == pre_bucket_mask && Lowmask(i, new_depth) != pre_bucket_mask) {
      dir_[i] = new_bucket;
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  size_t index = IndexOf(key);
  latch_.lock();
  if (dir_[index]->IsFull()) {
    if (global_depth_ == GetLocalDepthInternal(index)) {
      // 因该执行dirtory的扩张 ，扩张一倍
      for (int i = 0; i < (1 << global_depth_); i++) {
        auto tmp = dir_[i];
        dir_.push_back(tmp);
      }
      global_depth_++;
    }
    // 分裂bucket 并且bucket的localdepth++
    index = IndexOf(key);
    RedistributeBucket(dir_[index]);
  }
  dir_[index]->Insert(key, value);
  latch_.unlock();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end();) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
    ++it;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  bool flag = false;
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      it = list_.erase(it);
      flag = true;
    } else {
      ++it;
    }
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  bool flag = false;
  for (auto it = list_.begin(); it != list_.end();) {
    if ((*it).first == key) {
      (*it).second = value;
      flag = true;
    } else {
      ++it;
    }
  }
  if (flag) {
    return true;
  }
  list_.push_back(std::make_pair(key, value));
  return true;
  UNREACHABLE("not implemented");
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
