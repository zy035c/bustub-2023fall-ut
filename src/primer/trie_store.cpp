#include "primer/trie_store.h"
#include "common/exception.h"
#include <shared_mutex>

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // Pseudo-code:
  // (1) Take the root lock, get the root, and release the root lock. Don't lookup the value in the
  //     trie while holding the root lock.
  // (2) Lookup the value in the trie.
  // (3) If the value is found, return a ValueGuard object that holds a reference to the value and the
  //     root. Otherwise, return std::nullopt.
  std::shared_lock<std::shared_mutex> lock(this->root_lock_); // WHY there was a include shared_mutex in trie_Store.h ??
  // None of these two locks is shared_mutex
  auto root = this->root_;
  lock.unlock();
  auto val = root.Get<T>(key);
  if (val == nullptr) {
    return std::nullopt;
  }
  return ValueGuard<T>(root, *val);
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::unique_lock<std::shared_mutex> lock(this->write_lock_);
  std::unique_lock<std::shared_mutex> root_lock(this->root_lock_); // put lock to both root and write_lock
  auto root = this->root_;
  root.Put<T>(key, std::forward<T>(value)); // why forward?? does it work?
  this->root_ = root;
}

void TrieStore::Remove(std::string_view key) {
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
  std::unique_lock<std::shared_mutex> lock(this->write_lock_);
  std::unique_lock<std::shared_mutex> root_lock(this->root_lock_); // put lock to both root and write_lock
  auto root = this->root_;
  root.Remove(key);
  this->root_ = root;
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
