#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto cur = root_; /* Don't need to clone. Simply increment shared ptr counter. */

  for (char c : key) {
    if (cur == nullptr) {
      return nullptr;
    }

    auto it = cur->children_.find(c);
    if (it == cur->children_.end()) {
      return nullptr;
    }

    cur = it->second;
  }

  auto node_with_value = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());

  if (node_with_value == nullptr || !node_with_value->is_value_node_) {
    return nullptr;
  }

  return node_with_value->value_.get(); /* Get naked pointer T star */
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> new_root;
  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();
    // std::cout << "Creating a new root" << std::endl;
  } else {
    new_root = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  }

  if (key.empty()) {
    std::shared_ptr<TrieNode> root_with_value =
        std::make_shared<TrieNodeWithValue<T>>(new_root->children_, std::make_shared<T>(std::move(value)));
    return Trie(root_with_value);
  }

  std::shared_ptr<TrieNode> cur = new_root;

  for (std::size_t i = 0; i < key.length() - 1; ++i) {
    char c = key[i];

    auto it = cur->children_.find(c);

    std::shared_ptr<TrieNode> next;

    if (it == cur->children_.end()) { /* key not exist in children */
      next = std::make_shared<TrieNode>();
      // std::cout << "Can't' find node for char " << c << std::endl;
    } else {
      // std::cout << "Found node for char " << c << std::endl;
      next = std::shared_ptr<TrieNode>(it->second->Clone());
    }

    cur->children_[c] = next;
    cur = next;
  }

  std::shared_ptr<TrieNode> last;

  char last_key = key[key.length() - 1];
  auto it = cur->children_.find(last_key);

  if (it == cur->children_.end()) {
    /* std::shared_ptr<A> a = std::make_shared<B>(b); */
    /* Call the constructor of TrieNodeWithValue... */
    last = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    last = std::make_shared<TrieNodeWithValue<T>>(
        it->second->children_, std::make_shared<T>(std::move(value)) /* kind of depends on T's constructor? */
    );
  }

  cur->children_[last_key] = last;

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::vector<std::shared_ptr<TrieNode>> path;

  if (this->root_ == nullptr) { /* Try to remove from empty trie, return nothing */
    return *this;
  }

  auto new_root = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  auto cur = new_root;
  path.push_back(cur);

  for (char c : key) {
    auto it = cur->children_.find(c);

    // If the key doesn't exist, return the original trie.
    if (it == cur->children_.end()) {
      return *this;
    }

    auto next = std::shared_ptr<TrieNode>(std::move(it->second->Clone()));
    path.push_back(next);
    cur->children_[c] = next;  // connect new_root to cloned new nodes
    cur = next;
  }

  if (!cur->is_value_node_) {
    return *this;
  }

  if (key.size() == 0) {
    auto new_root = std::make_shared<TrieNode>(cur->children_);  // only delete the value at root
    return Trie(new_root);
  }

  char last_char = key[key.length() - 1];
  /* Now cur is still a TrieNode ptr. Instantiate a TrieNode. */
  std::shared_ptr<TrieNode> node_wo_value = std::make_shared<TrieNode>(cur->children_);
  path.pop_back();
  path.back()->children_[last_char] = node_wo_value;

  auto runner = node_wo_value;

  auto it = key.rbegin();
  while (!path.empty() && it != key.rend() && !runner->is_value_node_ && runner->children_.empty()) {
    path.back()->children_.erase(*it);
    runner = path.back();
    path.pop_back();
    ++it;
  }

  if (new_root->children_.empty()) {
    return Trie(nullptr);
  }

  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
