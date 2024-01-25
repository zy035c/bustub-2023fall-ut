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
#include "common/exception.h"
#include "algorithm"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if (this->curr_size_ <= 0) {
        return false;
    }

    if (!this->node_inf_k_dist.empty()) {
        for (auto it = this->node_inf_k_dist.begin(); it != this->node_inf_k_dist.end(); ++it) {
            if (this->node_store_[*it].GetEvictable()) {
                *frame_id = *it;
                this->node_store_.erase(*frame_id);
                this->node_inf_k_dist.remove(*frame_id);
                --this->curr_size_;
                return true;
            }
        }

    } else {
        for (auto it = this->node_k_dist.begin(); it != this->node_k_dist.end(); ++it) {
            if (this->node_store_[*it].GetEvictable()) {
                *frame_id = *it;
                this->node_store_.erase(*frame_id);
                this->node_k_dist.remove(*frame_id);
                --this->curr_size_;
                return true;
            }
        }
    }

    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if (static_cast<size_t>(frame_id) > this->replacer_size_) {
        throw bustub::Exception("frame id is invalid (ie. larger than replacer_size_");
    }
    LRUKNode cur_node;
    auto it = this->node_store_.find(frame_id);
    if (it != this->node_store_.end()) {
        cur_node = this->node_store_[frame_id];
    } else {
        if (this->Size() == this->replacer_size_) {
            // ? evict
            int val;
            this->Evict(&val);
        }
        cur_node = LRUKNode(this->k_, frame_id);
    }
    cur_node.AddHistory(this->current_timestamp_);

    size_t cur_k = cur_node.GetK();
        
    if (cur_k < this->k_) {
        cur_node.SetIsKInf(true);
        auto insert_it = std::find(this->node_inf_k_dist.begin(), this->node_inf_k_dist.end(), frame_id);
        if (insert_it == this->node_inf_k_dist.end()) {
            this->node_inf_k_dist.push_back(frame_id);
        } else {
            this->node_inf_k_dist.splice(
                this->node_inf_k_dist.end(),
                this->node_inf_k_dist,
                insert_it
            );
        }
    } else {
        if (cur_node.GetIsKInf()) {
            this->node_inf_k_dist.remove(frame_id);
            cur_node.SetIsKInf(false);
            this->node_k_dist.push_back(frame_id);
        } else {
            this->node_k_dist.splice(
                this->node_k_dist.end(),
                this->node_k_dist,
                std::find(this->node_k_dist.begin(), this->node_k_dist.end(), frame_id)
            );
        }
    }

    ++this->current_timestamp_;
    node_store_[frame_id] = cur_node;  // refresh
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    auto it = this->node_store_.find(frame_id);
    if (it == this->node_store_.end()) {
        throw bustub::Exception("frame id is invalid (it has not been not recorded in the replacer");
    }
    auto cur_node = this->node_store_[frame_id];

    // check status
    if (cur_node.GetEvictable() == set_evictable) {
        return;
    }

    if (set_evictable) {
        ++this->curr_size_;
    } else {
        --this->curr_size_;
    }
    cur_node.SetEvictable(set_evictable);
    this->node_store_[frame_id] = cur_node;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if (static_cast<size_t>(frame_id) > this->replacer_size_) {
        throw bustub::Exception("frame id is invalid (ie. larger than replacer_size_");
    }
    auto it = this->node_store_.find(frame_id);
    if (it == this->node_store_.end()) {
        return;
    }
    auto cur_node = this->node_store_[frame_id];
    if (!cur_node.GetEvictable()) {
        throw bustub::Exception("Remove is called on a non-evictable frame");
    }

    this->node_store_.erase(frame_id);
    if (cur_node.GetIsKInf()) {
        this->node_inf_k_dist.remove(frame_id);
    } else {
        this->node_k_dist.remove(frame_id);
    }
    --this->curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

//
//

LRUKNode::LRUKNode(size_t k, frame_id_t frame_id) : k_(k), fid_(frame_id) {}
LRUKNode::LRUKNode() {}

void LRUKNode::AddHistory(size_t history) {
    this->history_.push_back(history);
    if (this->history_.size() > this->k_) {
        this->history_.pop_front();
    }
}

auto LRUKNode::GetK() -> size_t {
    return this->history_.size();
}

auto LRUKNode::GetEvictable() -> bool { return this->is_evictable_; }
auto LRUKNode::SetEvictable(bool ev) -> void { this->is_evictable_ = ev; }
auto LRUKNode::SetIsKInf(bool is_k_inf) -> void {this->is_k_inf_ = is_k_inf; }
auto LRUKNode::GetIsKInf() -> bool {return this->is_k_inf_; }


}  // namespace bustub
