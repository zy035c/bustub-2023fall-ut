#include "storage/page/page_guard.h"
#include <cstddef>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->bpm_ = that.bpm_;
}

void BasicPageGuard::Drop() {
    this->bpm_->DeletePage(this->PageId());
    delete this->page_;
    delete this->bpm_;
    this->page_ = nullptr;
    this->bpm_ = nullptr;
    this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    if (this == &that) {
        return *this;
    }
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->bpm_ = that.bpm_;
    return *this;
}

BasicPageGuard::~BasicPageGuard(){
    delete this->page_;
    delete this->bpm_;
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { 
    
    return {bpm_, page_}; 
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
    this->guard_.bpm_ = bpm;
    this->guard_.page_ = page;
    // should i ?
    // page = nullptr;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    this->guard_ = std::move(that.guard_);
    return *this; 
}

void ReadPageGuard::Drop() {
    this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
    this->guard_.~BasicPageGuard();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
    this->guard_.bpm_ = bpm;
    this->guard_.page_ = page;
    // should i ?
    // page = nullptr;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
    this->guard_.~BasicPageGuard();
}  // NOLINT

}  // namespace bustub
