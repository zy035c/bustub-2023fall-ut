#include "storage/page/page_guard.h"
#include <cstddef>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->bpm_ = that.bpm_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
}

void BasicPageGuard::Drop() {
    if (this->page_ == nullptr) {
        return;
    }
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
    this->page_ = nullptr;
    this->bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    if (this == &that) {
        return *this;
    }
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    this->bpm_ = that.bpm_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;
    return *this;
}

BasicPageGuard::~BasicPageGuard(){
    this->Drop();
};  // NOLINT

/*
* -----------------------------------------
*/
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return {bpm_, page_}; }
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

/*
* -----------------------------------------
*/
ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
    this->guard_ = std::move(BasicPageGuard(bpm, page));
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    this->guard_ = std::move(that.guard_);  // go to line 23
    return *this; 
}

void ReadPageGuard::Drop() {
    this->guard_.page_->RUnlatch();
    this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
    this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
    this->guard_ = std::move(BasicPageGuard(bpm, page));
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    this->guard_.page_->WUnlatch();
    this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
    this->Drop();
}  // NOLINT

}  // namespace bustub
