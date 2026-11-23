#include "vfs/staging_budget.h"

#include <algorithm>

namespace telega::vfs {

void StagingBudget::Lease::shrink_to(uint64_t n) {
    if (budget_ == nullptr || n >= bytes_) {
        return;
    }
    budget_->shrink(bytes_, n, internal_);
    bytes_ = n;
}

void StagingBudget::Lease::release() {
    if (budget_ != nullptr) {
        budget_->release_internal(bytes_, internal_);
        budget_ = nullptr;
        bytes_ = 0;
    }
}

StagingBudget::StagingBudget(StagingConfig config, std::function<void(uint64_t)> probe)
    : cfg_(config), probe_(std::move(probe)) {
    user_cap_ = cfg_.limit > cfg_.reserve ? cfg_.limit - cfg_.reserve : 0;
    threshold_ = std::min(cfg_.high, user_cap_);
}

void StagingBudget::changed_locked() {
    const uint64_t total = total_locked();
    peak_ = std::max(peak_, total);
    if (probe_) {
        probe_(total);
    }
}

std::optional<StagingBudget::Lease> StagingBudget::try_acquire_user(uint64_t n) {
    const std::lock_guard lock(mu_);
    if (n == 0) {
        return Lease(this, 0, false);  // запись без роста staging не блокируется
    }
    if (n > user_cap_ || total_locked() > user_cap_ - n) {
        return std::nullopt;
    }
    used_ += n;
    changed_locked();
    return Lease(this, n, false);
}

StagingBudget::WaitResult StagingBudget::wait_user(uint64_t n,
                                                   std::chrono::steady_clock::time_point deadline,
                                                   const std::function<bool()>& abort) {
    std::unique_lock lock(mu_);
    bool aborted = false;
    const bool ok = cv_.wait_until(lock, deadline, [&] {
        if (abort()) {
            aborted = true;
            return true;
        }
        return n <= user_cap_ && total_locked() <= user_cap_ - n;
    });
    if (aborted) {
        return WaitResult::aborted;
    }
    return ok ? WaitResult::ok : WaitResult::timeout;
}

std::optional<StagingBudget::Lease> StagingBudget::acquire_internal(
    uint64_t n, const std::function<bool()>& abort) {
    std::unique_lock lock(mu_);
    bool aborted = false;
    bool over = false;
    cv_.wait(lock, [&] {
        if (abort()) {
            aborted = true;
            return true;
        }
        if (n <= cfg_.limit && total_locked() <= cfg_.limit - n) {
            return true;
        }
        if (internal_leases_ == 0) {
            // Держателей нет, место само не освободится: пропустить поверх лимита.
            over = true;
            return true;
        }
        return false;
    });
    if (aborted) {
        return std::nullopt;
    }
    if (over) {
        ++overcommits_;
    }
    used_ += n;
    leased_ += n;
    ++internal_leases_;
    changed_locked();
    return Lease(this, n, true);
}

void StagingBudget::charge_unchecked(uint64_t n) {
    const std::lock_guard lock(mu_);
    used_ += n;
    changed_locked();
}

void StagingBudget::release(uint64_t n) {
    {
        const std::lock_guard lock(mu_);
        used_ -= std::min(used_, n);
        changed_locked();
    }
    cv_.notify_all();
}

void StagingBudget::release_internal(uint64_t n, bool internal) {
    {
        const std::lock_guard lock(mu_);
        used_ -= std::min(used_, n);
        if (internal) {
            leased_ -= std::min(leased_, n);
            if (internal_leases_ > 0) {
                --internal_leases_;
            }
        }
        changed_locked();
    }
    cv_.notify_all();
}

void StagingBudget::shrink(uint64_t n, uint64_t to, bool internal) {
    {
        const std::lock_guard lock(mu_);
        used_ -= std::min(used_, n - to);
        if (internal) {
            leased_ -= std::min(leased_, n - to);
        }
        changed_locked();
    }
    cv_.notify_all();
}

void StagingBudget::reset(uint64_t used) {
    {
        const std::lock_guard lock(mu_);
        used_ = used;
        changed_locked();
    }
    cv_.notify_all();
}

void StagingBudget::set_external(uint64_t bytes) {
    {
        const std::lock_guard lock(mu_);
        if (external_ == bytes) {
            return;
        }
        external_ = bytes;
        changed_locked();
    }
    cv_.notify_all();
}

void StagingBudget::notify_all() {
    // Пустая секция: ожидающий не пропустит уведомление между проверкой предиката и сном.
    { const std::lock_guard lock(mu_); }
    cv_.notify_all();
}

uint64_t StagingBudget::used() const {
    const std::lock_guard lock(mu_);
    return total_locked();
}

uint64_t StagingBudget::peak() const {
    const std::lock_guard lock(mu_);
    return peak_;
}

uint64_t StagingBudget::overcommits() const {
    const std::lock_guard lock(mu_);
    return overcommits_;
}

uint64_t StagingBudget::leased() const {
    const std::lock_guard lock(mu_);
    return leased_;
}

bool StagingBudget::above_threshold(uint64_t extra) const {
    const std::lock_guard lock(mu_);
    return total_locked() + extra > threshold_;
}

}  // namespace telega::vfs
