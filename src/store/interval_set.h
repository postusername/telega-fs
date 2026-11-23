#pragma once

// Объединение полуинтервалов [a, b) целых чисел: готовые и запрошенные диапазоны файла
// планировщика скачиваний (SPEC 8.2). Не потокобезопасен.

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <map>
#include <utility>
#include <vector>

namespace telega::store {

class IntervalSet {
  public:
    // Добавить [a, b); смежные и пересекающиеся куски сливаются.
    void add(int64_t a, int64_t b) {
        if (b <= a) {
            return;
        }
        auto it = m_.upper_bound(a);
        if (it != m_.begin()) {
            const auto prev = std::prev(it);
            if (prev->second >= a) {
                a = prev->first;
                b = std::max(b, prev->second);
                it = m_.erase(prev);
            }
        }
        while (it != m_.end() && it->first <= b) {
            b = std::max(b, it->second);
            it = m_.erase(it);
        }
        m_[a] = b;
    }

    // Убрать [a, b) из набора.
    void remove(int64_t a, int64_t b) {
        if (b <= a) {
            return;
        }
        auto it = m_.upper_bound(a);
        if (it != m_.begin()) {
            --it;
        }
        std::vector<std::pair<int64_t, int64_t>> keep;
        while (it != m_.end() && it->first < b) {
            if (it->second <= a) {
                ++it;
                continue;
            }
            if (it->first < a) {
                keep.emplace_back(it->first, a);
            }
            if (it->second > b) {
                keep.emplace_back(b, it->second);
            }
            it = m_.erase(it);
        }
        for (const auto& [x, y] : keep) {
            m_[x] = y;
        }
    }

    [[nodiscard]] bool covers(int64_t a, int64_t b) const { return b <= a || end_of(a) >= b; }

    // Конец непрерывного куска, в котором лежит a; a, если a не покрыта.
    [[nodiscard]] int64_t end_of(int64_t a) const {
        auto it = m_.upper_bound(a);
        if (it == m_.begin()) {
            return a;
        }
        --it;
        return it->second > a ? it->second : a;
    }

    // Непокрытые куски [a, b) по возрастанию.
    [[nodiscard]] std::vector<std::pair<int64_t, int64_t>> missing(int64_t a, int64_t b) const {
        std::vector<std::pair<int64_t, int64_t>> gaps;
        int64_t pos = a;
        while (pos < b) {
            const int64_t e = end_of(pos);
            if (e > pos) {
                pos = e;
                continue;
            }
            const auto it = m_.upper_bound(pos);
            const int64_t next = it == m_.end() ? b : std::min(b, it->first);
            gaps.emplace_back(pos, next);
            pos = next;
        }
        return gaps;
    }

    [[nodiscard]] const std::map<int64_t, int64_t>& intervals() const { return m_; }
    [[nodiscard]] bool empty() const { return m_.empty(); }
    void clear() { m_.clear(); }

    [[nodiscard]] int64_t total() const {
        int64_t t = 0;
        for (const auto& [a, b] : m_) {
            t += b - a;
        }
        return t;
    }

  private:
    std::map<int64_t, int64_t> m_;
};

}  // namespace telega::store
