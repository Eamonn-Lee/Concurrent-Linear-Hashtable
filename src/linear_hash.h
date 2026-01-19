#ifndef MVCC_LINEAR_HASHTABLE_LINEAR_HASH_H
#define MVCC_LINEAR_HASHTABLE_LINEAR_HASH_H

#include <iostream>
#include <vector>
#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <iterator>

template <typename K, typename V>
class LinearHash {
private:
    struct Entry {
        K key;
        V value;
    };

    struct Bucket {
        std::vector<Entry> entries;
        mutable std::shared_mutex mutex;
    };

    using Bucket_ptr = std::unique_ptr<Bucket>; // memory optimisation
    std::vector<Bucket_ptr> table;

    double max_load_factor;
    std::atomic<size_t> num_elem;

    size_t split_ptr;           // current/next bucket to split
    const size_t init_size;   // starting size(2)
    size_t depth;       // hash depth, init_size << depth == post_split size

    mutable std::shared_mutex global_mutex;

    size_t hash2bucket(const K& key) const;
    bool split_cond() const;

public:
    //===== WARNING: Iterators are not thread safe! =====
    class Iterator {
    private:
        const LinearHash* _hm;
        size_t _bucket_idx;
        size_t _entry_idx;

        void go2data() {    //helper to skip empty bucket
            while (_bucket_idx < _hm->table.size()) {
                if (!(_hm->table.at(_bucket_idx)->entries.empty())) {
                    return;
                }
                ++_bucket_idx;
            }
        }

    public:
        using value_type = Entry;
        using pointer = const Entry*;
        using reference = const Entry&;
        using difference_type =  std::ptrdiff_t;
        using iterator_category = std::forward_iterator_tag;

        Iterator(const LinearHash* hm, size_t bucket_idx, size_t entry_idx)
            : _hm(hm), _bucket_idx(bucket_idx), _entry_idx(entry_idx) {
            if (_bucket_idx < _hm->table.size() && _hm->table.at(_bucket_idx)->entries.empty()) {
                go2data();
            }
        }

        reference operator*() const {
            return _hm->table.at(_bucket_idx)->entries.at(_entry_idx);
        }

        pointer operator->() const {
            return &(_hm->table.at(_bucket_idx)->entries.at(_entry_idx));
        }

        Iterator& operator++() {
            ++_entry_idx;
            if (_entry_idx >= _hm->table.at(_bucket_idx)->entries.size()) {
                _entry_idx = 0;
                ++_bucket_idx;
            }
            go2data();

            return *this;
        }

        Iterator operator++(int) {
            auto tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const Iterator& other) const {
            return _hm == other._hm &&
                _bucket_idx == other._bucket_idx &&
                _entry_idx == other._entry_idx;
        }

        bool operator!=(const Iterator & other) const {
            return !(*this == other);
        }
    };
    // Iterator end =============================

    explicit LinearHash(size_t size = 2, double load_factor = 0.75);

    void insert(const K& key, const V& val);
    std::optional<V> get(const K& key) const;
    bool in(const K& key) const;
    bool remove(const K& key);

    auto get_table_size() const{ return table.size(); }
    auto get_num_elem() const { return num_elem.load(); }
    auto get_split_ptr() const { return split_ptr; }

    Iterator begin() const { return Iterator(this, 0, 0); }
    Iterator end() const { return Iterator(this, table.size(), 0);}

    void print() const;
};

// IMPLEMENTATION===========================================
template <typename K, typename V>
LinearHash<K, V>::LinearHash(size_t size, double load_factor)
    : max_load_factor(load_factor), num_elem(0), split_ptr(0), init_size(size), depth(0) {
    if (size == 0 || (size & (size - 1)) != 0) {
        throw std::invalid_argument("Initial size must be positive power of 2");
    }

    table.reserve(init_size); // Performance optimization
    for (size_t i = 0; i < init_size; ++i) {
        table.push_back(std::make_unique<Bucket>());
    }
}

template <typename K, typename V>
size_t LinearHash<K, V>::hash2bucket(const K& key) const {
    const auto h = std::hash<K>{}(key);
    const auto pre_expansion_size = init_size << depth;

    auto mask = pre_expansion_size - 1; // bitwise mask
    auto index = h & mask;

    if (index < split_ptr) {    //evaluate potential split bucket
        mask = (mask << 1) + 1;   // move mask up 1, replace right 0 with 1
        index = h & mask;
    }

    return index;
}

template <typename K, typename V>
bool LinearHash<K, V>::split_cond() const {
    if (table.empty()) {
        return false;
    }

    const double load = static_cast<double>(num_elem) / static_cast<double>(table.size());
    return load > max_load_factor;
}

template <typename K, typename V>
void LinearHash<K, V>::insert(const K& key, const V& val) {
    auto should_split = false;   //carries check result out of lock scope
    {   // scope lock
        std::shared_lock<std::shared_mutex> global_read(global_mutex);
        const size_t i = hash2bucket(key); //Only one function call

        auto& bucket = *table.at(i);
        std::unique_lock<std::shared_mutex> bucket_write(bucket.mutex);

        for (auto& entry : bucket.entries) {
            if (entry.key == key) {
                entry.value = val;
                return;
            }
        }

    bucket.entries.push_back(Entry{key, val});
    ++num_elem;
    should_split = split_cond();
    }

    if (should_split) {
        std::unique_lock<std::shared_mutex> global_write(global_mutex);

        if (!split_cond()) {return;}  //check for split while thread waiting

        table.push_back(std::make_unique<Bucket>());
        auto& original = *table.at(split_ptr);
        auto& new_bucket = *table.back();

        auto higher_mask = init_size << depth;  //single bit mask of new depth

        Bucket temp;

        for (auto& entry : original.entries) {
            if ((std::hash<K>{}(entry.key)) & higher_mask) {    // new considered bit == 1
                new_bucket.entries.push_back(entry);
            } else {
                temp.entries.push_back(entry);
            }
        }

        original.entries = std::move(temp.entries);
        split_ptr++;

        if (split_ptr >= (init_size << depth)) {
            split_ptr = 0;
            depth++;
        }
    }
}

template <typename K, typename V>
std::optional<V> LinearHash<K, V>::get(const K& key) const {
    std::shared_lock<std::shared_mutex> global_read(global_mutex);

    const auto& bucket = *table.at(hash2bucket(key));
    std::shared_lock<std::shared_mutex> bucket_read(bucket.mutex);

    for (const auto& entry : bucket.entries) {
        if (entry.key == key) {
            return entry.value;
        }
    }
    return std::nullopt;
}

template <typename K, typename V>
void LinearHash<K, V>::print() const {
    std::unique_lock<std::shared_mutex> global_read(global_mutex);
    for (size_t i = 0; i < table.size(); ++i) {
        std::cout << "Bucket " << i << ": ";

        for (const auto& entry : table[i]->entries) {
            std::cout << "[" << entry.key << ":" << entry.value << "]";
        }
        std::cout << std::endl;
    }
}

template <typename K, typename V>
bool LinearHash<K, V>::in(const K& key) const {
    std::shared_lock<std::shared_mutex> global_read(global_mutex);

    const auto& bucket = *table.at(hash2bucket(key));
    std::shared_lock<std::shared_mutex> bucket_read(bucket.mutex);

    for (const auto& entry : bucket.entries) {
        if (entry.key == key) {
            return true;
        }
    }
    return false;
}

template <typename K, typename V>
bool LinearHash<K, V>::remove(const K& key) {
    std::shared_lock<std::shared_mutex> global_read(global_mutex);

    auto& bucket_struct = *table.at(hash2bucket(key));
    std::unique_lock<std::shared_mutex> bucket_write(bucket_struct.mutex);

    auto& bucket = bucket_struct.entries;

    // optimised vector del: std(O(n)) vs move(O(1)) + popback(O(1))
    for (size_t i = 0; i < bucket.size(); ++i) {
        if (bucket[i].key == key) {
            bucket[i] = std::move(bucket.back());
            bucket.pop_back();
            --num_elem;
            return true;
        }
    }
    return false;   //unable to find
}

#endif //MVCC_LINEAR_HASHTABLE_LINEAR_HASH_H