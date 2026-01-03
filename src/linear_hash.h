    #ifndef MVCC_LINEAR_HASHTABLE_LINEAR_HASH_H
    #define MVCC_LINEAR_HASHTABLE_LINEAR_HASH_H

    #include <iostream>
    #include <vector>
    #include <algorithm>
    #include <functional>
    #include <memory>
    #include <optional>
    #include <stdexcept>
    #include <mutex>    //Standard lock, mutual exclusion
    #include <shared_mutex>     // R/W, everyone read if no current writing, only one person can write
    //std::shared_lock - reading lock
    //std::unique_lock - write
    #include <atomic>           // hardware level locking, faster than lock
    #include <iterator> //already included in vector, but good practice

    template <typename K, typename V>
    class LinearHash {
    private:
        struct Entry {  //kv pair struct
            K key;
            V value;
        };

        //using Bucket = std::vector<Entry>;  //type alias bucket is vector of entries
        struct Bucket {
            std::vector<Entry> entries;
            mutable std::shared_mutex mutex;
        };

        using Bucket_ptr = std::unique_ptr<Bucket>; //performance optimisation to store pointers
        std::vector<Bucket_ptr> table;  //actual hashtable

        double max_load_factor;
        std::atomic<size_t> num_elem;        //use size_t to represent size of thing in memory, atomic as a hardware level lock for var

        // Splitting vars
        size_t split_ptr;           // index tracking of which bucket to split
        const size_t init_size;   // starting size(2)
        size_t depth;       // How many bits currently checking of hash? size of postsplit given by init_size << depth

        mutable std::shared_mutex global_mutex;

        size_t hash2bucket(const K& key) const;
        bool should_split() const;

    public:
        //===== WARNING: Iterators are not thread safe! =====
        class Iterator {
        private:
            const LinearHash* _hm; //reference to the map we're iterating
            size_t _bucket_idx;
            size_t _entry_idx;

            void go2data() {    //helper to skip empty bucket
                while (_bucket_idx < _hm->table.size()) {   //bucket in limit of table
                    if (!(_hm->table.at(_bucket_idx)->entries.empty())) {   //if found data
                        return;
                    }
                    ++_bucket_idx;
                }
            }

        public:
            using value_type = Entry;    //do not change entry, If making a copy
            using pointer = const Entry*;
            using reference = const Entry&;
            using difference_type =  std::ptrdiff_t;
            using iterator_category = std::forward_iterator_tag;

            Iterator(const LinearHash* hm, size_t bucket_idx, size_t entry_idx)
                : _hm(hm), _bucket_idx(bucket_idx), _entry_idx(entry_idx) {
                // skip forward to data
                if (_bucket_idx < _hm->table.size() && _hm->table.at(_bucket_idx)->entries.empty()) {
                    go2data();
                }
            }

            reference operator*() const {   //return ref to entry
                return _hm->table.at(_bucket_idx)->entries.at(_entry_idx);
            }

            pointer operator->() const {    //return pointer to entry
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

            Iterator operator++(int) { //dummy flag for post increment
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

        explicit LinearHash(size_t size = 2, double load_factor = 0.75);   //constructor with default args. MUST call this constructor

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

        if (size == 0 || (size & (size - 1)) != 0) {    // Safety check
            throw std::invalid_argument("Initial size must be positive power of 2");
        }

        table.reserve(init_size); // Performance optimization
        for (size_t i = 0; i < init_size; ++i) {
            table.push_back(std::make_unique<Bucket>());
        }
    }

    template <typename K, typename V>
    size_t LinearHash<K, V>::hash2bucket(const K& key) const { //hash function. k& to specify that ref to mem used, rather than make copy
        //const? -> promise that we do not change the value
        //std::hash<K> is the type/struct. {} creates a temporary object, (key) calls operator()
        const auto h = std::hash<K>{}(key);
        const auto pre_expansion_size = init_size << depth; // 2 * (2 ^ depth)

        auto mask = pre_expansion_size - 1;
        auto index = h & mask;    //get any bits active for hash

        if (index < split_ptr) {
            // potential for split bucket to exist
            //must evaluate for potential split bucket
            mask = (mask << 1) + 1;   // move mask up 1, replace 0 with 1
            index = h & mask;
        }

        return index;
    }

    template <typename K, typename V>
    bool LinearHash<K, V>::should_split() const {
        if (table.empty()) {
            return false;
        }

        double load = static_cast<double>(num_elem) / static_cast<double>(table.size());
        return load > max_load_factor;
    }

    template <typename K, typename V>
    void LinearHash<K, V>::insert(const K& key, const V& val) {
        {   //Lock scoping forces locks to expire for potential split
            std::shared_lock<std::shared_mutex> global_read(global_mutex);  //do not change table
            const size_t i = hash2bucket(key); //Only one function call

            auto& bucket = *table.at(i); //Safer call to get dst. *to get val behind ptr
            std::unique_lock<std::shared_mutex> bucket_write(bucket.mutex);  //now updating bucket, all other buckets accessable

            for (auto& entry : bucket.entries) {    //we may update entry, so no const
                if (entry.key == key) { //update value
                    entry.value = val;
                    return;
                }
            }

        bucket.entries.push_back(Entry{key, val});
        ++num_elem;
        }   //locks now expire

        if (should_split()) {
            std::unique_lock<std::shared_mutex> global_write(global_mutex); //full table lock for resize

            if (!should_split()) {return;}  //double check if someone has split while we were waiting in thread

            table.push_back(std::make_unique<Bucket>());    //make unique creates both dataitem but returns pointer
            auto& original = *table.at(split_ptr);
            auto& new_bucket = *table.back();

            auto higher_mask = init_size << depth; //mask onto the highest bit

            Bucket temp;    //empty default constructor

            for (auto& entry : original.entries) {
                if ((std::hash<K>{}(entry.key)) & higher_mask) {//single bit of on/off
                    new_bucket.entries.push_back(entry);
                } else {    //means that returned a 0
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
        //shared lock, multiple people can read at same time, making sure table is stable(mostly to protect from table resizing)
        std::shared_lock<std::shared_mutex> global_read(global_mutex);

        const auto& bucket = *table.at(hash2bucket(key));
        //single bucket lock, do not insert/change bucket until lock expires
        std::shared_lock<std::shared_mutex> bucket_read(bucket.mutex);

        for (const auto& entry : bucket.entries) {
            if (entry.key == key) {
                return entry.value;
            }
        }
        return std::nullopt;    //return safe wrapper which requires check
    }

    template <typename K, typename V>
    void LinearHash<K, V>::print() const {
        std::unique_lock<std::shared_mutex> global_read(global_mutex);  //We do not ever want the table to change while we're printing whole thing
        for (size_t i = 0; i < table.size(); ++i) {
            //++i means increment, then return new value
            std::cout << "Bucket " << i << ": ";

            for (const auto& entry : table[i]->entries) {    //for in loop, get val behind ptr
                std::cout << "[" << entry.key << ":" << entry.value << "]";
            }
            std::cout << std::endl;
        }
    }

    template <typename K, typename V>
    bool LinearHash<K, V>::in(const K& key) const { //shared
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
    bool LinearHash<K, V>::remove(const K& key) {   //exclusive write, change
        std::shared_lock<std::shared_mutex> global_read(global_mutex);

        auto& bucket_struct = *table.at(hash2bucket(key));
        // unique lock to block anyone accessing the bucket while we make potential changes
        std::unique_lock<std::shared_mutex> bucket_write(bucket_struct.mutex);

        auto& bucket = bucket_struct.entries;

        for (size_t i = 0; i < bucket.size(); ++i) {    //optimised algo for deleting from vector
            if (bucket[i].key == key) {
                bucket[i] = std::move(bucket.back());   //pointer swap last
                bucket.pop_back();       //remove 'zombie' from vector
                --num_elem;
                return true;
            }
        }
        return false;   //unable to find
    }

    #endif //MVCC_LINEAR_HASHTABLE_LINEAR_HASH_H