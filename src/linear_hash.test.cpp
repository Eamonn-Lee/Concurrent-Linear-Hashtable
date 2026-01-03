#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include <catch2/catch.hpp>
#include "linear_hash.h"

#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <random>
#include <chrono>
#include <algorithm> // Required for std::find_if
#include <set>       // Required for verification


TEST_CASE("Basic Operations") {
    LinearHash<int, int> map(2, 0.75);

    SECTION("Initialization") {
        REQUIRE(map.get_num_elem() == 0);
        REQUIRE(map.get_table_size() == 2);
    }

    SECTION("Insert, get") {
        map.insert(1, 100);
        map.insert(2, 200);

        REQUIRE(map.get_num_elem() == 2);
        REQUIRE(map.get(1).has_value());
        REQUIRE(map.get(1).value() == 100);
        REQUIRE(map.get(2).value() == 200);
    }

    SECTION("nullopt missing keys") {
        map.insert(1, 100);
        REQUIRE_FALSE(map.get(999).has_value());
    }

    SECTION("overwrite keys") {
        map.insert(1, 100);
        REQUIRE(map.get_num_elem() == 1); // Count should not increase
        map.insert(1, 999); // Update

        REQUIRE(map.get_num_elem() == 1);
        REQUIRE(map.get(1).value() == 999);
    }

    SECTION("In") {
        map.insert(5, 50);
        REQUIRE(map.in(5) == true);
        REQUIRE(map.in(10) == false);
    }
}

TEST_CASE("Complex types") {
    LinearHash<std::string, std::string> map(2, 0.8);

    SECTION("std::string") {
        map.insert("user1", "alice");
        map.insert("user2", "bob");

        REQUIRE(map.get("user1").value() == "alice");
        REQUIRE(map.get("user2").value() == "bob");
    }

    SECTION("string collision") {
        // Janky consistency test
        for (int i = 0; i < 20; ++i) {
            map.insert("key" + std::to_string(i), "val" + std::to_string(i));
        }
        REQUIRE(map.get("key10").value() == "val10");
        REQUIRE(map.get_num_elem() == 20);
    }
}

TEST_CASE("Resize") {
    // Size 2, Load Factor 0.5 (Splits when > 1 item)
    LinearHash<int, int> map(2, 0.5);

    SECTION("Incremental split") {
        // Load 0.5
        map.insert(1, 1);
        REQUIRE(map.get_table_size() == 2);
        REQUIRE(map.get_split_ptr() == 0);

        // Load 1, trigger Split
        map.insert(2, 2);
        REQUIRE(map.get_table_size() == 3); // grew 1 bucket
        REQUIRE(map.get_split_ptr() == 1);  // pointer moved

        // Load 1,  trigger split
        map.insert(3, 3);
        REQUIRE(map.get_table_size() == 4);
        REQUIRE(map.get_split_ptr() == 0);  // pointer reset
    }

    SECTION("Data integrity") {
        // force resizes
        for (int i = 0; i < 50; ++i) {
            map.insert(i, i * 10);
        }

        REQUIRE(map.get_num_elem() == 50);

        // Check random elements for order
        REQUIRE(map.get(0).value() == 0);
        REQUIRE(map.get(25).value() == 250);
        REQUIRE(map.get(49).value() == 490);
    }
}

TEST_CASE("Remove") {
    LinearHash<std::string, int> map(4, 0.8);
    map.insert("A", 1);
    map.insert("B", 2);
    map.insert("C", 3);

    SECTION("basic") {
        bool removed = map.remove("B");
        REQUIRE(removed == true);
        REQUIRE(map.get_num_elem() == 2);
        REQUIRE_FALSE(map.in("B"));
    }

    SECTION("items doesn't exist") {
        bool removed = map.remove("Z");
        REQUIRE(removed == false);
        REQUIRE(map.get_num_elem() == 3);
    }

    SECTION("preserves other items") {
        map.remove("A");
        REQUIRE(map.in("C") == true);
        REQUIRE(map.get("C").value() == 3);
    }
}

TEST_CASE("Concurrency") {
    SECTION("Parallel inserts") {
        const int NUM_THREADS = 8;
        const int ITEMS_PER_THREAD = 5000;
        LinearHash<int, int> map(2, 0.75);

        std::vector<std::thread> threads;

        for (int t = 0; t < NUM_THREADS; ++t) {
            threads.emplace_back([&map, t]() {
                for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
                    // Unique keys: (ThreadID * 1M) + i
                    int key = (t * 1000000) + i;
                    map.insert(key, i);
                }
            });
        }

        for (auto& t : threads) t.join();

        size_t expected_total = NUM_THREADS * ITEMS_PER_THREAD;
        REQUIRE(map.get_num_elem() == expected_total);
        REQUIRE(map.in(0) == true); // Check boundary
    }

    SECTION("Mixed readers and writers") {
        LinearHash<int, int> map(16, 0.75);
        // Pre-fill
        for (int i = 0; i < 1000; ++i) map.insert(i, i);

        std::atomic<bool> running{true};
        std::atomic<int> read_errors{0};
        std::vector<std::thread> threads;

        // Reader: verify data exists
        for (int i = 0; i < 4; ++i) {
            threads.emplace_back([&]() {
                while (running) {
                    int key = rand() % 1000;
                    auto res = map.get(key);
                    // if key exist, value must match, if missing, bug.
                    if (!res.has_value() || res.value() != key) {
                         ++read_errors;
                    }
                }
            });
        }

        // writer: add new data, trigger split
        for (int i = 0; i < 4; ++i) {
            threads.emplace_back([&, i]() {
                for (int j = 0; j < 1000; ++j) {
                    int key = 10000 + (i * 10000) + j;
                    map.insert(key, j);
                }
            });
        }

        // Run
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        running = false;

        for (auto& t : threads) t.join();

        REQUIRE(read_errors == 0); //no readers saw inconsistent state
        REQUIRE(map.get_num_elem() == 1000 + (4 * 1000));
    }

    SECTION("Concurrent, delete and insert") {
        LinearHash<int, int> map(4, 0.75);
        int range = 2000;

        std::thread t1([&]() {
            for(int i=0; i<range; ++i) map.insert(i, i);
        });

        std::thread t2([&]() {
            // chase insert
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            for(int i=0; i<range; ++i) map.remove(i);
        });

        t1.join();
        t2.join();

        // does not segfault
        REQUIRE_NOTHROW(map.insert(99999, 1));
    }
}

TEST_CASE("Assorted edge") {

    SECTION("Constructor throws") {
        using LinearHashType = LinearHash<int, int>;

        // Size 0 is invalid
        REQUIRE_THROWS_AS(LinearHashType(0), std::invalid_argument);

        // Size 3 (not power of 2) is invalid
        REQUIRE_THROWS_AS(LinearHashType(3), std::invalid_argument);
    }

    SECTION("empty string as key") {
        LinearHash<std::string, int> map(2, 0.75);
        map.insert("", 42);

        REQUIRE(map.in(""));
        REQUIRE(map.get("").value() == 42);

        // Ensure other keys still work
        map.insert("valid", 100);
        REQUIRE(map.get("valid").value() == 100);
    }

    SECTION("Scale test") {
        LinearHash<int, int> map(2, 0.8);
        const int LARGE_COUNT = 100000;

        for(int i=0; i<LARGE_COUNT; ++i) {
            map.insert(i, i);
        }

        REQUIRE(map.get_num_elem() == LARGE_COUNT);
        REQUIRE(map.get_table_size() > 65536);

        REQUIRE(map.get(0).value() == 0);
        REQUIRE(map.get(LARGE_COUNT-1).value() == LARGE_COUNT-1);
    }

    SECTION("Concurrent overload key") {
        LinearHash<int, int> map(2, 0.75);
        const int NUM_THREADS = 8;
        const int OPS_PER_THREAD = 5000;

        std::vector<std::thread> threads;

        for (int t = 0; t < NUM_THREADS; ++t) {
            threads.emplace_back([&map, t]() {
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    map.insert(0, t);
                }
            });
        }

        for (auto& t : threads) t.join();

        REQUIRE(map.get_num_elem() == 1);
        REQUIRE(map.get_table_size() == 2);
    }
}

TEST_CASE("Iterator") {

    SECTION("Empty map") {
        LinearHash<int, int> map(2, 0.75);
        REQUIRE(map.begin() == map.end());

        // Loop should not run
        int count = 0;
        for (const auto& kv : map) {
            (void)kv;
            count++;
        }
        REQUIRE(count == 0);
    }

    SECTION("Standard looping") {
        LinearHash<std::string, int> map(4, 0.75);
        map.insert("A", 1);
        map.insert("B", 2);
        map.insert("C", 3);

        std::set<std::string> keys_found;
        int sum_values = 0;

        for (const auto& entry : map) {
            keys_found.insert(entry.key);
            sum_values += entry.value;
        }

        REQUIRE(keys_found.size() == 3);
        REQUIRE(keys_found.count("A") == 1);
        REQUIRE(keys_found.count("B") == 1);
        REQUIRE(keys_found.count("C") == 1);
        REQUIRE(sum_values == 6); // 1 + 2 + 3
    }

    SECTION("Skip empty bucket") {
        LinearHash<int, int> map(16, 0.75);
        map.insert(1, 100); // Only 1 item, mostly empty

        int count = 0;
        for (const auto& entry : map) {
            REQUIRE(entry.key == 1);
            REQUIRE(entry.value == 100);
            count++;
        }
        // should only run once
        REQUIRE(count == 1);
    }

    SECTION("stl compatability") {
        LinearHash<int, int> map(4, 0.75);
        map.insert(10, 100);
        map.insert(20, 200);

        // std::findif
        auto it = std::find_if(map.begin(), map.end(), [](const auto& entry) {
            return entry.key == 20;
        });

        REQUIRE(it != map.end());
        REQUIRE(it->value == 200);

        auto missing = std::find_if(map.begin(), map.end(), [](const auto& entry) {
            return entry.key == 999;
        });

        REQUIRE(missing == map.end());
    }

    SECTION("Increments") {
        LinearHash<int, int> map(4, 0.75);
        map.insert(1, 10);
        map.insert(2, 20);

        auto it = map.begin();
        REQUIRE(it != map.end());

        // post
        auto old_it = it++;
        REQUIRE(old_it != it);
        REQUIRE(old_it == map.begin()); // old still at start

        // pre
        auto& ref_it = ++it;
        REQUIRE(&ref_it == &it); // return red

        // hit end
        while(it != map.end()) {
            ++it;
        }
        REQUIRE(it == map.end());
    }
}