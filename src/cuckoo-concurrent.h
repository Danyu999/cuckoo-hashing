#include <vector>
#include <stdlib.h>
#include <iostream>
#include <functional>
#include <ctime>
#include <list>
#include <mutex>

template <class T>
class CuckooConcurrentHashSet {
    const int PROBE_SIZE = 8;
    const int THRESHOLD = PROBE_SIZE/2;
    int limit;
    size_t salt0;
    size_t salt1;
    int capacity;
    std::vector<std::vector<std::list<T>>> table;
    // Note: locks cannot be resized
    std::vector<std::vector<std::recursive_mutex>> locks;

    template <class T>
    std::list<int>::iterator find(const std::list<T> &l, const T &value) {
        return std::find(l.begin(), l.end(), value);
    }

    // Taken from boost hash_combine
    template <class D>
    inline void hash_combine(std::size_t& seed, const D& v) {
        std::hash<D> hasher;
        seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }

    int hash0(T val) {
        size_t seed = 0;
        hash_combine(seed, val);
        hash_combine(seed, salt0);
        return seed;
    }

    int hash1(T val) {
        size_t seed = 0;
        hash_combine(seed, val);
        hash_combine(seed, salt1);
        return seed;
    }

    bool relocate(int i, int hi) {
        int hj = 0;
        int j = 1 - i;
        for (int round = 0; round < limit; round++) {
            std::list<int> i_set = table[i][hi];
            T val = i_set.front();
            switch (i) {
                case 0: hj = hash1(val) % capacity; break;
                case 1: hj = hash0(val) % capacity; break;
            }
            acquire(val);
            std::list<int> j_set = table[j][hj];
            if ((auto it = find(i_set, val) != i_set.end()) {
                i_set.erase(it);
                if (j_set.size() < THRESHOLD) {
                    j_set.push_back(val);
                    release(val);
                    return true;
                } else if (j_set.size() < PROBE_SIZE) {
                    j_set.push_back(val);
                    i = 1 - i;
                    hi = hj;
                    j = 1 - j;
                    release(val);
                } else {
                    i_set.push_back(val);
                    release(val);
                    return false;
                }
            } else if (i_set.size() >= THRESHOLD) {
                release(val);
                continue;
            } else {
                release(val);
                return true;
            }
        }
        return false;
    }

    void acquire(T val) {
        locks[0][hash0(val) % locks[0].size()].lock();
        locks[1][hash1(val) % locks[1].size()].lock();
    }

    void release(T val) {
        locks[0][hash0(val) % locks[0].size()].unlock();
        locks[1][hash1(val) % locks[1].size()].unlock();
    }

    /**
     * Resizes the table to be twice as big. Changes salt0 and salt1.
     */
    void resize() {
        //std::cout << "resize" << std::endl;
        int oldCapacity = capacity;
        // Since we have consistent ordering when acquiring locks, we only need
        // to acquire the locks for table0.
        for (auto lock : locks[0]) {
            lock.lock();
        }

        // Another resize happened
        if (capacity != oldCapacity)
            return;
        
        // Get new salt values to change the hashes
        hash_combine(salt0, time(NULL));
        hash_combine(salt1, time(NULL));

        capacity *= 2;
        limit *= 2;
        std::vector<std::vector<std::list<T>>> old_table(table);
        table.clear();
        for (int i = 0; i < 2; i++) {
            std::vector<std::list<T>> row;
            for (int j = 0; j < capacity; j++) {
                std::list<T> probe_set;
                row.push_back(probe_set);
            }
            table.push_back(row);
        }

        // Add the elements back into the bigger table
        for (auto row : old_table) {
            for (auto probe_set : row) {
                for (auto entry : probe_set) {
                    add(entry->val); //TODO: what if this add call calls resize again? segfault
                    delete entry;
                }
            }
        }

        // Release locks
        for (auto lock : locks[0])
            lock.unlock();
    }

    public:
        CuckooConcurrentHashSet(int capacity) : capacity(capacity), limit(capacity/2) {
            for (int i = 0; i < 2; i++) {
                std::vector<std::list<T>> row;
                std::vector<std::recursive_mutex> locks_row;
                for (int j = 0; j < capacity; j++) {
                    std::list<T> probe_set;
                    row.push_back(probe_set);
                    locks_row.emplace_back(new std::recursive_mutex());
                }
                table.push_back(row);
                locks.push_back(locks_row);
            }
            salt0 = time(NULL);
            salt1 = salt0;
            hash_combine(salt1, capacity);
        }

        ~CuckooConcurrentHashSet() {
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < capacity; j++) {
                    for (auto entry : table[i][j]) {
                        delete entry;
                    }
                    delete locks[i][j];
                    table[i][j].clear();
                }
                table[i].clear();
            }
            table.clear();
        }

        /** 
         * Adds val
         * return: true if add was successful
         */
        bool add(T val) {
            acquire(val);
            int h0 = hash0(val) % capacity, h1 = hash1(val) % capacity;
            int i = -1, h = -1;
            bool mustResize = false;
            if (contains(val)) {
                release(val);
                return false;
            }
            std::list<T> set0 = table[0][h0];
            std::list<T> set1 = table[1][h1];
            if (set0.size() < THRESHOLD) {
                set0.push_back(val);
                release(val);
                return true;
            } else if (set1.size() < THRESHOLD) {
                set1.push_back(val);
                release(val);
                return true;
            } else if (set0.size() < PROBE_SIZE) {
                set0.push_back(val);
                i = 0;
                h = h0;
            } else if (set1.size() < PROBE_SIZE) {
                set1.push_back(val);
                i = 1;
                h = h1;
            } else {
                mustResize = true;
            }
            release(val);

            if (mustResize) {
                resize();
                add(val);
            } else if (!relocate(i, h)) {
                resize();
            }
            return true;
        }

        /** 
         * Removes val
         * return: true if remove was successful
         */
        bool remove(T val) {
            acquire(val);
            std::list<T> set0 = table[0][hash0(val) % capacity];
            if ((auto it = find(set0, val)) != set0.end()) {
                set0.erase(it);
                release(val);
                return true;
            } else {
                std::list<T> set1 = table[1][hash1(val) % capacity];
                if ((auto it = find(set1, val)) != set1.end()) {
                    set1.erase(it);
                    release(val);
                    return true;
                }
            }
            release(val);
            return false;
        }

        /** 
         * Checks if the table contains val
         * return: true if the table contains val
         */
        bool contains(T val) {
            //std::cout << "contains" << std::endl;
            acquire(val);
            std::list<T> set0 = table[0][hash0(val) % capacity];
            if ((auto it = find(set0, val)) != set0.end()) {
                release(val);
                return true;
            } else {
                std::list<T> set1 = table[1][hash1(val) % capacity];
                if ((auto it = find(set1, val)) != set1.end()) {
                    release(val);
                    return true;
                }
            }
            release(val);
            return false;
        }

        /**
         * Counts the number of elements in the table
         * return: The number of elements in the table
         */
        int size() {
            int size = 0;
            for (auto row : table) {
                for (auto probe_set : row) {
                    for (auto entry : probe_set) {
                        size++;
                    }
                }
            }
            return size;
        }

        /**
         * Populates the table to some predetermined size
         * Thread non-safe!
         * return: true if successful
         */
        bool populate(std::vector<T> entries) {
            for (T entry : entries) {
                if (!add(entry)) {
                    std::cout << "Duplicate entry attempted for populate!" << std::endl;
                    return false;
                }
            }
            return true;
        }
};