#include <vector>
#include <stdlib.h>
#include <iostream>
#include <functional>
#include <ctime>

template <class T>
class CuckooSerialHashSet {

    // Wrapper class for entries to allow for nullptr to be the default
    struct Entry {
        T val;
        Entry(T val) : val(val) {}
    };

    int limit;
    size_t salt0;
    size_t salt1;
    int capacity;
    std::vector<std::vector<Entry*>> table;

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
        return seed % capacity;
    }

    int hash1(T val) {
        size_t seed = 0;
        hash_combine(seed, val);
        hash_combine(seed, salt1);
        return seed % capacity;
    }

    /**
     * Resizes the table to be twice as big. Changes salt0 and salt1.
     */
    void resize() {
        std::cout << "resize" << std::endl;
        // Get new salt values to change the hashes
        hash_combine(salt0, time(NULL));
        hash_combine(salt1, time(NULL));

        capacity *= 2;
        limit *= 2;
        std::vector<std::vector<Entry*>> old_table(table);
        table.clear();
        for (int i = 0; i < 2; i++) {
            std::vector<Entry*> row;
            row.assign(capacity, nullptr);
            table.push_back(row);
        }

        // Add the elements back into the bigger table
        for (auto row : old_table) {
            for (auto entry : row) {
                if (entry != nullptr) {
                    add(entry->val); //TODO: what if this add call calls resize again? segfault
                    delete entry;
                }
            }
        }
    }

    public:
        CuckooSerialHashSet(int capacity) : capacity(capacity), limit(capacity/2) {
            for (int i = 0; i < 2; i++) {
                std::vector<Entry*> row;
                row.assign(capacity, nullptr);
                table.push_back(row);
            }
            salt0 = time(NULL);
            salt1 = salt0;
            hash_combine(salt1, capacity);
        }

        ~CuckooSerialHashSet() {
            for (auto row : table) {
                for (auto entry : row) {
                    if (entry != nullptr) {
                        delete entry;
                    }
                }
                row.clear();
            }
            table.clear();
        }

        /**
         * Swaps the element at table[table_index][index] with val.
         * return: The old val
         */
        Entry* swap(int table_index, int index, Entry *val) {
            Entry *swap_val = table[table_index][index];
            table[table_index][index] = val;
            return swap_val;
        }

        /** 
         * Adds val
         * return: true if add was successful
         */
        bool add(T val) {
            if (contains(val)) {
                return false;
            }
            Entry *value = new Entry(val);
            for (int i = 0; i < limit; i++) {
                if ((value = swap(0, hash0(value->val), value)) == nullptr) {
                    return true;
                } else if ((value = swap(1, hash1(value->val), value)) == nullptr) {
                    return true;
                }
            }
            resize();
            add(value->val);
        }

        /** 
         * Removes val
         * return: true if remove was successful
         */
        bool remove(T val) {
            int index0 = hash0(val);
            int index1 = hash1(val);
            if (table[0][index0] != nullptr && table[0][index0]->val == val) {
                delete table[0][index0];
                table[0][index0] = nullptr;
                return true;
            } else if (table[1][index1] != nullptr && table[1][index1]->val == val) {
                delete table[1][index1];
                table[1][index1] = nullptr;
                return true;
            }
            return false;
        }

        /** 
         * Checks if the table contains val
         * return: true if the table contains val
         */
        bool contains(T val) {
            //std::cout << "contains" << std::endl;
            int index0 = hash0(val);
            int index1 = hash1(val);
            if (table[0][index0] != nullptr && table[0][index0]->val == val) {
                return true;
            } else if (table[1][index1] != nullptr && table[1][index1]->val == val) {
                return true;
            }
            return false;
        }

        /**
         * Counts the number of elements in the table
         * return: The number of elements in the table
         */
        int size() {
            int size = 0;
            for (auto row : table) {
                for (auto entry : row) {
                    if (entry != nullptr) {
                        size++;
                    }
                }
            }
            return size;
        }

        /**
         * Populates the table to some predetermined size
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