#include <vector>

template <class T>
class CuckooSerialHashSet {
    struct Entry {
        T val;
    };

    int capacity;
    std::vector<std::vector<Entry>> table;

    public:
        CuckooSerialHashSet(int capacity) : capacity(capacity) {
            for (int i = 0; i < 2; i++) {
                table.emplace_back({capacity});
            }
        }
};