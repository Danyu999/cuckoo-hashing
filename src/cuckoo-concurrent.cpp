class CuckooConcurrentHashSet<T> {
    private:
        int capacity;
        vector<T> table[][];

    public:
        CuckooSerialHashSet<T>(int size) {
            capacity = size;
            table[2][capacity];
            for (int i = 0; i < 2; i++) {
                
            }
        }
}