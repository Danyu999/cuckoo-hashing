#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <random>
#include <assert.h>

#include "cuckoo-serial.h"
//#include "cuckoo-concurrent.h"
//#include "cuckoo-transactional.h"

const int NUM_OPS = 1000000;
const int CAPACITY = 1000;
const int KEY_MAX = 10000;

struct Operation {
    int val;
    int type; // 0 => contains, 1 => add, 2 => remove
    Operation(int val, int type) : val(val), type(type) {}
};

struct Metrics {
    long long exec_time;
    int contains_hit = 0;
    int contains_miss = 0;
    int add_hit = 0;
    int add_miss = 0;
    int remove_hit = 0;
    int remove_miss = 0;
};

/**
 * Generates num_entires unique entries
 */
std::vector<int> generate_entries(int num_entries) {
    auto seed = std::chrono::high_resolution_clock::now()
            .time_since_epoch()
            .count();
	static thread_local std::mt19937 generator(seed);
    std::uniform_int_distribution<int> entry_generator(0, KEY_MAX);

    std::unordered_set<int> entries;
    while (entries.size() < num_entries) {
        entries.insert(entry_generator(generator));
    }
    return {entries.begin(), entries.end()};
}

std::vector<Operation> generate_operations(int num_ops, std::vector<int> entries) {
    // 50% contains, 25% add, 25% remove
    // With guarenteed success for add and remove operations, size of table should stay
    // relatively the same.
    auto seed = std::chrono::high_resolution_clock::now()
            .time_since_epoch()
            .count();
	static thread_local std::mt19937 generator(seed);
    std::uniform_int_distribution<int> distribution_percentage(0, 100);
    std::uniform_int_distribution<int> distribution_entries(0, KEY_MAX);
    std::vector<Operation> ops;
    for (int i = 0; i < num_ops; i++) {
        std::uniform_int_distribution<int> distribution_existing_entries(0, entries.size()-1);
        int which_op = distribution_percentage(generator);
        if (which_op <= 50) {
            // contains
            ops.emplace_back(distribution_entries(generator), 0);
        } else if (which_op <= 75) {
            // add
            int entry = distribution_entries(generator);
            entries.push_back(entry);
            ops.emplace_back(entry, 1);
        } else {
            // remove
            ops.emplace_back(entries[distribution_existing_entries(generator)], 2);
        }
    }
    return ops;
}

/**
 * Runs a workload for cuckoo serial
 */
void do_work_serial(CuckooSerialHashSet<int> *cuckoo_serial, std::vector<Operation> ops, Metrics &metrics) {
    // Start doing work
    long long exec_time_start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    for (auto op : ops) {
        // std::cout << "val: " << op.val << ", type: " << op.type << std::endl;
        switch (op.type) {
            // Contains
            case 0:
                if (cuckoo_serial->contains(op.val))
                    metrics.contains_hit++;
                else
                    metrics.contains_miss++;
                break;
            // Insert
            case 1:
                if (cuckoo_serial->add(op.val))
                    metrics.add_hit++;
                else
                    metrics.add_miss++;
                break;
            // Remove
            default:
                if (cuckoo_serial->remove(op.val))
                    metrics.remove_hit++;
                else
                    metrics.remove_miss++;
                break;
        }
    }
    long long exec_time_end = std::chrono::high_resolution_clock::now().time_since_epoch().count();
	metrics.exec_time = exec_time_end - exec_time_start;
}

/**
 * Runs a workload for cuckoo concurrent
 */
void do_work_concurrent() {

}

/**
 * Runs a workload for cuckoo transactional
 */
void do_work_transactional() {

}

int main(int argc, char *argv[]) {
    auto entries = generate_entries(CAPACITY/3);

    // Serial Cuckoo
    CuckooSerialHashSet<int> *cuckoo_serial = new CuckooSerialHashSet<int>(CAPACITY);
    // Setup hash table and pre-generate workload
    Metrics serial_metrics = {};
    if (!cuckoo_serial->populate(entries))
        return 0;
    auto ops = generate_operations(NUM_OPS, entries);
    std::cout << "Starting serial cuckoo..." << std::endl;
    do_work_serial(cuckoo_serial, ops, serial_metrics);
    int expected_size = CAPACITY/3 + serial_metrics.add_hit - serial_metrics.remove_hit;
    assert(expected_size == cuckoo_serial->size());
    std::cout << "Serial time (milliseconds):\t\t" << (double) serial_metrics.exec_time / 1000000.0 << std::endl;
    std::cout << std::fixed << "Serial average throughput (ops/sec):\t" << (double) NUM_OPS / ((double) serial_metrics.exec_time / 1000000000.0) << std::endl;
    std::cout << "Serial contains hit: " << serial_metrics.contains_hit << std::endl;
    std::cout << "Serial contains miss: " << serial_metrics.contains_miss << std::endl;
    std::cout << "Serial add hit: " << serial_metrics.add_hit << std::endl;
    std::cout << "Serial add miss: " << serial_metrics.add_miss << std::endl;
    std::cout << "Serial remove hit: " << serial_metrics.remove_hit << std::endl;
    std::cout << "Serial remove miss: " << serial_metrics.remove_miss << std::endl;
    delete cuckoo_serial;

    // Concurrent Cuckoo
    //CuckooConcurrentHashSet<int> *cuckoo_concurrent = new CuckooConcurrentHashSet<int>(CAPACITY);

    // Transactional Cuckoo
    //CuckooTransactionalHashSet<int> *cuckoo_transactional = new CuckooTransactionalHashSet<int>(CAPACITY);


    // int capacity = 15;
    // int val = 222;
    // cuckoo_serial->add(val);
    // std::cout << "test successful1: " << cuckoo_serial->contains(val) << std::endl;
    // cuckoo_serial->resize();
    // std::cout << "test successful2: " << cuckoo_serial->contains(val) << std::endl;
    // cuckoo_serial->remove(val);
    // std::cout << "test successful3: " << cuckoo_serial->contains(val) << std::endl;
}