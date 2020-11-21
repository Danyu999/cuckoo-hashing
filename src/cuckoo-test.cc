#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <random>
#include <assert.h>

#include "cuckoo-serial.h"
#include "cuckoo-concurrent.h"
#include "cuckoo-transactional.h"

const int NUM_OPS = 1000000;
const int CAPACITY = 1000;
const int KEY_MAX = 10000;
const int NUM_THREADS = 1;

struct Operation {
    int val;
    int type; // 0 => contains, 1 => add, 2 => remove
    Operation(int val, int type) : val(val), type(type) {}
};

struct Metrics {
    long long exec_time = 0;
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

std::vector<Operation> generate_operations(int num_ops, std::vector<int> *entries) {
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
        std::uniform_int_distribution<int> distribution_existing_entries(0, entries->size()-1);
        int which_op = distribution_percentage(generator);
        if (which_op <= 50) {
            // contains
            ops.emplace_back(distribution_entries(generator), 0);
        } else if (which_op <= 75) {
            // add
            int entry = distribution_entries(generator);
            entries->push_back(entry);
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
void do_work_concurrent(CuckooConcurrentHashSet<int> *cuckoo_concurrent, std::vector<Operation> ops, std::vector<Metrics> *concurrent_metrics) {
    Metrics metrics = {};
    // Start doing work
    long long exec_time_start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    for (auto op : ops) {
        // std::cout << "val: " << op.val << ", type: " << op.type << std::endl;
        switch (op.type) {
            // Contains
            case 0:
                if (cuckoo_concurrent->contains(op.val))
                    metrics.contains_hit++;
                else
                    metrics.contains_miss++;
                break;
            // Insert
            case 1:
                if (cuckoo_concurrent->add(op.val))
                    metrics.add_hit++;
                else
                    metrics.add_miss++;
                break;
            // Remove
            default:
                if (cuckoo_concurrent->remove(op.val))
                    metrics.remove_hit++;
                else
                    metrics.remove_miss++;
                break;
        }
    }
    long long exec_time_end = std::chrono::high_resolution_clock::now().time_since_epoch().count();
	metrics.exec_time = exec_time_end - exec_time_start;

    std::mutex lock();
    concurrent_metrics->push_back(metrics);
    std::mutex unlock();
}

/**
 * Runs a workload for cuckoo transactional
 */
void do_work_transactional() {

}

int main(int argc, char *argv[]) {
    // Serial Cuckoo
    std::cout << "Starting serial cuckoo..." << std::endl;
    CuckooSerialHashSet<int> *cuckoo_serial = new CuckooSerialHashSet<int>(CAPACITY);
    // Setup hash table and pre-generate workload
    Metrics serial_metrics = {};
    auto serial_entries = generate_entries(CAPACITY/3);
    if (!cuckoo_serial->populate(serial_entries))
        return 0;
    auto serial_ops = generate_operations(NUM_OPS, &serial_entries);
    do_work_serial(cuckoo_serial, serial_ops, serial_metrics);
    int expected_size = CAPACITY/3 + serial_metrics.add_hit - serial_metrics.remove_hit;
    assert(expected_size == cuckoo_serial->size());
    std::cout << "Serial time (milliseconds):\t\t" << (double) serial_metrics.exec_time / 1000000.0 << std::endl;
    std::cout << std::fixed << "Serial average throughput (ops/sec):\t" << (double) NUM_OPS / ((double) serial_metrics.exec_time / 1000000000.0) << std::endl;
    std::cout << "Serial contains hit: " << serial_metrics.contains_hit << std::endl;
    std::cout << "Serial contains miss: " << serial_metrics.contains_miss << std::endl;
    std::cout << "Serial add hit: " << serial_metrics.add_hit << std::endl;
    std::cout << "Serial add miss: " << serial_metrics.add_miss << std::endl;
    std::cout << "Serial remove hit: " << serial_metrics.remove_hit << std::endl;
    std::cout << "Serial remove miss: " << serial_metrics.remove_miss << std::endl << std::endl;
    delete cuckoo_serial;

    // Concurrent Cuckoo
    std::cout << "Starting concurrent cuckoo..." << std::endl;
    CuckooConcurrentHashSet<int> *cuckoo_concurrent = new CuckooConcurrentHashSet<int>(CAPACITY);
    auto concurrent_entries = generate_entries(CAPACITY/3);
    if (!cuckoo_concurrent->populate(concurrent_entries))
        return 0;
    std::vector<std::vector<Operation>> concurrent_ops = std::vector<std::vector<Operation>>();
    concurrent_ops.reserve(NUM_THREADS);
    std::vector<std::thread> concurrent_threads = std::vector<std::thread>();
	concurrent_threads.reserve(NUM_THREADS);
    std::vector<Metrics> concurrent_metrics = std::vector<Metrics>();
    concurrent_metrics.reserve(NUM_THREADS);
    for (int thread = 0; thread < NUM_THREADS; thread++)
        concurrent_ops.emplace_back(generate_operations(NUM_OPS, &concurrent_entries));
    for (int thread = 0; thread < NUM_THREADS; thread++)
        concurrent_threads.push_back(std::thread([&](){do_work_concurrent(cuckoo_concurrent, concurrent_ops[thread], &concurrent_metrics);}));
    for (int thread = 0; thread < NUM_THREADS; thread++)
        concurrent_threads[thread].join();
    int expected_size = CAPACITY/3 + serial_metrics.add_hit - serial_metrics.remove_hit;
    assert(expected_size == cuckoo_serial->size());
    Metrics total_concurrent_metrics = {};
    if (concurrent_metrics.size() != NUM_THREADS)
        std::cerr << "Concurrent metrics is incorrect size: " << concurrent_metrics.size() << std::endl;
    for (int thread = 0; thread < NUM_THREADS; thread++) {
        double exec_time = (double) concurrent_metrics[thread].exec_time / (double) 1000000;
        std::cout << "Time to execute (milliseconds):\t\t\t" << exec_time << std::endl;
        total_concurrent_metrics.exec_time += (exec_time - total_concurrent_metrics.exec_time) / (thread + 1);
        std::cout << "Concurrent contains hit: " << concurrent_metrics[thread].contains_hit << std::endl;
        std::cout << "Concurrent contains miss: " << concurrent_metrics[thread].contains_miss << std::endl;
        std::cout << "Concurrent add hit: " << concurrent_metrics[thread].add_hit << std::endl;
        std::cout << "Concurrent add miss: " << concurrent_metrics[thread].add_miss << std::endl;
        std::cout << "Concurrent remove hit: " << concurrent_metrics[thread].remove_hit << std::endl;
        std::cout << "Concurrent remove miss: " << concurrent_metrics[thread].remove_miss << std::endl << std::endl;
        total_concurrent_metrics.contains_hit += concurrent_metrics[thread].contains_hit;
        total_concurrent_metrics.contains_miss += concurrent_metrics[thread].contains_miss;
        total_concurrent_metrics.add_hit += concurrent_metrics[thread].add_hit;
        total_concurrent_metrics.add_miss += concurrent_metrics[thread].add_miss;
        total_concurrent_metrics.remove_hit += concurrent_metrics[thread].remove_hit;
        total_concurrent_metrics.remove_miss += concurrent_metrics[thread].remove_miss;
    }
    std::cout << "Average concurrent exec_time (milliseconds):\t\t" << total_concurrent_metrics.exec_time << std::endl;
    std::cout << std::fixed << "Average concurrent total throughput (ops/sec):\t\t" << (double) (NUM_OPS * NUM_THREADS) / (total_concurrent_metrics.exec_time / 1000.0) << std::endl;
    std::cout << "Concurrent total contains hit: " << total_concurrent_metrics.contains_hit << std::endl;
    std::cout << "Concurrent total contains miss: " << total_concurrent_metrics.contains_miss << std::endl;
    std::cout << "Concurrent total add hit: " << total_concurrent_metrics.add_hit << std::endl;
    std::cout << "Concurrent total add miss: " << total_concurrent_metrics.add_miss << std::endl;
    std::cout << "Concurrent total remove hit: " << total_concurrent_metrics.remove_hit << std::endl;
    std::cout << "Concurrent total remove miss: " << total_concurrent_metrics.remove_miss << std::endl;
    delete cuckoo_concurrent;

    // Transactional Cuckoo
    //CuckooTransactionalHashSet<int> *cuckoo_transactional = new CuckooTransactionalHashSet<int>(CAPACITY);
}