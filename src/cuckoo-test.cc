#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <random>
#include <assert.h>
#include <mutex>
#include <thread>

#include "cuckoo-serial.h"
#include "cuckoo-concurrent.h"
#include "cuckoo-transactional.h"

const int NUM_OPS = 10000000;
const int CAPACITY = 15000;
const int KEY_MAX = 10000;
const int INITIAL_SIZE = KEY_MAX/2;
const int NUM_THREADS = 8;

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
            ops.emplace_back(entries->at(distribution_existing_entries(generator)), 2);
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
void do_work_concurrent(CuckooConcurrentHashSet<int> *cuckoo_concurrent, std::vector<int> concurrent_entries, std::vector<Metrics> *concurrent_metrics) {
    Metrics metrics = {};
    auto ops = generate_operations(NUM_OPS, &concurrent_entries);
    // Start doing work
    long long exec_time_start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    for (auto op : ops) {
        //std::cout << "val: " << op.val << ", type: " << op.type << std::endl;
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
void do_work_transactional(CuckooTransactionalHashSet<int> *cuckoo_transactional, std::vector<int> transactional_entries, std::vector<Metrics> *transactional_metrics) {
    Metrics metrics = {};
    auto ops = generate_operations(NUM_OPS, &transactional_entries);
    // Start doing work
    long long exec_time_start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    for (auto op : ops) {
        //std::cout << "val: " << op.val << ", type: " << op.type << std::endl;
        switch (op.type) {
            // Contains
            case 0:
                if (cuckoo_transactional->contains(op.val))
                    metrics.contains_hit++;
                else
                    metrics.contains_miss++;
                break;
            // Insert
            case 1:
                if (cuckoo_transactional->add(op.val))
                    metrics.add_hit++;
                else
                    metrics.add_miss++;
                break;
            // Remove
            default:
                if (cuckoo_transactional->remove(op.val))
                    metrics.remove_hit++;
                else
                    metrics.remove_miss++;
                break;
        }
    }
    long long exec_time_end = std::chrono::high_resolution_clock::now().time_since_epoch().count();
	metrics.exec_time = exec_time_end - exec_time_start;

    std::mutex lock();
    transactional_metrics->push_back(metrics);
    std::mutex unlock();
}

int main(int argc, char *argv[]) {
    // Serial Cuckoo
    std::cout << "Starting serial cuckoo..." << std::endl;
    CuckooSerialHashSet<int> *cuckoo_serial = new CuckooSerialHashSet<int>(CAPACITY);
    // Setup hash table and pre-generate workload
    Metrics serial_metrics = {};
    auto serial_entries = generate_entries(INITIAL_SIZE);
    if (!cuckoo_serial->populate(serial_entries))
        return 0;
    auto serial_ops = generate_operations(NUM_OPS * NUM_THREADS, &serial_entries);
    do_work_serial(cuckoo_serial, serial_ops, serial_metrics);
    int serial_expected_size = INITIAL_SIZE + serial_metrics.add_hit - serial_metrics.remove_hit;
    assert(serial_expected_size == cuckoo_serial->size());
    std::cout << "Serial time (milliseconds):\t\t" << (double) serial_metrics.exec_time / 1000000.0 << std::endl;
    std::cout << std::fixed << "Serial average throughput (ops/sec):\t" << (double) (NUM_OPS * NUM_THREADS) / ((double) serial_metrics.exec_time / 1000000000.0) << std::endl;
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
    auto concurrent_entries = generate_entries(INITIAL_SIZE);
    if (!cuckoo_concurrent->populate(concurrent_entries))
        return 0;
    std::vector<std::thread> concurrent_threads = std::vector<std::thread>();
	concurrent_threads.reserve(NUM_THREADS);
    std::vector<Metrics> concurrent_metrics = std::vector<Metrics>();
    concurrent_metrics.reserve(NUM_THREADS);
    for (int thread = 0; thread < NUM_THREADS; thread++) {
        concurrent_threads.push_back(std::thread([&](){do_work_concurrent(cuckoo_concurrent, concurrent_entries, &concurrent_metrics);}));
    }
    for (int thread = 0; thread < NUM_THREADS; thread++) {
        concurrent_threads[thread].join();
    }
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
    int concurrent_expected_size = INITIAL_SIZE + total_concurrent_metrics.add_hit - total_concurrent_metrics.remove_hit;
    assert(concurrent_expected_size == cuckoo_concurrent->size());
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
    CuckooTransactionalHashSet<int> *cuckoo_transactional = new CuckooTransactionalHashSet<int>(CAPACITY);
    auto transactional_entries = generate_entries(INITIAL_SIZE);
    if (!cuckoo_transactional->populate(transactional_entries))
        return 0;
    std::vector<std::thread> transactional_threads = std::vector<std::thread>();
	transactional_threads.reserve(NUM_THREADS);
    std::vector<Metrics> transactional_metrics = std::vector<Metrics>();
    transactional_metrics.reserve(NUM_THREADS);
    for (int thread = 0; thread < NUM_THREADS; thread++) {
        transactional_threads.push_back(std::thread([&](){do_work_transactional(cuckoo_transactional, transactional_entries, &transactional_metrics);}));
    }
    for (int thread = 0; thread < NUM_THREADS; thread++) {
        transactional_threads[thread].join();
    }
    Metrics total_transactional_metrics = {};
    if (transactional_metrics.size() != NUM_THREADS)
        std::cerr << "Transactional metrics is incorrect size: " << transactional_metrics.size() << std::endl;
    for (int thread = 0; thread < NUM_THREADS; thread++) {
        double exec_time = (double) transactional_metrics[thread].exec_time / (double) 1000000;
        std::cout << "Time to execute (milliseconds):\t\t\t" << exec_time << std::endl;
        total_transactional_metrics.exec_time += (exec_time - total_transactional_metrics.exec_time) / (thread + 1);
        std::cout << "Transactional contains hit: " << transactional_metrics[thread].contains_hit << std::endl;
        std::cout << "Transactional contains miss: " << transactional_metrics[thread].contains_miss << std::endl;
        std::cout << "Transactional add hit: " << transactional_metrics[thread].add_hit << std::endl;
        std::cout << "Transactional add miss: " << transactional_metrics[thread].add_miss << std::endl;
        std::cout << "Transactional remove hit: " << transactional_metrics[thread].remove_hit << std::endl;
        std::cout << "Transactional remove miss: " << transactional_metrics[thread].remove_miss << std::endl << std::endl;
        total_transactional_metrics.contains_hit += transactional_metrics[thread].contains_hit;
        total_transactional_metrics.contains_miss += transactional_metrics[thread].contains_miss;
        total_transactional_metrics.add_hit += transactional_metrics[thread].add_hit;
        total_transactional_metrics.add_miss += transactional_metrics[thread].add_miss;
        total_transactional_metrics.remove_hit += transactional_metrics[thread].remove_hit;
        total_transactional_metrics.remove_miss += transactional_metrics[thread].remove_miss;
    }
    int transactional_expected_size = INITIAL_SIZE + total_transactional_metrics.add_hit - total_transactional_metrics.remove_hit;
    assert(transactional_expected_size == cuckoo_transactional->size());
    std::cout << "Average Transactional exec_time (milliseconds):\t\t" << total_transactional_metrics.exec_time << std::endl;
    std::cout << std::fixed << "Average Transactional total throughput (ops/sec):\t\t" << (double) (NUM_OPS * NUM_THREADS) / (total_transactional_metrics.exec_time / 1000.0) << std::endl;
    std::cout << "Transactional total contains hit: " << total_transactional_metrics.contains_hit << std::endl;
    std::cout << "Transactional total contains miss: " << total_transactional_metrics.contains_miss << std::endl;
    std::cout << "Transactional total add hit: " << total_transactional_metrics.add_hit << std::endl;
    std::cout << "Transactional total add miss: " << total_transactional_metrics.add_miss << std::endl;
    std::cout << "Transactional total remove hit: " << total_transactional_metrics.remove_hit << std::endl;
    std::cout << "Transactional total remove miss: " << total_transactional_metrics.remove_miss << std::endl;
    delete cuckoo_transactional;
}