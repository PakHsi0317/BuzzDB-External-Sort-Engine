#include "external_sort/external_sort.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <thread>
#include <vector>

#include "storage/file.h"

#define UNUSED(p) ((void)(p))

namespace buzzdb {

void external_sort(File &input, size_t num_values, File &output, size_t mem_size) {
    // Input Arguments:
    // @param input      Input file reference which contains the integers to be sorted. 
    //                   This file should not be written to.
    // @param num_values The number of integers that should be sorted from the
    //                   input.
    // @param output     Output file reference that should contain the sorted values in the
    //                   end.
    // @param mem_size   The maximum amount of main-memory in bytes that
    //                   should be used for internal sorting.

    /* ========================================================================
     *  GUIDE
     * ========================================================================
     * 
     * External sort is used when data doesn't fit in memory. The algorithm
     * consists of two main phases, that you are expected to implement.
     * Given below are few hints to help you in case you are stuck.
     * 
     * PHASE 1: CREATE SORTED RUNS
     * ---------------------------
     * Here you need to calculate how many values can fit in memory,
     * and based on that, process the input file in chunks.
     * 
     * IMPLEMENTATION NOTES:
     *  - Each value is a uint64_t (8 bytes)
     *  - How many values can you fit in mem_size bytes?
     *  - Remember that integers are stored as 8-byte little-endian values.
     * 
     * PHASE 2: MERGE THE RUNS
     * -----------------------
     * Here you need to merge the sorted runs into the output file.
     * 
     * IMPLEMENTATION NOTES:
     *  - Don't forget to resize the output file before writing.
     *  - Make sure to handle edge cases (empty input etc.)
     *  - Do you need to track the current read position for each temp file?
     * 
     * ========================================================================
     */
    
    // TODO: Remove these UNUSED statements and implement the algorithm above
    // UNUSED(input);
    // UNUSED(num_values);
    // UNUSED(output);
    // UNUSED(mem_size);
    if (num_values == 0) {
        output.resize(0);
        return;
    }

    // Each value is 8 bytes.
    constexpr size_t kValueBytes = sizeof(uint64_t);
    const size_t total_bytes = num_values * kValueBytes;
    output.resize(total_bytes);

    // If mem_size is extremely small, we still must make progress.
    // We also want enough memory left for the K-way merge bookkeeping.
    // We'll conservatively budget a fixed overhead per run during merge.
    const size_t max_values_in_memory = std::max<size_t>(1, mem_size / kValueBytes);

    // Conservative per-run overhead during merge (priority queue node + offsets, etc.).
    // This keeps us within the hard memory cap in the tests.
    constexpr size_t kPerRunMergeOverheadBytes = 64;
    const size_t max_runs_merge = std::max<size_t>(1, mem_size / kPerRunMergeOverheadBytes);

    // Choose a run size that (a) fits in memory for sorting, and (b) keeps number of runs
    // small enough so the K-way merge fits in memory.
    const size_t min_run_size_for_merge = (num_values + max_runs_merge - 1) / max_runs_merge;
    const size_t run_values = std::max<size_t>(
        1,
        std::min(max_values_in_memory, min_run_size_for_merge));

    // ----------------------
    // Phase 1: Create sorted runs
    // ----------------------
    std::vector<std::unique_ptr<File>> runs;
    std::vector<size_t> run_sizes;  // number of values in each run
    runs.reserve((num_values + run_values - 1) / run_values);
    run_sizes.reserve(runs.capacity());

    for (size_t base = 0; base < num_values; base += run_values) {
        const size_t count = std::min(run_values, num_values - base);
        const size_t bytes = count * kValueBytes;

        auto buf = std::make_unique<uint64_t[]>(count);
        input.read_block(base * kValueBytes, bytes, reinterpret_cast<char *>(buf.get()));

        std::sort(buf.get(), buf.get() + count);

        std::unique_ptr<File> tmp = std::move(File::make_temporary_file());
        tmp->resize(bytes);
        tmp->write_block(reinterpret_cast<char *>(buf.get()), 0, bytes);

        runs.emplace_back(std::move(tmp));
        run_sizes.emplace_back(count);
    }

    // ----------------------
    // Phase 2: K-way merge
    // ----------------------
    struct Node {
        uint64_t value;
        size_t run_id;
    };
    struct Greater {
        bool operator()(const Node &a, const Node &b) const { return a.value > b.value; }
    };

    const size_t k = runs.size();
    std::vector<size_t> next_index(k, 0);  // next value index to read from each run

    // Pre-reserve heap storage to avoid reallocations.
    std::vector<Node> heap_storage;
    heap_storage.reserve(k);
    std::priority_queue<Node, std::vector<Node>, Greater> pq(Greater(), std::move(heap_storage));

    // Initialize: push the first element of each run.
    for (size_t i = 0; i < k; ++i) {
        if (run_sizes[i] == 0) continue;
        uint64_t v = 0;
        runs[i]->read_block(0, kValueBytes, reinterpret_cast<char *>(&v));
        pq.push(Node{.value = v, .run_id = i});
        next_index[i] = 1;
    }

    size_t out_idx = 0;
    while (!pq.empty()) {
        Node cur = pq.top();
        pq.pop();

        output.write_block(reinterpret_cast<char *>(&cur.value), out_idx * kValueBytes, kValueBytes);
        ++out_idx;

        const size_t r = cur.run_id;
        if (next_index[r] < run_sizes[r]) {
            uint64_t v = 0;
            runs[r]->read_block(next_index[r] * kValueBytes, kValueBytes, reinterpret_cast<char *>(&v));
            ++next_index[r];
            pq.push(Node{.value = v, .run_id = r});
        }
    }
}

}  // namespace buzzdb
