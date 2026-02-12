#include "storage/btree_index.hpp"

namespace cloudsql {
namespace storage {

bool BTreeIndex::Iterator::next(Entry& out_entry) { 
    (void)out_entry; 
    return false; 
}

bool BTreeIndex::create() { return true; }
bool BTreeIndex::open() { return true; }
void BTreeIndex::close() {}
bool BTreeIndex::drop() { return true; }

bool BTreeIndex::insert(const common::Value& key, HeapTable::TupleId tuple_id) { 
    (void)key; 
    (void)tuple_id; 
    return true; 
}

bool BTreeIndex::remove(const common::Value& key, HeapTable::TupleId tuple_id) { 
    (void)key; 
    (void)tuple_id; 
    return true; 
}

std::vector<HeapTable::TupleId> BTreeIndex::search(const common::Value& key) const { 
    (void)key; 
    return {}; 
}

std::vector<HeapTable::TupleId> BTreeIndex::range_search(
    const std::unique_ptr<common::Value>& min_key,
    const std::unique_ptr<common::Value>& max_key
) const { 
    (void)min_key; 
    (void)max_key; 
    return {}; 
}

void BTreeIndex::get_stats(uint64_t& num_entries, int& depth, uint32_t& num_pages) const {
    num_entries = 0; depth = 0; num_pages = 0;
}

bool BTreeIndex::verify() const { return true; }
bool BTreeIndex::exists() const { return false; }

}  // namespace storage
}  // namespace cloudsql
