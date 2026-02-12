#include "storage/heap_table.hpp"

namespace cloudsql {
namespace storage {

bool HeapTable::Iterator::next(executor::Tuple& out_tuple) { 
    (void)out_tuple; 
    return false; 
}

HeapTable::TupleId HeapTable::insert(const executor::Tuple& tuple) { 
    (void)tuple; 
    return TupleId(); 
}

bool HeapTable::remove(const TupleId& tuple_id) { 
    (void)tuple_id; 
    return true; 
}

bool HeapTable::update(const TupleId& tuple_id, const executor::Tuple& tuple) { 
    (void)tuple_id; 
    (void)tuple; 
    return true; 
}

bool HeapTable::get(const TupleId& tuple_id, executor::Tuple& out_tuple) const { 
    (void)tuple_id; 
    (void)out_tuple; 
    return false; 
}

uint64_t HeapTable::tuple_count() const { return 0; }
uint64_t HeapTable::file_size() const { return 0; }

bool HeapTable::exists() const { return false; }
bool HeapTable::create() { return true; }
bool HeapTable::drop() { return true; }

int HeapTable::free_space(uint32_t page_num) const { 
    (void)page_num; 
    return 0; 
}

uint32_t HeapTable::vacuum() { return 0; }

}  // namespace storage
}  // namespace cloudsql
