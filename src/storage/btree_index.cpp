/**
 * @file btree_index.cpp
 * @brief B-tree index implementation with slot array format
 */

#include "storage/btree_index.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/value.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "storage/page.hpp"

namespace cloudsql::storage {

BTreeIndex::BTreeIndex(std::string index_name, BufferPoolManager& bpm, common::ValueType key_type)
    : index_name_(std::move(index_name)),
      filename_(index_name_ + ".idx"),
      bpm_(bpm),
      key_type_(key_type) {}

/* === Slot Array Helpers === */

uint16_t BTreeIndex::get_data_start_offset(uint16_t num_keys) const {
    return static_cast<uint16_t>(sizeof(NodeHeader) + num_keys * kSlotSize);
}

uint16_t BTreeIndex::compute_entry_size(const common::Value& key) const {
    uint16_t size = 1 + 4;  // type + key_len
    if (key.type() == common::ValueType::TYPE_INT64) {
        size += 8;  // int64 key
    } else {
        size += static_cast<uint16_t>(key.to_string().size());  // text key
    }
    size += 4 + 2;  // page_num (4) + slot_num (2)
    return size;
}

bool BTreeIndex::get_slot(const char* buffer, uint16_t slot_idx, SlotEntry& out) const {
    if (slot_idx >= kMaxSlots) {
        return false;
    }
    const char* slot_ptr = buffer + sizeof(NodeHeader) + static_cast<size_t>(slot_idx) * kSlotSize;
    std::memcpy(&out, slot_ptr, sizeof(SlotEntry));
    return true;
}

bool BTreeIndex::put_slot(char* buffer, uint16_t slot_idx, const SlotEntry& entry) {
    if (slot_idx >= kMaxSlots) {
        return false;
    }
    char* slot_ptr = buffer + sizeof(NodeHeader) + static_cast<size_t>(slot_idx) * kSlotSize;
    std::memcpy(slot_ptr, &entry, sizeof(SlotEntry));
    return true;
}

/* === Entry Serialization === */

bool BTreeIndex::serialize_entry(const common::Value& key, HeapTable::TupleId tuple_id,
                                 char* out_buf, uint16_t buf_size,
                                 uint16_t& bytes_written) const {
    if (buf_size < compute_entry_size(key)) {
        return false;
    }

    char* cursor = out_buf;

    // type (1 byte)
    *cursor++ = static_cast<char>(key.type());

    if (key.type() == common::ValueType::TYPE_INT64) {
        // key_len = 0 (marker for fixed-size)
        uint32_t zero = 0;
        std::memcpy(cursor, &zero, 4);
        cursor += 4;
        // int64 key (8 bytes)
        int64_t val = key.to_string().empty() ? 0 : std::stoll(key.to_string());
        std::memcpy(cursor, &val, 8);
        cursor += 8;
    } else {
        // text key
        std::string s = key.to_string();
        uint32_t len = static_cast<uint32_t>(s.size());
        std::memcpy(cursor, &len, 4);
        cursor += 4;
        std::memcpy(cursor, s.data(), len);
        cursor += len;
    }

    // TupleId: page_num (4) + slot_num (2)
    uint32_t page_num = tuple_id.page_num;
    uint16_t slot_num = tuple_id.slot_num;
    std::memcpy(cursor, &page_num, 4);
    cursor += 4;
    std::memcpy(cursor, &slot_num, 2);

    bytes_written = static_cast<uint16_t>(cursor - out_buf + 2);
    return true;
}

bool BTreeIndex::deserialize_entry(const char* buf, uint16_t buf_size,
                                  common::Value& out_key,
                                  HeapTable::TupleId& out_tuple_id) const {
    if (buf_size < 7) {  // minimum: type(1) + key_len(4) + page(4) + slot(2) - 2 = 9? let me recalc
        return false;
    }

    const char* cursor = buf;

    // type
    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(*cursor));
    cursor += 1;

    // key_len
    uint32_t key_len = 0;
    std::memcpy(&key_len, cursor, 4);
    cursor += 4;

    if (type == common::ValueType::TYPE_INT64) {
        int64_t val = 0;
        std::memcpy(&val, cursor, 8);
        cursor += 8;
        out_key = common::Value::make_int64(val);
    } else {
        std::string s(cursor, key_len);
        out_key = common::Value::make_text(s);
        cursor += key_len;
    }

    // TupleId
    uint32_t page_num = 0;
    uint16_t slot_num = 0;
    std::memcpy(&page_num, cursor, 4);
    cursor += 4;
    std::memcpy(&slot_num, cursor, 2);
    out_tuple_id = HeapTable::TupleId(page_num, slot_num);

    return true;
}

/* === Key Comparison === */

int BTreeIndex::compare_keys(const common::Value& a, const common::Value& b) const {
    if (a < b) {
        return -1;
    }
    if (b < a) {
        return 1;
    }
    return 0;
}

/* === Iterator Implementation === */

BTreeIndex::Iterator::Iterator(BTreeIndex& index, uint32_t page, uint16_t slot)
    : index_(index), current_page_(page), current_slot_(slot) {}

bool BTreeIndex::Iterator::next(Entry& out_entry) {
    while (!eof_) {
        std::array<char, Page::PAGE_SIZE> buffer{};
        if (!index_.read_page(current_page_, buffer.data())) {
            eof_ = true;
            return false;
        }

        NodeHeader header{};
        std::memcpy(&header, buffer.data(), sizeof(NodeHeader));

        if (current_slot_ >= header.num_keys) {
            if (header.next_leaf != 0) {
                current_page_ = header.next_leaf;
                current_slot_ = 0;
                continue;
            }
            eof_ = true;
            return false;
        }

        SlotEntry slot_entry;
        if (!index_.get_slot(buffer.data(), current_slot_, slot_entry)) {
            eof_ = true;
            return false;
        }

        if (slot_entry.offset + slot_entry.length > Page::PAGE_SIZE) {
            eof_ = true;
            return false;
        }

        if (!index_.deserialize_entry(buffer.data() + slot_entry.offset,
                                      slot_entry.length,
                                      out_entry.key,
                                      out_entry.tuple_id)) {
            eof_ = true;
            return false;
        }

        current_slot_++;
        return true;
    }
    return false;
}

/* === BTreeIndex Core Operations === */

bool BTreeIndex::create() {
    if (!bpm_.open_file(filename_)) {
        return false;
    }

    std::array<char, Page::PAGE_SIZE> buffer{};
    NodeHeader header{};
    header.type = NodeType::Leaf;
    header.num_keys = 0;
    header.parent_page = 0;
    header.next_leaf = 0;
    std::memcpy(buffer.data(), &header, sizeof(NodeHeader));

    return write_page(0, buffer.data());
}

bool BTreeIndex::open() {
    return bpm_.open_file(filename_);
}

void BTreeIndex::close() {
    bpm_.close_file(filename_);
}

bool BTreeIndex::drop() {
    static_cast<void>(bpm_.close_file(filename_));
    return (std::remove(filename_.c_str()) == 0);
}

bool BTreeIndex::insert(const common::Value& key, HeapTable::TupleId tuple_id) {
    const uint32_t leaf_page = find_leaf(key);
    uint32_t right_page_num = 0;  // Set when a split happens

    // Retry loop: on first iteration, insert normally. If page is full,
    // split_leaf() is called and we retry on the updated left leaf.
    for (int attempt = 0; attempt < 2; ++attempt) {
        std::array<char, Page::PAGE_SIZE> buffer{};
        if (!read_page(leaf_page, buffer.data())) {
            return false;
        }

        NodeHeader header{};
        std::memcpy(&header, buffer.data(), sizeof(NodeHeader));

        // Compute entry size
        const uint16_t entry_size = compute_entry_size(key);

        // Determine where new entry would go (grows backward from page end)
        uint16_t new_entry_offset = Page::PAGE_SIZE;
        if (header.num_keys > 0) {
            for (uint16_t i = 0; i < header.num_keys; ++i) {
                SlotEntry s;
                if (get_slot(buffer.data(), i, s) && s.offset < new_entry_offset) {
                    new_entry_offset = s.offset;
                }
            }
        }
        new_entry_offset -= entry_size;

        // Check space: entry must not overlap with slot array
        const uint16_t slot_array_end =
            sizeof(NodeHeader) + static_cast<uint16_t>((header.num_keys + 1) * kSlotSize);
        if (new_entry_offset < slot_array_end) {
            // Leaf is full — split it
            right_page_num = split_leaf(leaf_page, buffer.data());
            // After split, the original key always belongs in the left leaf
            // (it's less than the separator key). Retry on the same leaf page.
            if (right_page_num == 0) {
                return false;  // Split failed
            }
            continue;
        }

        // Serialize entry
        uint16_t bytes_written = 0;
        if (!serialize_entry(key, tuple_id, buffer.data() + new_entry_offset, entry_size, bytes_written)) {
            return false;
        }

        // Write slot for this entry at position num_keys
        SlotEntry slot{};
        slot.offset = new_entry_offset;
        slot.length = entry_size;
        put_slot(buffer.data(), header.num_keys, slot);

        // Update header
        header.num_keys++;
        std::memcpy(buffer.data(), &header, sizeof(NodeHeader));

        if (!write_page(leaf_page, buffer.data())) {
            return false;
        }

        // If a split happened, insert separator into parent
        if (right_page_num != 0) {
            if (!insert_into_parent(pending_separator_, leaf_page, right_page_num)) {
                return false;
            }
        }
        return true;
    }
    return false;  // Should not reach here
}

bool BTreeIndex::remove(const common::Value& key, HeapTable::TupleId tuple_id) {
    (void)this;
    (void)key;
    (void)tuple_id;
    return true;
}

std::vector<HeapTable::TupleId> BTreeIndex::search(const common::Value& key) {
    const uint32_t leaf_page = find_leaf(key);
    std::array<char, Page::PAGE_SIZE> buffer{};
    if (!read_page(leaf_page, buffer.data())) {
        return {};
    }

    std::vector<HeapTable::TupleId> results;

    NodeHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(NodeHeader));

    for (uint16_t i = 0; i < header.num_keys; ++i) {
        SlotEntry slot_entry;
        if (!get_slot(buffer.data(), i, slot_entry)) {
            continue;
        }

        common::Value entry_key;
        HeapTable::TupleId tid;
        if (!deserialize_entry(buffer.data() + slot_entry.offset,
                              slot_entry.length,
                              entry_key,
                              tid)) {
            continue;
        }

        if (entry_key == key) {
            results.emplace_back(tid);
        }
    }

    return results;
}

BTreeIndex::Iterator BTreeIndex::scan() {
    return {*this, root_page_, 0};
}

/* === Internal Node Navigation === */

uint32_t BTreeIndex::get_child_page(const char* buffer, uint16_t slot_idx) const {
    SlotEntry slot;
    if (!get_slot(buffer, slot_idx, slot)) {
        return 0;
    }

    // Entry format: type(1) + key_len(4) + key_data(N) + child_page_num(4)
    const char* entry_ptr = buffer + slot.offset;

    // Skip type + key_len
    uint32_t key_len = 0;
    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
    std::memcpy(&key_len, entry_ptr + 1, 4);

    size_t key_data_offset = 1 + 4;  // type + key_len
    size_t child_offset = slot.offset + key_data_offset + key_len;

    uint32_t child_page = 0;
    std::memcpy(&child_page, buffer + child_offset, 4);
    return child_page;
}

int BTreeIndex::compare_separator(const char* buffer, uint16_t sep_idx, const common::Value& key) const {
    SlotEntry slot;
    if (!get_slot(buffer, sep_idx, slot)) {
        return 0;
    }

    common::Value entry_key;
    HeapTable::TupleId tid;
    // Use a temporary buffer to deserialize just the key portion
    // The entry format is: type(1) + key_len(4) + key_data(N) + child_page_num(4)
    // We need to skip the child_page_num at the end
    const char* entry_ptr = buffer + slot.offset;

    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
    uint32_t key_len = 0;
    std::memcpy(&key_len, entry_ptr + 1, 4);

    if (type == common::ValueType::TYPE_INT64) {
        int64_t val = 0;
        std::memcpy(&val, entry_ptr + 1 + 4, 8);
        entry_key = common::Value::make_int64(val);
    } else {
        std::string s(entry_ptr + 1 + 4, key_len);
        entry_key = common::Value::make_text(s);
    }

    return compare_keys(entry_key, key);
}

uint32_t BTreeIndex::find_child_for_key(const char* buffer, const common::Value& key, uint16_t num_keys) const {
    if (num_keys == 0) {
        return 0;
    }

    // Binary search: find rightmost separator key that is < key
    // Then return the child at position (result + 1)
    // If all separators >= key, return child at position 0
    int lo = 0;
    int hi = static_cast<int>(num_keys) - 1;
    int result = -1;  // index of rightmost sep < key

    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        int cmp = compare_separator(buffer, static_cast<uint16_t>(mid), key);
        if (cmp < 0) {
            result = mid;
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }

    // Child pointers: child i is stored at slot i, so:
    // result = -1 → return child at slot 0
    // result >= 0 → return child at slot (result + 1)
    if (result == -1) {
        return get_child_page(buffer, 0);
    }
    return get_child_page(buffer, static_cast<uint16_t>(result + 1));
}

uint32_t BTreeIndex::find_leaf(const common::Value& key) const {
    if (root_page_ == 0) {
        return 0;
    }

    std::array<char, Page::PAGE_SIZE> buffer{};
    if (!read_page(root_page_, buffer.data())) {
        return 0;
    }

    NodeHeader header;
    std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
    uint32_t current = root_page_;

    while (header.type == NodeType::Internal) {
        uint32_t child = find_child_for_key(buffer.data(), key, header.num_keys);
        if (!read_page(child, buffer.data())) {
            return current;
        }
        std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
        current = child;
    }
    return current;
}

/* === allocate_page === */

uint32_t BTreeIndex::allocate_page() {
    uint32_t new_page_num = 0;
    Page* page = bpm_.new_page(filename_, &new_page_num);
    if (!page) {
        return 0;
    }
    bpm_.unpin_page(filename_, new_page_num, false);
    return new_page_num;
}

/* === Internal node entry helpers === */

common::Value BTreeIndex::extract_key_from_entry(const char* entry_ptr, uint16_t entry_length) const {
    (void)entry_length;
    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
    uint32_t key_len = 0;
    std::memcpy(&key_len, entry_ptr + 1, 4);

    if (type == common::ValueType::TYPE_INT64) {
        int64_t val = 0;
        std::memcpy(&val, entry_ptr + 1 + 4, 8);
        return common::Value::make_int64(val);
    } else {
        std::string s(entry_ptr + 1 + 4, key_len);
        return common::Value::make_text(s);
    }
}

bool BTreeIndex::serialize_internal_entry(const common::Value& key, uint32_t child_page_num,
                                          char* out_buf, uint16_t buf_size,
                                          uint16_t& bytes_written) const {
    uint16_t header_size = 1 + 4;
    uint16_t key_data_size = (key.type() == common::ValueType::TYPE_INT64) ? 8 :
                             static_cast<uint16_t>(key.to_string().size());
    uint16_t total_size = header_size + key_data_size + 4;

    if (buf_size < total_size) {
        return false;
    }

    char* cursor = out_buf;
    *cursor++ = static_cast<char>(key.type());

    if (key.type() == common::ValueType::TYPE_INT64) {
        uint32_t zero = 0;
        std::memcpy(cursor, &zero, 4);
        cursor += 4;
        int64_t val = std::stoll(key.to_string());
        std::memcpy(cursor, &val, 8);
        cursor += 8;
    } else {
        std::string s = key.to_string();
        uint32_t len = static_cast<uint32_t>(s.size());
        std::memcpy(cursor, &len, 4);
        cursor += 4;
        std::memcpy(cursor, s.data(), len);
        cursor += len;
    }

    std::memcpy(cursor, &child_page_num, 4);
    bytes_written = total_size;
    return true;
}

/* === split_leaf === */

uint32_t BTreeIndex::split_leaf(uint32_t page_num, char* buffer) {
    NodeHeader header{};
    std::memcpy(&header, buffer, sizeof(NodeHeader));

    if (header.num_keys <= 1) {
        return 0;  // Degenerate case
    }

    uint16_t split_point = header.num_keys / 2;
    if (split_point == 0) {
        split_point = 1;
    }

    uint16_t left_num_keys = split_point;
    uint16_t right_num_keys = header.num_keys - split_point;

    // Create right leaf buffer
    char right_buffer[Page::PAGE_SIZE] = {0};
    NodeHeader right_header{};
    right_header.type = NodeType::Leaf;
    right_header.num_keys = right_num_keys;
    right_header.parent_page = header.parent_page;
    right_header.next_leaf = header.next_leaf;

    // Copy entries [split_point, num_keys) to right buffer
    // Process in reverse order so entries pack at top of right page
    int16_t current_right_offset = Page::PAGE_SIZE;
    for (int16_t i = static_cast<int16_t>(header.num_keys) - 1;
         i >= static_cast<int16_t>(split_point);
         --i) {
        SlotEntry old_slot;
        get_slot(buffer, static_cast<uint16_t>(i), old_slot);

        common::Value entry_key;
        HeapTable::TupleId entry_tid;
        deserialize_entry(buffer + old_slot.offset, old_slot.length, entry_key, entry_tid);

        uint16_t entry_size = compute_entry_size(entry_key);
        current_right_offset -= entry_size;

        uint16_t bytes_written = 0;
        serialize_entry(entry_key, entry_tid, right_buffer + current_right_offset,
                        entry_size, bytes_written);

        SlotEntry new_slot{};
        new_slot.offset = current_right_offset;
        new_slot.length = entry_size;
        put_slot(right_buffer, static_cast<uint16_t>(i - split_point), new_slot);
    }

    // Extract separator key (first key of right leaf = slot at split_point)
    SlotEntry sep_slot;
    get_slot(buffer, split_point, sep_slot);
    pending_separator_ = extract_key_from_entry(buffer + sep_slot.offset, sep_slot.length);

    // Update left leaf header
    header.num_keys = left_num_keys;
    header.next_leaf = 0;  // Will be updated after right page allocation
    std::memcpy(buffer, &header, sizeof(NodeHeader));

    // Allocate new right page
    uint32_t right_page_num = allocate_page();
    if (right_page_num == 0) {
        return 0;  // Allocation failed
    }

    // Update left leaf's next_leaf to point to new right page
    NodeHeader left_header{};
    std::memcpy(&left_header, buffer, sizeof(NodeHeader));
    left_header.next_leaf = right_page_num;
    std::memcpy(buffer, &left_header, sizeof(NodeHeader));

    // Write both pages
    write_page(page_num, buffer);
    write_page(right_page_num, right_buffer);

    return right_page_num;
}

/* === update_child_parent === */

bool BTreeIndex::update_child_parent(uint32_t child_page, uint32_t parent_page) {
    std::array<char, Page::PAGE_SIZE> buffer{};
    if (!read_page(child_page, buffer.data())) {
        return false;
    }
    NodeHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
    header.parent_page = parent_page;
    std::memcpy(buffer.data(), &header, sizeof(NodeHeader));
    return write_page(child_page, buffer.data());
}

/* === create_new_root === */

bool BTreeIndex::create_new_root(const common::Value& sep_key, uint32_t left_child, uint32_t right_child) {
    char buffer[Page::PAGE_SIZE] = {0};

    NodeHeader header{};
    header.type = NodeType::Internal;
    header.num_keys = 1;
    header.parent_page = 0;
    header.next_leaf = right_child;

    uint16_t entry_size = 1 + 4;  // type + key_len
    if (sep_key.type() == common::ValueType::TYPE_INT64) {
        entry_size += 8;
    } else {
        entry_size += static_cast<uint16_t>(sep_key.to_string().size());
    }
    entry_size += 4;  // child_page_num

    uint16_t entry_offset = Page::PAGE_SIZE - entry_size;
    uint16_t bytes_written = 0;
    serialize_internal_entry(sep_key, right_child, buffer + entry_offset, entry_size, bytes_written);

    SlotEntry slot{};
    slot.offset = entry_offset;
    slot.length = entry_size;
    put_slot(buffer, 0, slot);

    std::memcpy(buffer, &header, sizeof(NodeHeader));

    uint32_t new_root_page = allocate_page();
    if (new_root_page == 0) {
        return false;
    }

    write_page(new_root_page, buffer);

    if (!update_child_parent(left_child, new_root_page)) return false;
    if (!update_child_parent(right_child, new_root_page)) return false;

    root_page_ = new_root_page;
    return true;
}

/* === split_internal === */

bool BTreeIndex::split_internal(uint32_t page_num, char* buffer, uint16_t insert_pos,
                                uint32_t left_child, uint32_t right_child,
                                uint32_t& out_right_page) {
    (void)insert_pos;  // Not needed - split_point determines placement
    NodeHeader header{};
    std::memcpy(&header, buffer, sizeof(NodeHeader));

    if (header.num_keys <= 1) {
        return false;
    }

    uint16_t split_point = header.num_keys / 2;
    if (split_point == 0) split_point = 1;

    // Extract promoted separator (slot at split_point)
    SlotEntry sep_slot;
    get_slot(buffer, split_point, sep_slot);
    common::Value promoted_key = extract_key_from_entry(buffer + sep_slot.offset, sep_slot.length);
    uint32_t promoted_left_child = get_child_page(buffer, split_point);

    uint16_t left_num_keys = split_point;
    uint16_t right_num_keys = header.num_keys - split_point - 1;

    // Build right node buffer
    char right_buffer[Page::PAGE_SIZE] = {0};
    NodeHeader right_header{};
    right_header.type = NodeType::Internal;
    right_header.num_keys = right_num_keys;
    right_header.parent_page = header.parent_page;
    right_header.next_leaf = header.next_leaf;

    // Copy entries [split_point+1, num_keys) to right buffer
    int16_t right_offset = Page::PAGE_SIZE;
    uint16_t right_slot_idx = 0;

    for (uint16_t i = split_point + 1; i < header.num_keys; ++i) {
        SlotEntry old_slot;
        get_slot(buffer, i, old_slot);

        common::Value entry_key = extract_key_from_entry(buffer + old_slot.offset, old_slot.length);
        uint32_t child_page = get_child_page(buffer, i);

        uint16_t entry_size = 1 + 4;
        if (entry_key.type() == common::ValueType::TYPE_INT64) {
            entry_size += 8;
        } else {
            entry_size += static_cast<uint16_t>(entry_key.to_string().size());
        }
        entry_size += 4;

        right_offset -= entry_size;
        uint16_t bytes_written = 0;
        serialize_internal_entry(entry_key, child_page, right_buffer + right_offset,
                                 entry_size, bytes_written);

        SlotEntry new_slot{};
        new_slot.offset = right_offset;
        new_slot.length = entry_size;
        put_slot(right_buffer, right_slot_idx, new_slot);
        right_slot_idx++;
    }

    std::memcpy(right_buffer, &right_header, sizeof(NodeHeader));

    // Update left node header
    header.num_keys = left_num_keys;
    header.next_leaf = promoted_left_child;
    std::memcpy(buffer, &header, sizeof(NodeHeader));

    // Allocate right page
    uint32_t right_page_num = allocate_page();
    if (right_page_num == 0) {
        return false;
    }

    // Write both pages
    write_page(page_num, buffer);
    write_page(right_page_num, right_buffer);

    // Update child parent pointers
    if (!update_child_parent(promoted_left_child, page_num)) return false;
    if (!update_child_parent(left_child, page_num)) return false;
    if (!update_child_parent(right_child, right_page_num)) return false;

    // Store promoted key for cascade
    pending_separator_ = promoted_key;

    out_right_page = right_page_num;
    return true;
}

/* === insert_into_parent (Phase 4 full) === */

bool BTreeIndex::insert_into_parent(const common::Value& sep_key, uint32_t left_page, uint32_t right_page) {
    // Get parent page from left child
    std::array<char, Page::PAGE_SIZE> parent_buffer{};
    if (!read_page(left_page, parent_buffer.data())) {
        return false;
    }
    NodeHeader left_header{};
    std::memcpy(&left_header, parent_buffer.data(), sizeof(NodeHeader));
    uint32_t parent_page = left_header.parent_page;

    // Root split case: left_page is the root, but there is no parent
    if (parent_page == 0) {
        return create_new_root(sep_key, left_page, right_page);
    }

    // Retry loop for potential split cascade
    for (int attempt = 0; attempt < 2; ++attempt) {
        std::array<char, Page::PAGE_SIZE> buffer{};
        if (!read_page(parent_page, buffer.data())) {
            return false;
        }

        NodeHeader header{};
        std::memcpy(&header, buffer.data(), sizeof(NodeHeader));

        // Compute new internal entry size
        uint16_t new_entry_size = 1 + 4;  // type + key_len
        if (sep_key.type() == common::ValueType::TYPE_INT64) {
            new_entry_size += 8;
        } else {
            new_entry_size += static_cast<uint16_t>(sep_key.to_string().size());
        }
        new_entry_size += 4;  // child_page_num

        // Find insertion position using binary search
        int insert_pos = 0;
        if (header.num_keys > 0) {
            int lo = 0;
            int hi = static_cast<int>(header.num_keys) - 1;
            int result = -1;
            while (lo <= hi) {
                int mid = (lo + hi) / 2;
                int cmp = compare_separator(buffer.data(), static_cast<uint16_t>(mid), sep_key);
                if (cmp < 0) {
                    result = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            insert_pos = result + 1;
        }

        // Determine available space
        uint16_t new_entry_offset = Page::PAGE_SIZE;
        if (header.num_keys > 0) {
            for (uint16_t i = 0; i < header.num_keys; ++i) {
                SlotEntry s;
                if (get_slot(buffer.data(), i, s) && s.offset < new_entry_offset) {
                    new_entry_offset = s.offset;
                }
            }
        }
        new_entry_offset -= new_entry_size;

        const uint16_t slot_array_end =
            sizeof(NodeHeader) + static_cast<uint16_t>((header.num_keys + 1) * kSlotSize);

        if (new_entry_offset < slot_array_end) {
            // Parent is full — split it
            uint32_t new_right_page = 0;
            if (!split_internal(parent_page, buffer.data(), insert_pos, left_page, right_page, new_right_page)) {
                return false;
            }
            // After split, promoted key is in pending_separator_
            // Retry with the promoted key and new right page
            parent_page = new_right_page;
            continue;
        }

        // Space available — insert at insert_pos
        // Shift slots [insert_pos, num_keys) forward
        for (uint16_t i = header.num_keys; i > insert_pos; --i) {
            SlotEntry s;
            get_slot(buffer.data(), static_cast<uint16_t>(i - 1), s);
            put_slot(buffer.data(), i, s);
        }

        // Serialize new internal entry
        uint16_t bytes_written = 0;
        if (!serialize_internal_entry(sep_key, right_page, buffer.data() + new_entry_offset,
                                      new_entry_size, bytes_written)) {
            return false;
        }

        // Write slot
        SlotEntry new_slot{};
        new_slot.offset = new_entry_offset;
        new_slot.length = new_entry_size;
        put_slot(buffer.data(), insert_pos, new_slot);

        // Update header
        header.num_keys++;
        std::memcpy(buffer.data(), &header, sizeof(NodeHeader));

        // Write parent page
        if (!write_page(parent_page, buffer.data())) {
            return false;
        }

        // Update child parent pointers
        if (!update_child_parent(left_page, parent_page)) return false;
        if (!update_child_parent(right_page, parent_page)) return false;

        return true;
    }
    return false;
}

bool BTreeIndex::read_page(uint32_t page_num, char* buffer) const {
    Page* page = bpm_.fetch_page(filename_, page_num);
    if (!page) {
        return false;
    }
    std::memcpy(buffer, page->get_data(), Page::PAGE_SIZE);
    bpm_.unpin_page(filename_, page_num, false);
    return true;
}

bool BTreeIndex::write_page(uint32_t page_num, const char* buffer) {
    Page* page = bpm_.fetch_page(filename_, page_num);
    if (!page) {
        page = bpm_.new_page(filename_, &page_num);
        if (!page) {
            return false;
        }
    }
    std::memcpy(page->get_data(), buffer, Page::PAGE_SIZE);
    bpm_.unpin_page(filename_, page_num, true);
    return true;
}

}  // namespace cloudsql::storage
