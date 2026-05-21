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

    bytes_written = static_cast<uint16_t>(cursor - out_buf);
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
    fprintf(stderr, "DEBUG compare_keys: a=%s b=%s\n", a.to_string().c_str(), b.to_string().c_str());
    if (a < b) {
        fprintf(stderr, "DEBUG compare_keys: a < b returning -1\n");
        return -1;
    }
    if (b < a) {
        fprintf(stderr, "DEBUG compare_keys: b < a returning 1\n");
        return 1;
    }
    fprintf(stderr, "DEBUG compare_keys: equal returning 0\n");
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

        // If current node is internal, descend to leftmost leaf
        while (header.type == NodeType::Internal) {
            uint32_t child_page = index_.get_child_page(buffer.data(), 0);
            current_page_ = child_page;
            if (!index_.read_page(current_page_, buffer.data())) {
                eof_ = true;
                return false;
            }
            std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
            current_slot_ = 0;
        }

        if (current_slot_ >= header.num_keys) {
            if (header.next_leaf != 0) {
                current_page_ = header.next_leaf;
                current_slot_ = 0;
                fprintf(stderr, "DEBUG Iterator: advanced to next_leaf=%u\n", current_page_);
                continue;
            }
            fprintf(stderr, "DEBUG Iterator: at end, next_leaf=0, slot_idx=%u num_keys=%u\n",
                    current_slot_, header.num_keys);
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
        // The separator at slot[split_point] is promoted to parent.
    // But we need to pass the ORIGINAL next_leaf (the leftmost child of the right node after split)
    // as the right_page to insert_into_parent, NOT the newly allocated right_page_num.
    // The newly allocated right page is for the NEW right sibling, not the right child of the separator.
    // Wait - that's not right either. Let me reconsider.
    //
    // When leaf L splits into L' (left) and R (right):
    // - L' contains keys < separator
    // - R contains keys >= separator
    // - L'.next_leaf = R (the newly created right page)
    // - The separator goes to parent, with left_child=L' and right_child=R
    //
    // But the parent entry for separator points to R (the new right page), not the old next_leaf.
    // So right_page_num IS correct for insert_into_parent.
    //
    // The issue must be something else. Let me add debug to see what's happening.
    if (right_page_num != 0) {
        fprintf(stderr, "DEBUG insert: split happened, calling insert_into_parent sep=%s left=%u right=%u\n",
                pending_separator_.to_string().c_str(), leaf_page, right_page_num);
        if (!insert_into_parent(pending_separator_, leaf_page, right_page_num)) {
            return false;
        }
        right_page_num = 0;  // Reset to prevent duplicate insert_into_parent on retry
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
    fprintf(stderr, "DEBUG search: key=%s leaf_page=%u\n", key.to_string().c_str(), leaf_page);
    std::array<char, Page::PAGE_SIZE> buffer{};
    if (!read_page(leaf_page, buffer.data())) {
        return {};
    }

    std::vector<HeapTable::TupleId> results;

    NodeHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
    fprintf(stderr, "DEBUG search: leaf_page=%u num_keys=%u\n", leaf_page, header.num_keys);

    for (uint16_t i = 0; i < header.num_keys; ++i) {
        SlotEntry slot_entry;
        if (!get_slot(buffer.data(), i, slot_entry)) {
            continue;
        }

        if (slot_entry.offset < sizeof(NodeHeader) || slot_entry.offset >= Page::PAGE_SIZE) {
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

        fprintf(stderr, "DEBUG search: leaf=%u slot[%u] key=%s tid=%u\n", leaf_page, i, entry_key.to_string().c_str(), tid);
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
    NodeHeader header;
    std::memcpy(&header, buffer, sizeof(NodeHeader));

    // For internal nodes with N keys, children are 0 through N (N+1 children total)
    // Slots 0 through N-1 store children 0 through N-1
    // Child N (rightmost) is stored in next_leaf
    fprintf(stderr, "DEBUG get_child_page: slot_idx=%u num_keys=%u header.next_leaf=%u type=%d\n",
            slot_idx, header.num_keys, header.next_leaf, static_cast<int>(header.type));
    if (slot_idx >= header.num_keys) {
        if (header.type == NodeType::Internal) {
            fprintf(stderr, "DEBUG get_child_page: slot_idx >= num_keys, returning next_leaf=%u\n", header.next_leaf);
            return header.next_leaf;  // Rightmost child
        }
        return 0;  // Invalid for leaf nodes
    }

    SlotEntry slot;
    if (!get_slot(buffer, slot_idx, slot)) {
        return 0;
    }

    // Entry format: type(1) + key_len(4) + key_data(N) + child_page_num(4)
    const char* entry_ptr = buffer + slot.offset;

    uint32_t key_len = 0;
    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
    std::memcpy(&key_len, entry_ptr + 1, 4);

    // For fixed-size keys (INT64), key_len=0 but actual data is 8 bytes
    size_t key_data_size = (key_len == 0) ? 8 : key_len;
    size_t child_offset = slot.offset + 1 + 4 + key_data_size;

    uint32_t child_page = 0;
    std::memcpy(&child_page, buffer + child_offset, 4);
    fprintf(stderr, "DEBUG get_child_page: slot_idx=%u slot.offset=%u type=%d key_len=%u key_data_size=%zu child_offset=%zu child_page=%u\n",
            slot_idx, slot.offset, static_cast<int>(type), key_len, key_data_size, child_offset, child_page);
    return child_page;
}

int BTreeIndex::compare_separator(const char* buffer, uint16_t sep_idx, const common::Value& key) const {
    SlotEntry slot;
    if (!get_slot(buffer, sep_idx, slot)) {
        return 0;
    }

    common::Value entry_key;
    HeapTable::TupleId tid;
    const char* entry_ptr = buffer + slot.offset;

    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
    uint32_t key_len = 0;
    std::memcpy(&key_len, entry_ptr + 1, 4);

    if (type == common::ValueType::TYPE_INT64) {
        int64_t val = 0;
        std::memcpy(&val, entry_ptr + 1 + 4, 8);
        entry_key = common::Value::make_int64(val);
        fprintf(stderr, "DEBUG compare_separator: sep_idx=%u sep_key=%ld key=%s\n", sep_idx, val, key.to_string().c_str());
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

    // result = -1: all separators >= key, return child 0 (leftmost)
    // result >= 0: separator at result is < key, so key >= separator[result]
    //   Therefore key should go to child at result+1
    fprintf(stderr, "DEBUG find_child_for_key: key=%s num_keys=%u result=%d\n", key.to_string().c_str(), num_keys, result);
    if (result == -1) {
        uint32_t child = get_child_page(buffer, 0);
        fprintf(stderr, "DEBUG find_child_for_key: result=-1, get_child_page(buffer, 0)=%u\n", child);
        return child;
    }
    uint32_t child = get_child_page(buffer, static_cast<uint16_t>(result + 1));

    // Debug: show the separator at result
    SlotEntry slot;
    get_slot(buffer, static_cast<uint16_t>(result), slot);
    common::Value sep_key;
    HeapTable::TupleId tid;
    const char* entry_ptr = buffer + slot.offset;
    uint32_t key_len = 0;
    std::memcpy(&key_len, entry_ptr + 1, 4);
    common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
    if (type == common::ValueType::TYPE_INT64) {
        int64_t val = 0;
        std::memcpy(&val, entry_ptr + 1 + 4, 8);
        sep_key = common::Value::make_int64(val);
    } else {
        std::string s(entry_ptr + 1 + 4, key_len);
        sep_key = common::Value::make_text(s);
    }
    fprintf(stderr, "DEBUG find_child_for_key: separator[%d]=%s child=%u\n", result, sep_key.to_string().c_str(), child);

    return child;
}

uint32_t BTreeIndex::find_leaf(const common::Value& key) const {
    fprintf(stderr, "DEBUG find_leaf: ENTRY key=%s root_page_=%u\n", key.to_string().c_str(), root_page_);
    if (root_page_ == 0) {
        return 0;
    }

    std::array<char, Page::PAGE_SIZE> buffer{};
    if (!read_page(root_page_, buffer.data())) {
        fprintf(stderr, "DEBUG find_leaf: read_page failed for root_page_=%u\n", root_page_);
        return 0;
    }

    NodeHeader header;
    std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
    uint32_t current = root_page_;

    // Debug: dump root structure
    fprintf(stderr, "DEBUG find_leaf: ROOT page=%u type=%d num_keys=%u next_leaf=%u\n",
            root_page_, static_cast<int>(header.type), header.num_keys, header.next_leaf);
    if (header.num_keys > 0) {
        for (uint16_t i = 0; i < header.num_keys && i < 5; ++i) {
            SlotEntry slot;
            get_slot(buffer.data(), i, slot);
            uint32_t child = get_child_page(buffer.data(), i);
            const char* entry_ptr = buffer.data() + slot.offset;
            uint32_t key_len = 0;
            std::memcpy(&key_len, entry_ptr + 1, 4);
            common::ValueType type = static_cast<common::ValueType>(static_cast<uint8_t>(entry_ptr[0]));
            if (type == common::ValueType::TYPE_INT64) {
                int64_t val = 0;
                std::memcpy(&val, entry_ptr + 1 + 4, 8);
                fprintf(stderr, "DEBUG find_leaf:   slot[%u] child=%u sep_key=%ld slot.offset=%u\n", i, child, val, slot.offset);
            }
        }
        if (header.num_keys > 5) {
            fprintf(stderr, "DEBUG find_leaf:   ... (%u more slots)\n", header.num_keys - 5);
        }
    }

    while (header.type == NodeType::Internal) {
        uint32_t child = find_child_for_key(buffer.data(), key, header.num_keys);
        current = child;
        fprintf(stderr, "DEBUG find_leaf: key=%s at internal page=%u going to child=%u\n", key.to_string().c_str(), current, child);
        if (!read_page(child, buffer.data())) {
            return current;
        }
        std::memcpy(&header, buffer.data(), sizeof(NodeHeader));
    }
    fprintf(stderr, "DEBUG find_leaf: key=%s final leaf_page=%u\n", key.to_string().c_str(), current);
    return current;
}

/* === allocate_page === */

uint32_t BTreeIndex::allocate_page() {
    uint32_t new_page_num = 0;
    fprintf(stderr, "DEBUG allocate_page: calling bpm_.new_page for file '%s'\n", filename_.c_str());
    Page* page = bpm_.new_page(filename_, &new_page_num);
    fprintf(stderr, "DEBUG allocate_page: new_page returned page=%p new_page_num=%u\n", (void*)page, new_page_num);
    if (!page) {
        fprintf(stderr, "DEBUG allocate_page: page was null, returning 0\n");
        return 0;
    }
    bpm_.unpin_page(filename_, new_page_num, false);
    fprintf(stderr, "DEBUG allocate_page: success, returning %u\n", new_page_num);
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
    fprintf(stderr, "DEBUG split_leaf: called for page=%u\n", page_num);
    NodeHeader header{};
    std::memcpy(&header, buffer, sizeof(NodeHeader));
    fprintf(stderr, "DEBUG split_leaf: page=%u num_keys=%u next_leaf=%u parent=%u\n",
            page_num, header.num_keys, header.next_leaf, header.parent_page);

    if (header.num_keys <= 1) {
        return 0;  // Degenerate case
    }

    uint16_t split_point = header.num_keys / 2;
    if (split_point == 0) {
        split_point = 1;
    }

    uint16_t left_num_keys = split_point;
    uint16_t right_num_keys = header.num_keys - split_point - 1;  // -1 for separator promoted to parent

    // Create right leaf buffer
    char right_buffer[Page::PAGE_SIZE] = {0};
    NodeHeader right_header{};
    right_header.type = NodeType::Leaf;
    right_header.num_keys = right_num_keys;
    right_header.parent_page = header.parent_page;
    right_header.next_leaf = header.next_leaf;

    // Write header early so get_slot can read from right_buffer
    std::memcpy(right_buffer, &right_header, sizeof(NodeHeader));

    // Extract separator key (slot at split_point, which gets promoted to parent)
    // We need to do this BEFORE we modify the buffer
    SlotEntry sep_slot;
    get_slot(buffer, split_point, sep_slot);
    pending_separator_ = extract_key_from_entry(buffer + sep_slot.offset, sep_slot.length);

    // Copy entries [split_point + 1, num_keys) to right buffer
    // (entries 0 through split_point-1 stay in left, split_point entry promoted to parent)
    // Process in reverse order so entries pack at top of right page
    // Note: we start at header.num_keys - 1 and go down to split_point + 1 (split_point entry promoted to parent)
    int16_t current_right_offset = Page::PAGE_SIZE;
    for (int16_t i = static_cast<int16_t>(header.num_keys) - 1;
         i > static_cast<int16_t>(split_point);
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
        // Right page slots: entries from [split_point+1, num_keys) go to slots
        // [0, right_num_keys-1]. Since we iterate i from high to low, the first
        // entry (i=num_keys-1) goes to slot 0, next to slot 1, etc.
        // So slot_idx = num_keys - 1 - i
        put_slot(right_buffer, static_cast<uint16_t>(header.num_keys - 1 - i), new_slot);
    }

    // Update left leaf header
    header.num_keys = left_num_keys;
    header.next_leaf = 0;  // Will be updated after right page allocation
    fprintf(stderr, "DEBUG split_leaf: left leaf page=%u num_keys=%u next_leaf=0 (temp)\n", page_num, left_num_keys);
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
    fprintf(stderr, "DEBUG split_leaf: left leaf page=%u next_leaf updated to %u\n", page_num, right_page_num);

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
    header.next_leaf = right_child;  // Rightmost child (child 1)

    uint16_t entry_size = 1 + 4;  // type + key_len
    if (sep_key.type() == common::ValueType::TYPE_INT64) {
        entry_size += 8;
    } else {
        entry_size += static_cast<uint16_t>(sep_key.to_string().size());
    }
    entry_size += 4;  // child_page_num

    uint16_t entry_offset = Page::PAGE_SIZE - entry_size;
    uint16_t bytes_written = 0;
    serialize_internal_entry(sep_key, left_child, buffer + entry_offset, entry_size, bytes_written);

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
                                uint32_t& out_right_page) {
    (void)insert_pos;  // Not needed - split_point determines placement
    fprintf(stderr, "DEBUG split_internal: called for page=%u\n", page_num);
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

    // Write header early
    std::memcpy(right_buffer, &right_header, sizeof(NodeHeader));

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

    std::memcpy(right_buffer, &right_header, sizeof(NodeHeader));

    // Update left node header - preserve next_leaf (rightmost child) since left node
    // still has children 0 through split_point (split_point+1 children), and
    // split_point entry was promoted as separator, not moved to right node
    header.num_keys = left_num_keys;
    // header.next_leaf already points to the rightmost child of the left node
    std::memcpy(buffer, &header, sizeof(NodeHeader));

    // Allocate right page
    uint32_t right_page_num = allocate_page();
    if (right_page_num == 0) {
        return false;
    }

    // Write both pages
    write_page(page_num, buffer);
    write_page(right_page_num, right_buffer);

    // Update child parent pointer for promoted_left_child
    if (!update_child_parent(promoted_left_child, page_num)) return false;

    // Store promoted key for cascade
    pending_separator_ = promoted_key;

    out_right_page = right_page_num;
    return true;
}

/* === insert_into_parent (Phase 4 full) === */

bool BTreeIndex::insert_into_parent(common::Value sep_key, uint32_t left_page, uint32_t right_page) {
    fprintf(stderr, "DEBUG insert_into_parent: sep_key=%s left_page=%u right_page=%u\n",
            sep_key.to_string().c_str(), left_page, right_page);
    // Get parent page from left child
    std::array<char, Page::PAGE_SIZE> parent_buffer{};
    if (!read_page(left_page, parent_buffer.data())) {
        return false;
    }
    NodeHeader left_header{};
    std::memcpy(&left_header, parent_buffer.data(), sizeof(NodeHeader));
    uint32_t parent_page = left_header.parent_page;
    fprintf(stderr, "DEBUG insert_into_parent: left_page=%u parent_page=%u\n", left_page, parent_page);

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
            if (!split_internal(parent_page, buffer.data(), insert_pos, new_right_page)) {
                return false;
            }
            // After split, promoted key is in pending_separator_
            // Update sep_key and retry with the promoted key and new right page
            sep_key = pending_separator_;
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
    fprintf(stderr, "DEBUG read_page: page_num=%u\n", page_num);
    Page* page = bpm_.fetch_page(filename_, page_num);
    fprintf(stderr, "DEBUG read_page: fetch_page returned page=%p\n", (void*)page);
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
    // Flush immediately to storage so allocate_page can see the written data
    bpm_.flush_page(filename_, page_num);
    return true;
}

}  // namespace cloudsql::storage