#pragma once
#include <cstdint>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace cloudsql {
namespace storage {

struct StorageStats {
    uint64_t pages_read = 0;
    uint64_t pages_written = 0;
    uint64_t files_opened = 0;
    uint64_t bytes_read = 0;
    uint64_t bytes_written = 0;
};

class StorageManager {
   public:
    static constexpr size_t PAGE_SIZE = 4096;

    explicit StorageManager(std::string data_dir);
    ~StorageManager();

    bool open_file(const std::string& filename);
    bool close_file(const std::string& filename);
    bool read_page(const std::string& filename, uint32_t page_num, char* buffer);
    bool write_page(const std::string& filename, uint32_t page_num, const char* buffer);

    StorageStats get_stats() const { return stats_; }

   private:
    std::string data_dir_;
    std::map<std::string, std::unique_ptr<std::fstream>> open_files_;
    StorageStats stats_;

    bool create_dir_if_not_exists();
};

}  // namespace storage
}  // namespace cloudsql
