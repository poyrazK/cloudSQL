/**
 * @file config.hpp
 * @brief C++ Configuration wrapper for SQL Engine
 */

#ifndef SQL_ENGINE_COMMON_CONFIG_HPP
#define SQL_ENGINE_COMMON_CONFIG_HPP

#include <cstdint>
#include <string>

namespace cloudsql { namespace config {

/**
 * @brief Run modes for the database engine
 */
enum class RunMode {
    Embedded = 0,
    Distributed = 1
};

/**
 * @brief Server configuration structure (C++ wrapper)
 */
class Config {
public:
    static constexpr uint16_t DEFAULT_PORT = 5432;
    static constexpr const char* DEFAULT_DATA_DIR = "./data";
    static constexpr int DEFAULT_MAX_CONNECTIONS = 100;
    static constexpr int DEFAULT_BUFFER_POOL_SIZE = 128;
    static constexpr int DEFAULT_PAGE_SIZE = 8192;

    // Configuration fields
    uint16_t port;
    std::string data_dir;
    std::string config_file;
    RunMode mode;
    int max_connections;
    int buffer_pool_size;
    int page_size;
    bool debug;
    bool verbose;

    /**
     * @brief Default constructor with default values
     */
    Config();

    /**
     * @brief Load configuration from file
     * @param filename Path to configuration file
     * @return true on success, false on error
     */
    bool load(const std::string& filename);

    /**
     * @brief Save configuration to file
     * @param filename Path to configuration file
     * @return true on success, false on error
     */
    bool save(const std::string& filename) const;

    /**
     * @brief Validate configuration
     * @return true if valid, false otherwise
     */
    bool validate() const;

    /**
     * @brief Print configuration to stdout
     */
    void print() const;

private:
    /**
     * @brief Trim whitespace from string
     */
    static std::string trim(const std::string& str);
};

} // namespace config
} // namespace cloudsql

#endif // SQL_ENGINE_COMMON_CONFIG_HPP
