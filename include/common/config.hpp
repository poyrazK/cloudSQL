/**
 * @file config.hpp
 * @brief C++ Configuration wrapper for SQL Engine
 */

#ifndef SQL_ENGINE_COMMON_CONFIG_HPP
#define SQL_ENGINE_COMMON_CONFIG_HPP

#include <cstdint>
#include <string>

namespace cloudsql::config {

/**
 * @brief Run modes for the database engine
 */
enum class RunMode : uint8_t { Embedded = 0, Distributed = 1 };

/**
 * @brief Server configuration structure (C++ wrapper)
 */
class Config {
   public:
    static constexpr uint16_t DEFAULT_PORT = 5432;
    static constexpr uint16_t MAX_PORT = 65535;
    static constexpr const char* DEFAULT_DATA_DIR = "./data";
    static constexpr int DEFAULT_MAX_CONNECTIONS = 100;
    static constexpr int DEFAULT_BUFFER_POOL_SIZE = 128;
    static constexpr int DEFAULT_PAGE_SIZE = 8192;
    static constexpr int MIN_PAGE_SIZE = 1024;
    static constexpr int MAX_PAGE_SIZE = 65536;

    // Configuration fields
    uint16_t port = DEFAULT_PORT;
    std::string data_dir = DEFAULT_DATA_DIR;
    std::string config_file;
    RunMode mode = RunMode::Embedded;
    int max_connections = DEFAULT_MAX_CONNECTIONS;
    int buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE;
    int page_size = DEFAULT_PAGE_SIZE;
    bool debug = false;
    bool verbose = false;

    /**
     * @brief Default constructor with default values
     */
    Config() = default;

    /**
     * @brief Load configuration from file
     * @param filename Path to configuration file
     * @return true on success, false on error
     */
    [[nodiscard]] bool load(const std::string& filename);

    /**
     * @brief Save configuration to file
     * @param filename Path to configuration file
     * @return true on success, false on error
     */
    [[nodiscard]] bool save(const std::string& filename) const;

    /**
     * @brief Validate configuration
     * @return true if valid, false otherwise
     */
    [[nodiscard]] bool validate() const;

    /**
     * @brief Print configuration to stdout
     */
    void print() const;

   private:
    /**
     * @brief Trim whitespace from string
     */
    [[nodiscard]] static std::string trim(const std::string& str);
};

}  // namespace cloudsql::config

#endif  // SQL_ENGINE_COMMON_CONFIG_HPP
