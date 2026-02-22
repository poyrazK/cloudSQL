/**
 * @file config.cpp
 * @brief Configuration implementation
 *
 * @defgroup config Configuration
 * @{
 */

#include "common/config.hpp"

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

namespace cloudsql::config {

/**
 * @brief Load configuration from file
 */
bool Config::load(const std::string& filename) {
    if (filename.empty()) {
        return false;
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open config file: " << filename << "\n";
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        /* Skip empty lines and comments */
        if (line.empty() || line[0] == '#' || line[0] == '\r') {
            continue;
        }

        /* Parse key=value */
        const auto eq_pos = line.find('=');
        if (eq_pos == std::string::npos) {
            continue;
        }

        const std::string key = trim(line.substr(0, eq_pos));
        const std::string value = trim(line.substr(eq_pos + 1));

        if (key.empty() || value.empty()) {
            continue;
        }

        /* Parse configuration options */
        if (key == "port") {
            port = static_cast<uint16_t>(std::stoi(value));
        } else if (key == "data_dir") {
            data_dir = value;
        } else if (key == "max_connections") {
            max_connections = std::stoi(value);
        } else if (key == "buffer_pool_size") {
            buffer_pool_size = std::stoi(value);
        } else if (key == "page_size") {
            page_size = std::stoi(value);
        } else if (key == "mode") {
            mode = (value == "distributed") ? RunMode::Distributed : RunMode::Embedded;
        } else if (key == "debug") {
            debug = (value == "true" || value == "1");
        } else if (key == "verbose") {
            verbose = (value == "true" || value == "1");
        }
    }

    file.close();
    return true;
}

/**
 * @brief Save configuration to file
 */
bool Config::save(const std::string& filename) const {
    if (filename.empty()) {
        return false;
    }

    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open config file for writing: " << filename << "\n";
        return false;
    }

    file << "# SQL Engine Configuration\n";
    file << "# Auto-generated\n\n";

    file << "port=" << port << "\n";
    file << "data_dir=" << data_dir << "\n";
    file << "max_connections=" << max_connections << "\n";
    file << "buffer_pool_size=" << buffer_pool_size << "\n";
    file << "page_size=" << page_size << "\n";
    file << "mode=" << (mode == RunMode::Distributed ? "distributed" : "embedded") << "\n";
    file << "debug=" << (debug ? "true" : "false") << "\n";
    file << "verbose=" << (verbose ? "true" : "false") << "\n";

    file.close();
    return true;
}

/**
 * @brief Validate configuration
 */
bool Config::validate() const {
    if (port == 0 || port > MAX_PORT) {
        std::cerr << "Invalid port number: " << port << "\n";
        return false;
    }

    if (max_connections < 1) {
        std::cerr << "Invalid max connections: " << max_connections << "\n";
        return false;
    }

    if (buffer_pool_size < 1) {
        std::cerr << "Invalid buffer pool size: " << buffer_pool_size << "\n";
        return false;
    }

    if (page_size < MIN_PAGE_SIZE || page_size > MAX_PAGE_SIZE) {
        std::cerr << "Invalid page size: " << page_size << " (must be between " << MIN_PAGE_SIZE
                  << " and " << MAX_PAGE_SIZE << ")\n";
        return false;
    }

    if (data_dir.empty()) {
        std::cerr << "Data directory cannot be empty\n";
        return false;
    }

    return true;
}

/**
 * @brief Print configuration to stdout
 */
void Config::print() const {
    std::cout << "=== SQL Engine Configuration ===\n";
    std::cout << "Mode:         " << (mode == RunMode::Distributed ? "distributed" : "embedded")
              << "\n";
    std::cout << "Port:         " << port << "\n";
    std::cout << "Data dir:     " << data_dir << "\n";
    std::cout << "Max conns:    " << max_connections << "\n";
    std::cout << "Buffer pool:  " << buffer_pool_size << " pages\n";
    std::cout << "Page size:    " << page_size << " bytes\n";
    std::cout << "Debug:        " << (debug ? "enabled" : "disabled") << "\n";
    std::cout << "Verbose:      " << (verbose ? "enabled" : "disabled") << "\n";
    std::cout << "================================\n";
}

/**
 * @brief Trim whitespace from string
 */
std::string Config::trim(const std::string& str) {
    const size_t start = str.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return "";
    }
    const size_t end = str.find_last_not_of(" \t\n\r");
    return str.substr(start, end - start + 1);
}

}  // namespace cloudsql::config

/** @} */ /* config */
