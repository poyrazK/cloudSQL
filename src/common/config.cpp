#include "common/config.hpp"
#include <iostream>
#include <fstream>
#include <algorithm>

namespace cloudsql {
namespace config {

Config::Config()
    : port(DEFAULT_PORT)
    , data_dir(DEFAULT_DATA_DIR)
    , config_file("")
    , mode(RunMode::Embedded)
    , max_connections(DEFAULT_MAX_CONNECTIONS)
    , buffer_pool_size(DEFAULT_BUFFER_POOL_SIZE)
    , page_size(DEFAULT_PAGE_SIZE)
    , debug(false)
    , verbose(false)
{}

bool Config::load(const std::string& filename) {
    if (filename.empty()) {
        return false;
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open config file: " << filename << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#' || line[0] == '
') {
            continue;
        }

        // Parse key=value
        auto eq_pos = line.find('=');
        if (eq_pos == std::string::npos) {
            continue;
        }

        std::string key = trim(line.substr(0, eq_pos));
        std::string value = trim(line.substr(eq_pos + 1));

        if (key.empty() || value.empty()) {
            continue;
        }

        // Parse configuration options
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

bool Config::save(const std::string& filename) const {
    if (filename.empty()) {
        return false;
    }

    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open config file for writing: " << filename << std::endl;
        return false;
    }

    file << "# SQL Engine Configuration
";
    file << "# Auto-generated

";

    file << "port=" << port << "
";
    file << "data_dir=" << data_dir << "
";
    file << "max_connections=" << max_connections << "
";
    file << "buffer_pool_size=" << buffer_pool_size << "
";
    file << "page_size=" << page_size << "
";
    file << "mode=" << (mode == RunMode::Distributed ? "distributed" : "embedded") << "
";
    file << "debug=" << (debug ? "true" : "false") << "
";
    file << "verbose=" << (verbose ? "true" : "false") << "
";

    file.close();
    return true;
}

bool Config::validate() const {
    if (port == 0 || port > 65535) {
        std::cerr << "Invalid port number: " << port << std::endl;
        return false;
    }

    if (max_connections < 1) {
        std::cerr << "Invalid max connections: " << max_connections << std::endl;
        return false;
    }

    if (buffer_pool_size < 1) {
        std::cerr << "Invalid buffer pool size: " << buffer_pool_size << std::endl;
        return false;
    }

    if (page_size < 1024 || page_size > 65536) {
        std::cerr << "Invalid page size: " << page_size << " (must be between 1024 and 65536)" << std::endl;
        return false;
    }

    if (data_dir.empty()) {
        std::cerr << "Data directory cannot be empty" << std::endl;
        return false;
    }

    return true;
}

void Config::print() const {
    std::cout << "=== SQL Engine Configuration ===" << std::endl;
    std::cout << "Mode:         " << (mode == RunMode::Distributed ? "distributed" : "embedded") << std::endl;
    std::cout << "Port:         " << port << std::endl;
    std::cout << "Data dir:     " << data_dir << std::endl;
    std::cout << "Max conns:    " << max_connections << std::endl;
    std::cout << "Buffer pool:  " << buffer_pool_size << " pages" << std::endl;
    std::cout << "Page size:    " << page_size << " bytes" << std::endl;
    std::cout << "Debug:        " << (debug ? "enabled" : "disabled") << std::endl;
    std::cout << "Verbose:      " << (verbose ? "enabled" : "disabled") << std::endl;
    std::cout << "================================" << std::endl;
}

std::string Config::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" 	

");
    if (start == std::string::npos) {
        return "";
    }
    size_t end = str.find_last_not_of(" 	

");
    return str.substr(start, end - start + 1);
}

} // namespace config
} // namespace cloudsql
