/**
 * @file config_tests.cpp
 * @brief Unit tests for Config parsing, validation, and serialization
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "common/config.hpp"

using namespace cloudsql::config;

namespace {

// Helper to clean up test files
void cleanup(const std::string& file) {
    static_cast<void>(std::remove(file.c_str()));
}

// ============= Config Validation Tests =============

TEST(ConfigTests, Validate_PortZero) {
    Config cfg;
    cfg.port = 0;
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_PortTooHigh) {
    Config cfg;
    cfg.port = static_cast<uint16_t>(65536);  // Truncates to 0, triggers port==0 check
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_ClusterPortZero) {
    Config cfg;
    cfg.cluster_port = 0;
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_MaxConnectionsZero) {
    Config cfg;
    cfg.max_connections = 0;
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_BufferPoolSizeZero) {
    Config cfg;
    cfg.buffer_pool_size = 0;
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_PageSizeTooSmall) {
    Config cfg;
    cfg.page_size = 512;  // Below MIN_PAGE_SIZE (1024)
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_PageSizeTooLarge) {
    Config cfg;
    cfg.page_size = 131072;  // Above MAX_PAGE_SIZE (65536)
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_EmptyDataDir) {
    Config cfg;
    cfg.data_dir = "";
    EXPECT_FALSE(cfg.validate());
    cleanup("test.cfg");
}

TEST(ConfigTests, Validate_AllDefaults) {
    Config cfg;  // Uses all defaults
    EXPECT_TRUE(cfg.validate());
    cleanup("test.cfg");
}

// ============= Config Load Tests =============

TEST(ConfigTests, Load_EmptyFilename) {
    Config cfg;
    EXPECT_FALSE(cfg.load(""));
    cleanup("test.cfg");
}

TEST(ConfigTests, Load_FileNotFound) {
    Config cfg;
    EXPECT_FALSE(cfg.load("/nonexistent/path/config.cfg"));
    cleanup("test.cfg");
}

TEST(ConfigTests, Load_EmptyFile) {
    const std::string filename = "test_empty.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));            // Empty file is valid (uses defaults)
    EXPECT_EQ(cfg.port, Config::DEFAULT_PORT);  // Defaults preserved

    cleanup(filename);
}

TEST(ConfigTests, Load_EmptyLine) {
    const std::string filename = "test_emptyline.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));

    cleanup(filename);
}

TEST(ConfigTests, Load_CommentLine) {
    const std::string filename = "test_comment.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "# This is a comment\n";
        f << "port=1234\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.port, 1234);

    cleanup(filename);
}

TEST(ConfigTests, Load_LineWithoutEquals) {
    const std::string filename = "test_noequals.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "port 1234\n";  // No equals sign
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    // "port 1234" has no '=' so skipped; "valid_key" unknown so ignored
    // port remains at default value
    EXPECT_EQ(cfg.port, Config::DEFAULT_PORT);

    cleanup(filename);
}

TEST(ConfigTests, Load_ValidPort) {
    const std::string filename = "test_port.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "port=9000\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.port, 9000);

    cleanup(filename);
}

TEST(ConfigTests, Load_ModeDistributed) {
    const std::string filename = "test_mode_dist.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "mode=distributed\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.mode, RunMode::Coordinator);

    cleanup(filename);
}

TEST(ConfigTests, Load_ModeCoordinator) {
    const std::string filename = "test_mode_coord.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "mode=coordinator\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.mode, RunMode::Coordinator);

    cleanup(filename);
}

TEST(ConfigTests, Load_ModeData) {
    const std::string filename = "test_mode_data.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "mode=data\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.mode, RunMode::Data);

    cleanup(filename);
}

TEST(ConfigTests, Load_ModeStandalone) {
    const std::string filename = "test_mode_standalone.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "mode=standalone\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.mode, RunMode::Standalone);

    cleanup(filename);
}

TEST(ConfigTests, Load_UnknownKey) {
    const std::string filename = "test_unknown.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "unknown_key=should_be_ignored\n";
        f << "port=7777\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.port, 7777);  // Known key parsed, unknown ignored

    cleanup(filename);
}

TEST(ConfigTests, Load_WhitespaceAroundKeyValue) {
    const std::string filename = "test_whitespace.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "  port  =  8888  \n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.port, 8888);

    cleanup(filename);
}

// ============= Config Save Tests =============

TEST(ConfigTests, Save_EmptyFilename) {
    Config cfg;
    EXPECT_FALSE(cfg.save(""));
    cleanup("test.cfg");
}

TEST(ConfigTests, Save_UnwritablePath) {
    Config cfg;
    EXPECT_FALSE(cfg.save("/root/impossible_path/config.cfg"));
    cleanup("test.cfg");
}

TEST(ConfigTests, Save_RoundTrip) {
    const std::string filename = "test_roundtrip.cfg";
    cleanup(filename);

    Config original;
    original.port = 9999;
    original.cluster_port = 7777;
    original.max_connections = 50;
    original.buffer_pool_size = 256;
    original.page_size = 16384;
    original.mode = RunMode::Data;
    original.debug = true;
    original.verbose = false;

    EXPECT_TRUE(original.save(filename));

    Config loaded;
    EXPECT_TRUE(loaded.load(filename));
    EXPECT_EQ(loaded.port, 9999);
    EXPECT_EQ(loaded.cluster_port, 7777);
    EXPECT_EQ(loaded.max_connections, 50);
    EXPECT_EQ(loaded.buffer_pool_size, 256);
    EXPECT_EQ(loaded.page_size, 16384);
    EXPECT_EQ(loaded.mode, RunMode::Data);
    EXPECT_EQ(loaded.debug, true);
    EXPECT_EQ(loaded.verbose, false);

    cleanup(filename);
}

TEST(ConfigTests, Save_CoordinatorMode) {
    const std::string filename = "test_save_coord.cfg";
    cleanup(filename);

    Config cfg;
    cfg.mode = RunMode::Coordinator;
    EXPECT_TRUE(cfg.save(filename));

    // Verify file contains "coordinator"
    std::ifstream f(filename);
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    EXPECT_TRUE(content.find("mode=coordinator") != std::string::npos);

    cleanup(filename);
}

TEST(ConfigTests, Save_DataMode) {
    const std::string filename = "test_save_data.cfg";
    cleanup(filename);

    Config cfg;
    cfg.mode = RunMode::Data;
    EXPECT_TRUE(cfg.save(filename));

    std::ifstream f(filename);
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    EXPECT_TRUE(content.find("mode=data") != std::string::npos);

    cleanup(filename);
}

TEST(ConfigTests, Save_StandaloneMode) {
    const std::string filename = "test_save_standalone.cfg";
    cleanup(filename);

    Config cfg;
    cfg.mode = RunMode::Standalone;
    EXPECT_TRUE(cfg.save(filename));

    std::ifstream f(filename);
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    EXPECT_TRUE(content.find("mode=standalone") != std::string::npos);

    cleanup(filename);
}

// ============= Config Print Tests =============

TEST(ConfigTests, Print_StandaloneMode) {
    Config cfg;
    cfg.mode = RunMode::Standalone;

    testing::internal::CaptureStdout();
    cfg.print();
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_TRUE(output.find("Standalone") != std::string::npos);
    cleanup("test.cfg");
}

TEST(ConfigTests, Print_CoordinatorMode) {
    Config cfg;
    cfg.mode = RunMode::Coordinator;

    testing::internal::CaptureStdout();
    cfg.print();
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_TRUE(output.find("Coordinator") != std::string::npos);
    cleanup("test.cfg");
}

TEST(ConfigTests, Print_DataMode) {
    Config cfg;
    cfg.mode = RunMode::Data;

    testing::internal::CaptureStdout();
    cfg.print();
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_TRUE(output.find("Data") != std::string::npos);
    cleanup("test.cfg");
}

TEST(ConfigTests, Print_DebugEnabled) {
    Config cfg;
    cfg.debug = true;

    testing::internal::CaptureStdout();
    cfg.print();
    std::string output = testing::internal::GetCapturedStdout();

    EXPECT_TRUE(output.find("enabled") != std::string::npos);
    cleanup("test.cfg");
}

// ============= Config ClusterPort Tests =============

TEST(ConfigTests, Load_ClusterPort) {
    const std::string filename = "test_cluster_port.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "cluster_port=7500\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.cluster_port, 7500);

    cleanup(filename);
}

// ============= Config SeedNodes Tests =============

TEST(ConfigTests, Load_SeedNodes) {
    const std::string filename = "test_seeds.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "seed_nodes=host1:1234,host2:5678\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.seed_nodes, "host1:1234,host2:5678");

    cleanup(filename);
}

// ============= Config MaxConnections Tests =============

TEST(ConfigTests, Load_MaxConnections) {
    const std::string filename = "test_maxconn.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "max_connections=200\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.max_connections, 200);

    cleanup(filename);
}

// ============= Config BufferPoolSize Tests =============

TEST(ConfigTests, Load_BufferPoolSize) {
    const std::string filename = "test_bufsize.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "buffer_pool_size=512\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.buffer_pool_size, 512);

    cleanup(filename);
}

// ============= Config PageSize Tests =============

TEST(ConfigTests, Load_PageSize) {
    const std::string filename = "test_pagesize.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "page_size=16384\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.page_size, 16384);

    cleanup(filename);
}

// ============= Config DataDir Tests =============

TEST(ConfigTests, Load_DataDir) {
    const std::string filename = "test_datadir.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "data_dir=/tmp/custom_data\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_EQ(cfg.data_dir, "/tmp/custom_data");

    cleanup(filename);
}

// ============= Config Debug/Verbose Tests =============

TEST(ConfigTests, Load_DebugTrue) {
    const std::string filename = "test_debug.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "debug=true\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_TRUE(cfg.debug);

    cleanup(filename);
}

TEST(ConfigTests, Load_DebugFalse) {
    const std::string filename = "test_nodebug.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "debug=false\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_FALSE(cfg.debug);

    cleanup(filename);
}

TEST(ConfigTests, Load_VerboseOne) {
    const std::string filename = "test_verbose.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "verbose=1\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_TRUE(cfg.verbose);

    cleanup(filename);
}

// ============= Integration Tests =============

TEST(ConfigTests, LoadAndValidate_FullConfig) {
    const std::string filename = "test_full.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "port=6000\n";
        f << "cluster_port=7000\n";
        f << "data_dir=/tmp/testdb\n";
        f << "max_connections=150\n";
        f << "buffer_pool_size=256\n";
        f << "page_size=16384\n";
        f << "mode=coordinator\n";
        f << "debug=true\n";
        f.close();
    }

    Config cfg;
    EXPECT_TRUE(cfg.load(filename));
    EXPECT_TRUE(cfg.validate());
    EXPECT_EQ(cfg.port, 6000);
    EXPECT_EQ(cfg.cluster_port, 7000);
    EXPECT_EQ(cfg.data_dir, "/tmp/testdb");
    EXPECT_EQ(cfg.max_connections, 150);
    EXPECT_EQ(cfg.buffer_pool_size, 256);
    EXPECT_EQ(cfg.page_size, 16384);
    EXPECT_EQ(cfg.mode, RunMode::Coordinator);
    EXPECT_TRUE(cfg.debug);

    cleanup(filename);
}

TEST(ConfigTests, Load_InvalidKeyValuePair) {
    const std::string filename = "test_invalid_kv.cfg";
    cleanup(filename);

    {
        std::ofstream f(filename);
        f << "port=invalid_number\n";  // Should be numeric
        f.close();
    }

    // stoi will throw exception - the load will catch it or fail
    Config cfg;
    // This test documents current behavior - stoi throws on non-numeric
    EXPECT_THROW((void)cfg.load(filename), std::exception);

    cleanup(filename);
}

}  // namespace