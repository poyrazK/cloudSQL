cmake_minimum_required(VERSION 3.10)
project(cloudSQL)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Build Options
option(STRICT_LINT "Enable strict linting" ON)
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(BUILD_COVERAGE "Enable code coverage reporting" OFF)

# Add include directories
include_directories(include)

# Find GoogleTest
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/heads/main.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Find Google Benchmark
FetchContent_Declare(
  googlebenchmark
  URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googlebenchmark)

# Find SQLite3 (for comparison benchmarks)
FetchContent_Declare(
  sqlite3
  URL https://www.sqlite.org/2024/sqlite-amalgamation-3450300.zip
)
FetchContent_GetProperties(sqlite3)
if(NOT sqlite3_POPULATED)
  FetchContent_Populate(sqlite3)
  add_library(sqlite3 STATIC ${sqlite3_SOURCE_DIR}/sqlite3.c)
  target_include_directories(sqlite3 PUBLIC ${sqlite3_SOURCE_DIR})
  # Optimize SQLite for speed
  target_compile_definitions(sqlite3 PRIVATE SQLITE_THREADSAFE=0 SQLITE_OMIT_LOAD_EXTENSION)
endif()

# Core Library
set(CORE_SOURCES
    src/common/config.cpp
    src/catalog/catalog.cpp
    src/storage/storage_manager.cpp
    src/storage/buffer_pool_manager.cpp
    src/storage/lru_replacer.cpp
    src/storage/heap_table.cpp
    src/storage/btree_index.cpp
    src/parser/lexer.cpp
    src/parser/parser.cpp
    src/parser/statement.cpp
    src/parser/expression.cpp
    src/executor/operator.cpp
    src/executor/query_executor.cpp
    src/network/rpc_client.cpp
    src/network/rpc_server.cpp
    src/network/server.cpp
    src/transaction/lock_manager.cpp
    src/transaction/transaction_manager.cpp
    src/recovery/log_manager.cpp
    src/recovery/log_record.cpp
    src/recovery/recovery_manager.cpp
    src/distributed/raft_group.cpp
    src/distributed/raft_manager.cpp
    src/distributed/distributed_executor.cpp
    src/common/bloom_filter.cpp
    src/storage/columnar_table.cpp
)

add_library(sqlEngineCore ${CORE_SOURCES})

# Coverage
if(BUILD_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        # Use LLVM coverage mapping for branch coverage support (Clang only)
        target_compile_options(sqlEngineCore PUBLIC -fprofile-instr-generate -fcoverage-mapping -O0)
        target_link_libraries(sqlEngineCore PUBLIC -fprofile-instr-generate -fcoverage-mapping)
    else()
        # Fallback for GCC - uses traditional gcov format
        target_compile_options(sqlEngineCore PUBLIC --coverage -O0)
        target_link_libraries(sqlEngineCore PUBLIC --coverage)
    endif()
endif()

# Sanitizers
if(ENABLE_ASAN)
    target_compile_options(sqlEngineCore PUBLIC -fsanitize=address)
    target_link_libraries(sqlEngineCore PUBLIC -fsanitize=address)
endif()

if(ENABLE_TSAN)
    target_compile_options(sqlEngineCore PUBLIC -fsanitize=thread)
    target_link_libraries(sqlEngineCore PUBLIC -fsanitize=thread)
endif()

# Main executable
add_executable(cloudSQL src/main.cpp)
target_link_libraries(cloudSQL sqlEngineCore)

# Test helper macro
macro(add_cloudsql_test NAME SOURCE)
    add_executable(${NAME} ${SOURCE})
    target_link_libraries(${NAME} sqlEngineCore GTest::gtest_main GTest::gmock)
    add_test(NAME ${NAME} COMMAND ${NAME})
endmacro()

# Benchmark helper macro
macro(add_cloudsql_benchmark NAME SOURCE)
    add_executable(${NAME} ${SOURCE})
    target_link_libraries(${NAME} sqlEngineCore benchmark::benchmark benchmark::benchmark_main)
endmacro()

# Tests
if(BUILD_TESTS)
    enable_testing()
    add_cloudsql_test(catalog_coverage_tests tests/catalog_coverage_tests.cpp)
    add_cloudsql_test(transaction_coverage_tests tests/transaction_coverage_tests.cpp)
    add_cloudsql_test(utils_coverage_tests tests/utils_coverage_tests.cpp)
    add_cloudsql_test(bloom_filter_tests tests/bloom_filter_test.cpp)
    add_cloudsql_test(cloudSQL_tests tests/cloudSQL_tests.cpp)
    add_cloudsql_test(server_tests tests/server_tests.cpp)
    add_cloudsql_test(statement_tests tests/statement_tests.cpp)
    add_cloudsql_test(transaction_manager_tests tests/transaction_manager_tests.cpp)
    add_cloudsql_test(lock_manager_tests tests/lock_manager_tests.cpp)
    add_cloudsql_test(recovery_tests tests/recovery_tests.cpp)
    add_cloudsql_test(recovery_manager_tests tests/recovery_manager_tests.cpp)
    add_cloudsql_test(buffer_pool_tests tests/buffer_pool_tests.cpp)
    add_cloudsql_test(raft_tests tests/raft_tests.cpp)
    add_cloudsql_test(distributed_tests tests/distributed_tests.cpp)
    add_cloudsql_test(raft_sim_tests tests/raft_simulation_tests.cpp)
    add_cloudsql_test(multi_raft_tests tests/multi_raft_tests.cpp)
    add_cloudsql_test(distributed_txn_tests tests/distributed_txn_tests.cpp)
    add_cloudsql_test(analytics_tests tests/analytics_tests.cpp)
    add_cloudsql_test(raft_manager_tests tests/raft_manager_tests.cpp)
    add_cloudsql_test(raft_group_tests tests/raft_group_tests.cpp)
    add_cloudsql_test(raft_protocol_tests tests/raft_protocol_tests.cpp)
    add_cloudsql_test(columnar_table_tests tests/columnar_table_tests.cpp)
    add_cloudsql_test(heap_table_tests tests/heap_table_tests.cpp)
    add_cloudsql_test(lexer_tests tests/lexer_tests.cpp)
    add_cloudsql_test(parser_tests tests/parser_tests.cpp)
    add_cloudsql_test(expression_tests tests/expression_tests.cpp)
    add_cloudsql_test(btree_index_tests tests/btree_index_tests.cpp)
    add_cloudsql_test(storage_manager_tests tests/storage_manager_tests.cpp)
    add_cloudsql_test(rpc_server_tests tests/rpc_server_tests.cpp)
    add_cloudsql_test(rpc_client_tests tests/rpc_client_tests.cpp)
    add_cloudsql_test(operator_tests tests/operator_tests.cpp)
    add_cloudsql_test(query_executor_tests tests/query_executor_tests.cpp)
    add_cloudsql_test(distributed_executor_tests tests/distributed_executor_tests.cpp)

    add_custom_target(run-tests
        COMMAND ${CMAKE_CTEST_COMMAND}
        COMMENT "Running all tests via CTest")
endif()

# Benchmarks
option(BUILD_BENCHMARKS "Enable performance benchmarks" OFF)
if(BUILD_BENCHMARKS)
    add_cloudsql_benchmark(storage_bench benchmarks/storage_bench.cpp)
    add_cloudsql_benchmark(execution_bench benchmarks/execution_bench.cpp)
    add_cloudsql_benchmark(network_bench benchmarks/network_bench.cpp)
    
    # SQLite comparison benchmark
    add_executable(sqlite_comparison_bench benchmarks/sqlite_comparison_bench.cpp)
    target_link_libraries(sqlite_comparison_bench sqlEngineCore benchmark::benchmark benchmark::benchmark_main sqlite3)
endif()
