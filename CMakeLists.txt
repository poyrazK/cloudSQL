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
    src/storage/columnar_table.cpp
)

add_library(sqlEngineCore ${CORE_SOURCES})

# Coverage
if(BUILD_COVERAGE)
    target_compile_options(sqlEngineCore PUBLIC --coverage -O0)
    target_link_libraries(sqlEngineCore PUBLIC --coverage)
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

# Tests
if(BUILD_TESTS)
    enable_testing()
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

    add_custom_target(run-tests
        COMMAND ${CMAKE_CTEST_COMMAND}
        COMMENT "Running all tests via CTest")
endif()
