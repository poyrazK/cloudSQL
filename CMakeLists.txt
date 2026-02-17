cmake_minimum_required(VERSION 3.16)
project(sqlEngine VERSION 0.2.0 LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Position Independent Code (required for TSan and good practice for libraries)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Build options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(BUILD_COVERAGE "Build with coverage instrumentation" OFF)
set(USE_SANITIZER "address" CACHE STRING "Sanitizer to use: address, thread, or none")

# Configure Sanitizers
if(NOT USE_SANITIZER STREQUAL "none")
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        if (USE_SANITIZER STREQUAL "address")
            set(SAN_FLAGS "-fsanitize=address" "-fno-omit-frame-pointer")
        elseif (USE_SANITIZER STREQUAL "thread")
            set(SAN_FLAGS "-fsanitize=thread")
        endif()
        
        if (SAN_FLAGS)
            add_compile_options(${SAN_FLAGS})
            add_link_options(${SAN_FLAGS})
            message(STATUS "Enabled sanitizer: ${USE_SANITIZER}")
        endif()
    endif()
endif()

# Enable compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
endif()

if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
endif()

# Clang-Tidy integration
find_program(CLANG_TIDY_BIN 
    NAMES clang-tidy
    PATHS /opt/homebrew/opt/llvm/bin /usr/local/opt/llvm/bin /usr/bin /usr/local/bin
)

if(CLANG_TIDY_BIN)
    message(STATUS "Found clang-tidy: ${CLANG_TIDY_BIN}")
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_BIN}")
else()
    message(WARNING "clang-tidy not found")
endif()

if(BUILD_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
    endif()
endif()

# Source directories
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Collect all source files
file(GLOB_RECURSE ALL_SOURCES "${SRC_DIR}/*.cpp")

# Separate main.cpp from other sources
set(CORE_SOURCES ${ALL_SOURCES})
list(FILTER CORE_SOURCES EXCLUDE REGEX "src/main\\.cpp$")

# Create core library
add_library(sqlEngineCore STATIC ${CORE_SOURCES})
target_include_directories(sqlEngineCore PUBLIC ${INCLUDE_DIR})

# Create main executable
add_executable(sqlEngine src/main.cpp)
target_link_libraries(sqlEngine PRIVATE sqlEngineCore)

# Find and link required libraries
find_package(Threads REQUIRED)
target_link_libraries(sqlEngineCore PUBLIC Threads::Threads)

# System libraries for networking
if(UNIX AND NOT APPLE)
    target_link_libraries(sqlEngineCore PUBLIC rt)
endif()

# Build test executable
if(BUILD_TESTS)
    # Create test executable
    add_executable(sqlEngine_tests tests/cloudSQL_tests.cpp)
    target_link_libraries(sqlEngine_tests PRIVATE sqlEngineCore)
    
    # Create lock manager unit tests
    add_executable(lock_manager_tests tests/lock_manager_tests.cpp)
    target_link_libraries(lock_manager_tests PRIVATE sqlEngineCore)

    # Create server unit tests
    add_executable(server_tests tests/server_tests.cpp)
    target_link_libraries(server_tests PRIVATE sqlEngineCore)

    # Create transaction manager unit tests
    add_executable(transaction_manager_tests tests/transaction_manager_tests.cpp)
    target_link_libraries(transaction_manager_tests PRIVATE sqlEngineCore)

    # Create statement unit tests
    add_executable(statement_tests tests/statement_tests.cpp)
    target_link_libraries(statement_tests PRIVATE sqlEngineCore)

    # Create recovery unit tests
    add_executable(recovery_tests tests/recovery_tests.cpp)
    target_link_libraries(recovery_tests PRIVATE sqlEngineCore)

    # Check if GoogleTest is available
    find_package(GTest QUIET)
    if(GTest_FOUND)
        target_link_libraries(sqlEngine_tests PRIVATE GTest::GTest GTest::GTestMain)
        include(GoogleTest)
        gtest_discover_tests(sqlEngine_tests)
    endif()
    
    add_custom_target(run-tests
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/sqlEngine_tests
        DEPENDS sqlEngine_tests
        COMMENT "Running cloudSQL tests"
    )
endif()

# Installation
install(TARGETS sqlEngine RUNTIME DESTINATION bin)
install(DIRECTORY ${INCLUDE_DIR} DESTINATION include)

# Print configuration
message(STATUS "=============================================")
message(STATUS "cloudSQL Build Configuration")
message(STATUS "=============================================")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "  Build Tests: ${BUILD_TESTS}")
message(STATUS "  Sanitizer: ${USE_SANITIZER}")
message(STATUS "=============================================")
