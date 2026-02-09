cmake_minimum_required(VERSION 3.10)
project(sqlEngine VERSION 0.1.0 LANGUAGES C)

# C Standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Enable compiler warnings
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic")
endif()

# Build options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

# Source directories
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Collect source files
file(GLOB_RECURSE SOURCES "${SRC_DIR}/*.c")
file(GLOB_RECURSE HEADERS "${INCLUDE_DIR}/*.h")

# Create executable
add_executable(sqlEngine
    ${SOURCES}
    ${HEADERS}
)

# Include directories
target_include_directories(sqlEngine PRIVATE
    ${INCLUDE_DIR}
)

# Link libraries
target_link_libraries(sqlEngine PRIVATE
    pthread
    m
)

# System libraries for networking
if(UNIX AND NOT APPLE)
    target_link_libraries(sqlEngine PRIVATE
        rt
    )
endif()

# Tests
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Installation
install(TARGETS sqlEngine RUNTIME DESTINATION bin)
install(FILES ${HEADERS} DESTINATION include/sqlEngine)

# Print configuration
message(STATUS "SQL Engine Build Configuration:")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  C Standard: ${CMAKE_C_STANDARD}")
message(STATUS "  Build Tests: ${BUILD_TESTS}")
