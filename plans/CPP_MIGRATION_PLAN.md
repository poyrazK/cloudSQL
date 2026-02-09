# cloudSQL C to C++ Migration Plan

## Overview

Migrate the cloudSQL distributed SQL engine from C to C++ to improve type safety, memory management, and developer productivity.

## Current State Analysis

### Source Files (11 files)
| File | Purpose | Lines | Issues |
|------|---------|-------|--------|
| `src/main.c` | Entry point | ~50 | Simple |
| `src/catalog/catalog.c` | System catalog | ~200 | Moderate |
| `src/common/config.c` | Configuration | ~100 | Simple |
| `src/executor/evaluator.c` | Expression evaluator | ~500 | Complex - unions, type casting |
| `src/executor/executor.c` | Query executor | ~700 | Moderate - function pointers |
| `src/network/server.c` | PostgreSQL wire protocol | ~300 | Moderate |
| `src/parser/ast.c` | AST construction | ~900 | Complex - pointer semantics |
| `src/parser/lexer.c` | Tokenization | ~400 | Simple |
| `src/storage/btree.c` | B-tree index | ~500 | Moderate |
| `src/storage/heap.c` | Heap table storage | ~300 | Moderate |
| `src/storage/manager.c` | Storage manager | ~200 | Simple |

### Key Problems in C Code

1. **Type Safety Issues**
   - `token_t` uses union with `float_val`/`int_val` but `value_t` uses `float64_val`/`int64_val`
   - Manual type casting with runtime checks
   - No compile-time type guarantees

2. **Memory Management**
   - Manual `malloc/free` throughout
   - No RAII - resources leak on error paths
   - `ALLOC_ZERO` macro pattern error-prone

3. **String Handling**
   - `char*` everywhere
   - Manual `strdup` calls
   - Buffer overflow risks

4. **Function Pointers**
   - Executor uses C-style function pointers
   - No virtual functions or polymorphism

## Migration Strategy

### Phase 1: Foundation (Week 1)
- [ ] Update CMakeLists.txt for C++17
- [ ] Create new header files with C++ classes
- [ ] Rename `.c` → `.cpp`
- [ ] Add `extern "C"` guards for C compatibility if needed

### Phase 2: Core Types (Week 2)
- [ ] Replace `value_t` union with `std::variant`
- [ ] Replace `token_t` union with proper types
- [ ] Create `Value` class with type-safe accessors
- [ ] Create `String` wrapper with RAII

### Phase 3: Parser Layer (Week 3)
- [ ] Convert `lexer.c` → `Lexer` class
- [ ] Convert `ast.c` → `AST` classes with inheritance
- [ ] Replace manual memory with smart pointers
- [ ] Use `std::vector` for dynamic arrays

### Phase 4: Executor Layer (Week 4)
- [ ] Replace function pointers with virtual methods
- [ ] Create operator base class hierarchy
- [ ] Implement RAII for operator lifecycle
- [ ] Use `std::unique_ptr` for ownership

### Phase 5: Storage Layer (Week 5)
- [ ] Convert `heap.c` → `HeapTable` class
- [ ] Convert `btree.c` → `BTreeIndex` class
- [ ] Implement proper iterator patterns
- [ ] Add move semantics

### Phase 6: Testing & Polish (Week 6)
- [ ] Add GoogleTest for unit tests
- [ ] Fix all compiler warnings
- [ ] Add static analysis
- [ ] Performance benchmarking

## C++ Architecture

### Namespace Structure
```cpp
namespace cloudsql {
    namespace common {
        class Value;
        class String;
        class Config;
    }
    
    namespace catalog {
        class Catalog;
        class TableMetadata;
    }
    
    namespace parser {
        class Lexer;
        class ASTNode;
        class Expression;
        class SelectStatement;
        // ...
    }
    
    namespace executor {
        class Operator;
        class SeqScanOperator;
        class FilterOperator;
        // ...
        class QueryExecutor;
    }
    
    namespace storage {
        class HeapTable;
        class BTreeIndex;
        class StorageManager;
    }
    
    namespace network {
        class Server;
        class Connection;
    }
}
```

### Key Type Transformations

#### 1. Value Type (Before → After)

**Before (C):**
```c
typedef struct {
    value_type_t type;
    bool is_null;
    union {
        bool bool_val;
        int64_t int64_val;
        double float64_val;
        char *string_val;
    } value;
} value_t;
```

**After (C++):**
```cpp
namespace cloudsql::common {

enum class ValueType {
    Null = 0, Bool = 1, Int64 = 5, Float64 = 7,
    Text = 11, // etc
};

class Value {
private:
    ValueType type_;
    std::variant<
        std::monostate,  // Null
        bool,
        int64_t,
        double,
        std::string
    > data_;
    
public:
    Value() : type_(ValueType::Null), data_(std::monostate{}) {}
    
    // Type-safe accessors
    auto type() const -> ValueType { return type_; }
    auto is_null() const -> bool { return type_ == ValueType::Null || 
                                          std::holds_alternative<std::monostate>(data_); }
    
    auto as_int64() const -> int64_t { return std::get<int64_t>(data_); }
    auto as_float64() const -> double { return std::get<double>(data_); }
    auto as_string() const -> const std::string& { return std::get<std::string>(data_); }
    
    // Factory methods
    static Value from_int64(int64_t v) { Value val; val.type_ = ValueType::Int64; val.data_ = v; return val; }
    static Value from_float64(double v) { Value val; val.type_ = ValueType::Float64; val.data_ = v; return val; }
    static Value from_string(std::string v) { Value val; val.type_ = ValueType::Text; val.data_ = std::move(v); return val; }
};

} // namespace cloudsql::common
```

#### 2. Token Type (Before → After)

**Before (C):**
```c
typedef struct {
    token_type_t type;
    union {
        int64_t int_val;
        double float_val;
        char *str_val;
    } value;
} token_t;
```

**After (C++):**
```cpp
namespace cloudsql::parser {

enum class TokenType {
    // Keywords
    Select, From, Where, Insert, Into, Values,
    // ... more keywords
    // Literals
    Identifier, String, Number, Param,
    // Operators
    Eq, Ne, Lt, Le, Gt, Ge, Plus, Minus, Star, Slash,
    End
};

class Token {
private:
    TokenType type_;
    std::variant<std::monostate, int64_t, double, std::string> value_;
    
public:
    Token() : type_(TokenType::End), value_(std::monostate{}) {}
    explicit Token(TokenType t) : type_(t), value_(std::monostate{}) {}
    
    Token(TokenType t, int64_t v) : type_(t), value_(v) {}
    Token(TokenType t, double v) : type_(t), value_(v) {}
    Token(TokenType t, std::string v) : type_(t), value_(std::move(v)) {}
    
    auto type() const -> TokenType { return type_; }
    auto is_keyword() const -> bool;
    auto is_literal() const -> bool;
    
    auto as_int64() const -> int64_t { return std::get<int64_t>(value_); }
    auto as_float64() const -> double { return std::get<double>(value_); }
    auto as_string() const -> const std::string& { return std::get<std::string>(value_); }
};

} // namespace cloudsql::parser
```

#### 3. Expression AST (Before → After)

**Before (C):**
```c
typedef struct ast_expression_t {
    expr_type_t type;
    struct ast_expression_t *left;
    struct ast_expression_t *right;
    token_type_t op;
    struct ast_expression_t *expr;
    char *column_name;
    value_t value;
    char *func_name;
    struct ast_expression_t **func_args;
    int num_args;
    struct ast_expression_t **list;
    bool not_flag;
} ast_expression_t;
```

**After (C++):**
```cpp
namespace cloudsql::parser {

enum class ExprType { Binary, Unary, Column, Constant, Function, Subquery, In, Like, Between, IsNull };

class Expression {
public:
    virtual ~Expression() = default;
    virtual auto evaluate() -> common::Value = 0;
    virtual auto to_string() const -> std::string = 0;
};

class BinaryExpr : public Expression {
    std::unique_ptr<Expression> left_;
    TokenType op_;
    std::unique_ptr<Expression> right_;
    
public:
    BinaryExpr(std::unique_ptr<Expression> l, TokenType op, std::unique_ptr<Expression> r)
        : left_(std::move(l)), op_(op), right_(std::move(r)) {}
    
    auto evaluate() -> common::Value override;
    auto to_string() const -> std::string override;
};

class ConstantExpr : public Expression {
    common::Value value_;
    
public:
    explicit ConstantExpr(common::Value v) : value_(std::move(v)) {}
    auto evaluate() -> common::Value override { return value_; }
    auto to_string() const -> std::string override;
};

class ColumnExpr : public Expression {
    std::string name_;
    
public:
    explicit ColumnExpr(std::string n) : name_(std::move(n)) {}
    auto evaluate() -> common::Value override;
    auto to_string() const -> std::string override { return name_; }
};

} // namespace cloudsql::parser
```

#### 4. Executor Operator (Before → After)

**Before (C):**
```c
typedef struct operator_t {
    operator_type_t type;
    void *op_state;
    struct operator_t **children;
    int num_children;
    char **column_names;
    value_type_t *column_types;
    int num_columns;
    exec_state_t exec_state;
    int (*init)(struct operator_t *op, ast_node_t *ast, catalog_t *catalog);
    int (*open)(struct operator_t *op);
    tuple_t *(*next)(struct operator_t *op);
    int (*close)(struct operator_t *op);
} operator_t;
```

**After (C++):**
```cpp
namespace cloudsql::executor {

class Tuple;
class Schema;

class Operator {
public:
    virtual ~Operator() = default;
    
    virtual auto open() -> Result<void, Error> = 0;
    virtual auto next() -> std::optional<Tuple> = 0;
    virtual auto close() -> Result<void, Error> = 0;
    virtual auto schema() const -> const Schema& = 0;
};

class SeqScanOperator : public Operator {
    std::string table_name_;
    std::unique_ptr<storage::HeapTable> table_;
    std::optional<Tuple> current_tuple_;
    
public:
    SeqScanOperator(std::string table_name, catalog::Catalog& catalog);
    
    auto open() -> Result<void, Error> override;
    auto next() -> std::optional<Tuple> override;
    auto close() -> Result<void, Error> override;
    auto schema() const -> const Schema& override;
};

class FilterOperator : public Operator {
    std::unique_ptr<Operator> child_;
    std::unique_ptr<parser::Expression> condition_;
    
public:
    FilterOperator(std::unique_ptr<Operator> child, std::unique_ptr<parser::Expression> cond)
        : child_(std::move(child)), condition_(std::move(cond)) {}
    
    auto open() -> Result<void, Error> override;
    auto next() -> std::optional<Tuple> override;
    auto close() -> Result<void, Error> override;
    auto schema() const -> const Schema& override;
};

} // namespace cloudsql::executor
```

## CMakeLists.txt Updates

```cmake
cmake_minimum_required(VERSION 3.16)
project(sqlEngine VERSION 0.2.0 LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Werror")
endif()

# Build options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

# Dependencies
find_package(Threads REQUIRED)

# Source directories
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Collect source files
file(GLOB_RECURSE SOURCES "${SRC_DIR}/*.cpp")
file(GLOB_RECURSE HEADERS "${INCLUDE_DIR}/*.hpp")

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
    Threads::Threads
)

# Tests
if(BUILD_TESTS)
    enable_testing()
    FetchContentDeclare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
    )
    FetchContent_MakeAvailable(googletest)
    
    add_executable(sqlEngine_tests
        tests/unit_tests.cpp
        tests/lexer_test.cpp
        tests/ast_test.cpp
        tests/executor_test.cpp
    )
    target_link_libraries(sqlEngine_tests PRIVATE
        sqlEngine
        GTest::gtest_main
    )
    include(GoogleTest)
    gtest_discover_tests(sqlEngine_tests)
endif()
```

## File Renaming Map

| Old (C) | New (C++) |
|---------|-----------|
| `include/common/types.h` | `include/common/value.hpp` |
| `include/common/common.h` | `include/common/macros.hpp` |
| `include/parser/lexer.h` | `include/parser/lexer.hpp` |
| `include/parser/ast.h` | `include/parser/ast.hpp` |
| `include/executor/executor.h` | `include/executor/operator.hpp` |
| `include/executor/evaluator.h` | `include/executor/evaluator.hpp` |
| `include/catalog/catalog.h` | `include/catalog/catalog.hpp` |
| `include/storage/heap.h` | `include/storage/heap_table.hpp` |
| `include/storage/btree.h` | `include/storage/btree_index.hpp` |
| `include/storage/manager.h` | `include/storage/manager.hpp` |
| `include/network/server.h` | `include/network/server.hpp` |

## Benefits of Migration

| Aspect | C (Before) | C++ (After) |
|--------|-----------|-------------|
| Type Safety | Runtime checks | Compile-time with `std::variant` |
| Memory | Manual `malloc/free` | RAII, smart pointers |
| Strings | `char*` + `strdup` | `std::string` |
| Collections | Manual arrays | `std::vector`, `std::map` |
| Polymorphism | Function pointers | Virtual functions |
| Error Handling | Error codes | Exceptions + `std::expected` |
| Namespacing | Prefixes (`sql_engine_`) | `namespace cloudsql` |
| Iterators | Manual while loops | Range-based for |

## Risk Mitigation

1. **Keep working lexer test** - `test_lexer.c` passes, can be used as reference
2. **Incremental migration** - One module at a time
3. **C compatibility layer** - Use `extern "C"` for PostgreSQL wire protocol
4. **Performance validation** - Benchmark after each phase

## Success Criteria

- [ ] All source files compile with C++20
- [ ] No memory leaks (valgrind/ASan clean)
- [ ] All existing tests pass
- [ ] Type safety enforced at compile time
- [ ] PostgreSQL wire protocol still works
- [ ] Performance within 10% of original C implementation
