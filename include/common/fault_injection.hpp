/**
 * @file fault_injection.hpp
 * @brief Fault injection utility for testing error handling paths
 */

#pragma once

#include <atomic>
#include <cstdint>

namespace cloudsql {
namespace common {

// Fault injection modes — used as values for the fault injection flag
enum FaultMode : int {
    FAULT_NONE = 0,
    FAULT_LOG_COMMIT,
    FAULT_LOG_ABORT,
    FAULT_LOG_PREPARE,
    FAULT_INDEX_REMOVE,
    FAULT_INDEX_INSERT,
    FAULT_PHYSICAL_REMOVE,
    FAULT_UNDO_REMOVE,
};

class FaultInjection {
   public:
    static FaultInjection& instance();

    void set_fault(FaultMode mode);
    void clear();
    bool should_fault(FaultMode mode) const;

   private:
    FaultInjection() = default;

    std::atomic<int> fault_mode_{FAULT_NONE};
};

inline FaultInjection& FaultInjection::instance() {
    static FaultInjection inst;
    return inst;
}

inline void FaultInjection::set_fault(FaultMode mode) {
    fault_mode_.store(static_cast<int>(mode), std::memory_order_release);
}

inline void FaultInjection::clear() {
    fault_mode_.store(static_cast<int>(FAULT_NONE), std::memory_order_release);
}

inline bool FaultInjection::should_fault(FaultMode mode) const {
    return fault_mode_.load(std::memory_order_acquire) == static_cast<int>(mode);
}

// FAULT_IF(mode) — returns true when fault is armed for that mode
#define FAULT_IF(mode) (cloudsql::common::FaultInjection::instance().should_fault(mode))

}  // namespace common
}  // namespace cloudsql