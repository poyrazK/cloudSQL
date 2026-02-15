/**
 * @file lock_manager.cpp
 * @brief Lock Manager implementation
 */

#include "transaction/lock_manager.hpp"
#include <algorithm>

namespace cloudsql {
namespace transaction {

bool LockManager::acquire_shared(Transaction* txn, const std::string& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[rid];

    /* Check if we already hold a lock */
    for (const auto& req : queue.request_queue) {
        if (req.txn_id == txn->get_id()) {
            if (req.mode == LockMode::EXCLUSIVE || req.mode == LockMode::SHARED) return true;
        }
    }

    queue.request_queue.push_back({txn->get_id(), LockMode::SHARED, false});
    auto it = std::prev(queue.request_queue.end()); // Iterator to our request

    /* Wait loop */
    queue.cv.wait(lock, [&]() {
        /* Check if we are aborted */
        if (txn->get_state() == TransactionState::ABORTED) return true;

        /* Check compatibility */
        /* Shared locks are compatible with other shared locks, but not exclusive */
        bool compatible = true;
        for (auto iter = queue.request_queue.begin(); iter != it; ++iter) {
            if (iter->mode == LockMode::EXCLUSIVE) {
                compatible = false;
                break;
            }
        }
        return compatible;
    });

    if (txn->get_state() == TransactionState::ABORTED) {
        queue.request_queue.erase(it);
        return false;
    }

    it->granted = true;
    txn->add_shared_lock(rid);
    return true;
}

bool LockManager::acquire_exclusive(Transaction* txn, const std::string& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    auto& queue = lock_table_[rid];

    /* Check for lock upgrade */
    bool upgrade = false;
    auto it = queue.request_queue.end();
    
    for (auto iter = queue.request_queue.begin(); iter != queue.request_queue.end(); ++iter) {
        if (iter->txn_id == txn->get_id()) {
            if (iter->mode == LockMode::EXCLUSIVE) return true; // Already held
            if (iter->mode == LockMode::SHARED) {
                /* We have S lock, need upgrade */
                upgrade = true;
                it = iter; // Mark our current S lock
                break;
            }
        }
    }

    if (upgrade) {
        /* Release S lock temporarily or just modify request? */
        /* Simple upgrade: drop S, queue X. Real implementation needs care for deadlocks/starvation */
        /* For now, let's just queue a new X request and wait. This is simplistic. */
        /* NOTE: Upgrades are deadlock-prone without proper handling. */
    }

    queue.request_queue.push_back({txn->get_id(), LockMode::EXCLUSIVE, false});
    auto my_req = std::prev(queue.request_queue.end());

    queue.cv.wait(lock, [&]() {
        if (txn->get_state() == TransactionState::ABORTED) return true;
        
        /* Exclusive requires NO other locks held by OTHERS */
        bool can_grant = true;
        for (auto iter = queue.request_queue.begin(); iter != my_req; ++iter) {
            /* If it's us (upgrade case), we ignore our own previous lock? */
            /* Simplified: Strictly FIFO for X locks relative to others */
            if (iter->txn_id != txn->get_id()) {
                can_grant = false;
                break;
            }
        }
        return can_grant;
    });

    if (txn->get_state() == TransactionState::ABORTED) {
        queue.request_queue.erase(my_req);
        return false;
    }

    my_req->granted = true;
    txn->add_exclusive_lock(rid);
    return true;
}

bool LockManager::unlock(Transaction* txn, const std::string& rid) {
    std::unique_lock<std::mutex> lock(latch_);
    if (lock_table_.find(rid) == lock_table_.end()) return false;

    auto& queue = lock_table_[rid];
    bool found = false;
    for (auto it = queue.request_queue.begin(); it != queue.request_queue.end(); ++it) {
        if (it->txn_id == txn->get_id()) {
            queue.request_queue.erase(it);
            found = true;
            break;
        }
    }

    if (found) {
        queue.cv.notify_all();
    }
    return found;
}

} // namespace transaction
} // namespace cloudsql
