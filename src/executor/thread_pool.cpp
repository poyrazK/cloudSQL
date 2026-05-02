/**
 * @file thread_pool.cpp
 * @brief ThreadPool implementation
 */

#include "executor/thread_pool.hpp"

namespace cloudsql::executor {

ThreadPool::ThreadPool(size_t num_threads) {
    workers_.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    cv_.wait(lock, [this] { return shutdown_ || !tasks_.empty(); });
                    if (shutdown_ && tasks_.empty()) {
                        return;
                    }
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
                task();
                pending_tasks_.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ThreadPool::wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] {
        return tasks_.empty() && pending_tasks_.load(std::memory_order_acquire) == 0;
    });
}

}  // namespace cloudsql::executor
