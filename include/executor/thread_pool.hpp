/**
 * @file thread_pool.hpp
 * @brief Fixed-size thread pool for parallel query execution
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace cloudsql::executor {

/**
 * @brief A fixed-size thread pool for parallel task execution.
 *
 * Worker threads pull tasks from a shared queue until shutdown().
 * submit() returns a std::future for the caller's result.
 */
class ThreadPool {
public:
    /**
     * @brief Construct a thread pool with the given number of workers.
     * @param num_threads Number of worker threads. Defaults to hardware
     * concurrency.
     */
    explicit ThreadPool(size_t num_threads = std::thread::hardware_concurrency());

    /** @brief Destructor — signals shutdown and joins all workers. */
    ~ThreadPool();

    // Non-copyable / non-movable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    /**
     * @brief Submit a callable for asynchronous execution.
     * @tparam F Callable type (function, lambda, etc.)
     * @param f The callable to execute
     * @return std::future with the result of invoking f
     */
    template <typename F>
    std::future<std::invoke_result_t<F>> submit(F&& f) {
        using R = std::invoke_result_t<F>;
        auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));
        auto result = task->get_future();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            tasks_.emplace([task]() { (*task)(); });
        }
        pending_tasks_.fetch_add(1, std::memory_order_acq_rel);
        cv_.notify_one();
        return result;
    }

    /**
     * @brief Signal all workers to stop after their current task.
     *
     * After shutdown() the pool cannot accept new tasks.
     */
    void shutdown();

    /**
     * @brief Block until all submitted tasks complete.
     */
    void wait();

    /** @brief Number of worker threads in the pool. */
    size_t num_threads() const { return workers_.size(); }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool shutdown_ = false;
    std::atomic<size_t> pending_tasks_{0};
};

}  // namespace cloudsql::executor
