#include <iostream>
#include <string>

#include "catalog/catalog.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"
#include "test_utils.hpp"

using namespace cloudsql::recovery;

namespace {

using cloudsql::tests::tests_failed;
using cloudsql::tests::tests_passed;

TEST(RecoveryManager_Basic) {
    cloudsql::storage::StorageManager disk_manager("./test_data");
    cloudsql::storage::BufferPoolManager bpm(10, disk_manager);
    auto catalog = cloudsql::Catalog::create();
    LogManager log_manager("./test_data/test.log");

    RecoveryManager rm(bpm, *catalog, log_manager);
    EXPECT_TRUE(rm.recover());
}

}  // namespace

int main() {
    std::cout << "Recovery Manager Unit Tests\n";
    std::cout << "===========================\n";

    RUN_TEST(RecoveryManager_Basic);

    std::cout << "\nResults: \n" << tests_passed << " passed, \n" << tests_failed << " failed\n";
    return (tests_failed > 0);
}
