#include "catch.hpp"
#include "stryke/sequential.hpp"
#include "stryke/miscellaneous/sequential_duplicate.hpp"
#include "stryke/miscellaneous/dispatch_duplicate.hpp"
#include "stryke/options.hpp"
#include "stryke/thread.hpp"
#include "utils_for_test.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterThread Writers", "[Thread][Multifile]") {

  stryke::WriterOptions options;
  uint64_t nbr_rows = 100;
  std::string root_folder = "data_test";

  // SECTION("Sequential") {
  //   {
  //     stryke::OrcWriterThread<stryke::OrcWriterSequential, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
  //     for (uint64_t i = 0; i < nbr_rows; ++i) {
  //       writer.write(17875 + i / (nbr_rows), i);
  //     }
  //   }

  //   stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
  //   REQUIRE(tmp_b.nbr_columns == 3);
  //   REQUIRE(tmp_b.nbr_rows == nbr_rows);

  // }
  SECTION("Dispatch Duplicate") {
    {
      stryke::OrcWriterThread<stryke::OrcWriterDispatchDuplicate, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(17875 + i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == nbr_rows);

  }
  SECTION("Sequential Duplicate") {
    {
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(17875 + i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == nbr_rows);

  }
  fs::remove_all(root_folder);
}
