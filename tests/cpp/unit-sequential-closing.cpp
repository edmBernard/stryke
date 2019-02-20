#include "catch.hpp"
#include "stryke/sequential.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
#include "stryke/options.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterSequential test file closing", "[Sequential]") {

  stryke::WriterOptions options;
  options.set_batch_size(1000);
  options.set_batch_max(0);
  options.set_stripe_size(500);

  uint64_t nbr_rows = 10000;

  std::string root_folder = "data_test";

  SECTION("Split by TimestampNumber") {

    SECTION("0 split") {
      {
        stryke::OrcWriterSequential<stryke::TimestampNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(1544400000 + 86400 * i / (nbr_rows), i);
        }
      }

      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);
    }
    SECTION("1 split") {
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterSequential<stryke::TimestampNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(1544400000 + 86400 * i / (nbr_rows / 2.), i);
          if (1 == i / (nbr_rows / 2.)) {
            tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
            REQUIRE_FALSE(fs::exists(root_folder + "/date=0/" + "test-2018-12-10-0.orc" + ".lock"));
            REQUIRE(tmp_b.nbr_columns == 2);
            REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
          }
        }
      }

      tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/" + "test-2018-12-11-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
    }
  }
  SECTION("Split by DateNumber") {

    SECTION("0 split") {
      {
        stryke::OrcWriterSequential<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }
      }

      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);
    }
    SECTION("1 split") {
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterSequential<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows / 2.), i);
          if (1 == i / (nbr_rows / 2.)) {
            tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
            REQUIRE_FALSE(fs::exists(root_folder + "/date=0/" + "test-2018-12-10-0.orc" + ".lock"));
            REQUIRE(tmp_b.nbr_columns == 2);
            REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
          }
        }

      }

      tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/" + "test-2018-12-11-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
    }
  }
  SECTION("Split by Int") {

    SECTION("0 split") {
      {
        stryke::OrcWriterSequential<stryke::Int, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(i / (nbr_rows), i);
        }
      }

      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/date=0/" + "test-0-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);
    }
    SECTION("1 split") {
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterSequential<stryke::Int, stryke::Int> writer({"date", "col1"}, root_folder, "test", options);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(i / (nbr_rows / 2.), i);
          if (i == nbr_rows / 2.) {
            tmp_b = stryke::get_basic_stats(root_folder + "/date=0/" + "test-0-0.orc");
            REQUIRE_FALSE(fs::exists(root_folder + "/date=0/" + "test-0-0.orc" + ".lock"));
            REQUIRE(tmp_b.nbr_columns == 2);
            REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
          }
        }

      }

      tmp_b = stryke::get_basic_stats(root_folder + "/date=0/" + "test-0-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/date=1/" + "test-1-0.orc");
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
    }
  }

  fs::remove_all(root_folder);
}