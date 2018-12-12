#include "catch.hpp"
#include "stryke_multifile.hpp"
#include "stryke_read_for_test.hpp"
#include "stryke_template.hpp"
#include "stryke_thread.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterThread Split", "[Thread][Multifile]") {

  SECTION("Split by date") {
    uint64_t batchsize = 1000;
    uint64_t nbr_rows = 10000;

    std::string root_folder = "data_test";
    SECTION("0 split") {
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }
      }

      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);
    }
    SECTION("1 split") {
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows / 2.), i);
        }
      }

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/11/" + "test_2018-12-11-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
    }

    fs::remove_all(root_folder);
  }

  SECTION("Split by number of batch") {
    uint64_t batchsize = 1000;
    uint64_t nbr_rows = 10000;

    std::string root_folder = "data_test";
    SECTION("0 split") {
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }
      }

      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);
    }
    SECTION("1 split") {
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 5);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }

      }

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-1.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows / 2.);
    }

    fs::remove_all(root_folder);
  }

  SECTION("Split by force") {
    uint64_t batchsize = 1000;
    uint64_t nbr_rows = 10000;

    std::string root_folder = "data_test";
    SECTION("1 split sync") {

      uint64_t check_point = 10;
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
          if (i == check_point - 1) {
            writer.close_sync();
            tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
            REQUIRE(tmp_b.nbr_columns == 3);
            REQUIRE(tmp_b.nbr_rows == check_point);
          }
        }
      }

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-1.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows - check_point);
    }
    SECTION("1 split async") {

      uint64_t check_point = 10;
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
          if (i == check_point - 1) {

            writer.close_async();
            while (!writer.has_closed()) {}

            tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
            REQUIRE(tmp_b.nbr_columns == 3);
            REQUIRE(tmp_b.nbr_rows == check_point);
          }
        }
      }

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-1.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows - check_point);
    }
    SECTION("2 split sync") {

      uint64_t check_point1 = 10;
      uint64_t check_point2 = 100;
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);

          if (i == check_point1 - 1) {
            writer.close_sync();
            tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
            REQUIRE(tmp_b.nbr_columns == 3);
            REQUIRE(tmp_b.nbr_rows == check_point1);
          }

          if (i == check_point2 - 1) {
            writer.close_sync();
            tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-1.orc");
            REQUIRE(tmp_b.nbr_columns == 3);
            REQUIRE(tmp_b.nbr_rows == check_point2 - check_point1);
          }
        }
      }

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-2.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows - check_point2);
    }
    SECTION("2 split async") {

      uint64_t check_point1 = 10;
      uint64_t check_point2 = 100;
      stryke::BasicStats tmp_b;
      {
        stryke::OrcWriterThread<stryke::OrcWriterMulti, stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);

          if (i == check_point1 - 1) {

            writer.close_async();

            while (!writer.has_closed()) {}

            tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
            REQUIRE(tmp_b.nbr_columns == 3);
            REQUIRE(tmp_b.nbr_rows == check_point1);
          }

          if (i == check_point2 - 1) {

            writer.close_async();

            while (!writer.has_closed()) {}

            tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-1.orc");
            REQUIRE(tmp_b.nbr_columns == 3);
            REQUIRE(tmp_b.nbr_rows == check_point2 - check_point1);
          }
        }
      }

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-2.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows - check_point2);
    }

    fs::remove_all(root_folder);
  }
}