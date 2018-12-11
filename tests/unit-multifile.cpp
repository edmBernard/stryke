#include "catch.hpp"
#include "stryke_multifile.hpp"
#include "stryke_read_for_test.hpp"
#include "stryke_template.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterMulti Split", "[Multifile]") {


  SECTION("Split by date") {
    uint64_t batchsize = 1000;
    uint64_t nbr_rows = 10000;

    std::string root_folder = "data_test";
    SECTION("0 split") {
      {
        stryke::OrcWriterMulti<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }
      }
      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);
    }
    SECTION("1 split") {
      {
        stryke::OrcWriterMulti<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows/2.), i);
        }
      }

      stryke::BasicStats tmp_b;

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows/2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/11/" + "test_2018-12-11-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows/2.);
    }


    fs::remove_all(root_folder);

  }

  SECTION("Split by number of batch") {
    uint64_t batchsize = 1000;
    uint64_t nbr_rows = 10000;

    std::string root_folder = "data_test";
    SECTION("0 split") {
      {
        stryke::OrcWriterMulti<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 0);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }
      }
      stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows);

    }
    SECTION("1 split") {
      {
        stryke::OrcWriterMulti<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, root_folder, "test_", batchsize, 5);
        for (uint64_t i = 0; i < nbr_rows; ++i) {
          writer.write(17875 + i / (nbr_rows), i);
        }
      }

      stryke::BasicStats tmp_b;

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-0.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows/2.);

      tmp_b = stryke::get_basic_stats(root_folder + "/2018/12/10/" + "test_2018-12-10-1.orc");
      REQUIRE(tmp_b.nbr_columns == 3);
      REQUIRE(tmp_b.nbr_rows == nbr_rows/2.);
    }

    fs::remove_all(root_folder);
  }

}