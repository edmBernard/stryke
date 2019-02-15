#include "catch.hpp"
#include "stryke/dispatch.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
#include "stryke/options.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterDispatch Data in folder name", "[Multifile]") {

  stryke::WriterOptions options;
  std::string root_folder = "data_test";
  uint64_t nbr_rows = 10000;

  SECTION("0 columns encoded with empty FolderEncode") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<>, stryke::TimestampNumber, stryke::Int, stryke::Int> writer({"date", "value", "_folder1"}, root_folder, "test_", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i, i%2);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test_2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows);

  }
  SECTION("0 columns encoded with non FolderEncode") {
    {
      stryke::OrcWriterDispatch<stryke::TimestampNumber, stryke::Int, stryke::Int> writer({"date", "value", "folder1"}, root_folder, "test_", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i, i%2);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test_2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows);

  }
  SECTION("1 columns encoded Int") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int>, stryke::TimestampNumber, stryke::Int, stryke::Int> writer({"folder1", "date", "value", "folder1fordebug"}, root_folder, "test_", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(i%2, 1544400000 + 86400 * i / (nbr_rows), i, i%2);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=0/" + "test_2018-12-10-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=1/" + "test_2018-12-10-1-0.orc");
    REQUIRE(tmp_b.nbr_columns == 4);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("1 columns encoded String") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::String>, stryke::TimestampNumber, stryke::Int, stryke::String> writer({"folder1", "date", "value", "folder1fordebug"}, root_folder, "test_", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(std::to_string(i%2), 1544400000 + 86400 * i / (nbr_rows), i, std::to_string(i%2));
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=0/" + "test_2018-12-10-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=1/" + "test_2018-12-10-1-0.orc");
    REQUIRE(tmp_b.nbr_columns == 4);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("2 columns encoded") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int, stryke::String>, stryke::TimestampNumber, stryke::Int, stryke::Int, stryke::String> writer({"folder1", "folder2", "date", "value", "folder1fordebug", "folder1fordebug"}, root_folder, "test_", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(i%2, std::to_string(i%4), 1544400000 + 86400 * i / (nbr_rows), i, i%2, std::to_string(i%4));
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=0/folder2=0/" + "test_2018-12-10-0-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 5);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 4.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=0/folder2=2/" + "test_2018-12-10-0-2-0.orc");
    REQUIRE(tmp_b.nbr_columns == 5);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /4.);


    stryke::BasicStats tmp_c = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=1/folder2=1/" + "test_2018-12-10-1-1-0.orc");
    REQUIRE(tmp_c.nbr_columns == 5);
    REQUIRE(tmp_c.nbr_rows == nbr_rows / 4.);

    stryke::BasicStats tmp_d = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=1/folder2=3/" + "test_2018-12-10-1-3-0.orc");
    REQUIRE(tmp_d.nbr_columns == 5);
    REQUIRE(tmp_d.nbr_rows == nbr_rows /4.);
  }

  fs::remove_all(root_folder);
}

