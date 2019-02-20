#include "catch.hpp"
#include "stryke/dispatch.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
#include "stryke/options.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterDispatch Data in folder name", "[Dispatch]") {

  stryke::WriterOptions options;
  std::string root_folder = "data_test";
  uint64_t nbr_rows = 10000;

  SECTION("0 columns encoded with empty FolderEncode") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<>, stryke::TimestampNumber, stryke::Int> writer({"date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/" + "test-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows);

  }
  SECTION("0 columns encoded with non FolderEncode") {
    {
      stryke::OrcWriterDispatch<stryke::TimestampNumber, stryke::Int> writer({"date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/" + "test-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows);

  }
  SECTION("1 columns encoded <Int>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int>, stryke::TimestampNumber, stryke::Int, stryke::Int> writer({"folder1", "date", "value", "folder1fordebug"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(i%2, 1544400000 + 86400 * i / (nbr_rows), i, i%2);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/folder1=0/" + "test-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/folder1=1/" + "test-1-0.orc");
    REQUIRE(tmp_b.nbr_columns == 4);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("1 columns encoded <String>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::String>, stryke::TimestampNumber, stryke::Int, stryke::String> writer({"folder1", "date", "value", "folder1fordebug"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(std::to_string(i%2), 1544400000 + 86400 * i / (nbr_rows), i, std::to_string(i%2));
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/folder1=0/" + "test-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/folder1=1/" + "test-1-0.orc");
    REQUIRE(tmp_b.nbr_columns == 4);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("1 columns encoded <DateNumber>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::DateNumber>, stryke::DateNumber, stryke::Int> writer({"folder1", "date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(17875 + i / (nbr_rows/2.), 17875 + i / (nbr_rows/2.), i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/" + "test-2018-12-11-0.orc");
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("1 columns encoded <TimestampNumber>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::TimestampNumber>, stryke::TimestampNumber, stryke::Int> writer({"folder1", "date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows/2.), 1544400000 + 86400 * i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/" + "test-2018-12-11-0.orc");
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("1 columns encoded <Date>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Date>, stryke::Date, stryke::Int> writer({"folder1", "date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        std::string date1 = "2018-12-10";
        std::string date2 = "2018-12-11";
        writer.write(i%2==0 ? date1 : date2, i%2==0 ? date1 : date2, i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/" + "test-2018-12-11-0.orc");
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("1 columns encoded <Timestamp>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Timestamp>, stryke::Timestamp, stryke::Int> writer({"folder1", "date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        std::string date1 = "2018-12-10 12:26:20.123456789";
        std::string date2 = "2018-12-11 12:26:20.123456789";
        writer.write(i%2==0 ? date1 : date2, i%2==0 ? date1 : date2, i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/" + "test-2018-12-11-0.orc");
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }
  SECTION("2 columns encoded <Int, String>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int, stryke::String>, stryke::TimestampNumber, stryke::Int, stryke::Int, stryke::String> writer({"folder1", "folder2", "date", "value", "folder1fordebug", "folder2fordebug"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(i%2, std::to_string(i%4), 1544400000 + 86400 * i / (nbr_rows), i, i%2, std::to_string(i%4));
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/folder1=0/folder2=0/" + "test-0-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 5);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 4.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/folder1=0/folder2=2/" + "test-0-2-0.orc");
    REQUIRE(tmp_b.nbr_columns == 5);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /4.);


    stryke::BasicStats tmp_c = stryke::get_basic_stats(root_folder + "/folder1=1/folder2=1/" + "test-1-1-0.orc");
    REQUIRE(tmp_c.nbr_columns == 5);
    REQUIRE(tmp_c.nbr_rows == nbr_rows / 4.);

    stryke::BasicStats tmp_d = stryke::get_basic_stats(root_folder + "/folder1=1/folder2=3/" + "test-1-3-0.orc");
    REQUIRE(tmp_d.nbr_columns == 5);
    REQUIRE(tmp_d.nbr_rows == nbr_rows /4.);
  }

  SECTION("2 columns encoded <TimestampNumber, Int>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::TimestampNumber, stryke::Int>, stryke::TimestampNumber, stryke::Int, stryke::Int> writer({"date", "folder1", "datefordebug", "value", "folder1fordebug"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows/2.), i%2, 1544400000 + 86400 * i / (nbr_rows/2.), i, i%2);
      }
    }
    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=0/" + "test-2018-12-10-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 4.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/folder1=0/" + "test-2018-12-11-0-0.orc");
    REQUIRE(tmp_b.nbr_columns == 4);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /4.);


    stryke::BasicStats tmp_c = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=1/" + "test-2018-12-10-1-0.orc");
    REQUIRE(tmp_c.nbr_columns == 4);
    REQUIRE(tmp_c.nbr_rows == nbr_rows / 4.);

    stryke::BasicStats tmp_d = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=11/folder1=1/" + "test-2018-12-11-1-0.orc");
    REQUIRE(tmp_d.nbr_columns == 4);
    REQUIRE(tmp_d.nbr_rows == nbr_rows /4.);
  }

  fs::remove_all(root_folder);
}

