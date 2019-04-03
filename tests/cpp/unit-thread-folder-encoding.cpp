#include "catch.hpp"
#include "stryke/sequential.hpp"
#include "stryke/thread.hpp"
#include "stryke/options.hpp"
#include "utils_for_test.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterThread Folder encoding", "[Thread][Multifile]") {

  stryke::WriterOptions options;
  std::string root_folder = "data_test";
  fs::remove_all(root_folder);

  uint64_t nbr_rows = 100;

  SECTION("0 columns encoded with empty FolderEncode") {
    {
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::TimestampNumber, stryke::FolderEncode<>, stryke::Int> writer({"date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows);

  }
  SECTION("0 columns encoded with non FolderEncode") {
    {
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::TimestampNumber, stryke::Int> writer({"date", "value"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/" + "test-2018-12-10-0.orc");
    REQUIRE(tmp_a.nbr_columns == 3);
    REQUIRE(tmp_a.nbr_rows == nbr_rows);

  }
  SECTION("1 columns encoded <Int>") {
    {
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::TimestampNumber, stryke::FolderEncode<stryke::Int>, stryke::Int, stryke::Int> writer({"date", "folder1", "value", "folder1fordebug"}, root_folder, "test", options);
      for (uint64_t i = 0; i < nbr_rows; ++i) {
        writer.write(1544400000 + 86400 * i / (nbr_rows), i%2, i, i%2);
      }
    }

    stryke::BasicStats tmp_a = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=0/" + "test-2018-12-10-0-0.orc");
    REQUIRE(tmp_a.nbr_columns == 4);
    REQUIRE(tmp_a.nbr_rows == nbr_rows / 2.);

    stryke::BasicStats tmp_b = stryke::get_basic_stats(root_folder + "/year=2018/month=12/day=10/folder1=1/" + "test-2018-12-10-1-0.orc");
    REQUIRE(tmp_b.nbr_columns == 4);
    REQUIRE(tmp_b.nbr_rows == nbr_rows /2.);
  }


  fs::remove_all(root_folder);
}