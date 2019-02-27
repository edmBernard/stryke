#include "catch.hpp"
#include "stryke/miscellaneous/sequential_duplicate.hpp"
#include "stryke/thread.hpp"
#include "stryke/options.hpp"
#include "utils_for_test.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterThread Folder encoding", "[Thread][Multifile]") {

  stryke::WriterOptions options;
  std::string root_folder = "data_test";
  uint64_t nbr_rows = 10000;

  SECTION("0 columns encoded with empty FolderEncode") {
    {
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::TimestampNumber, stryke::FolderEncode<>, stryke::Int> writer({"date", "value"}, root_folder, "test", options);
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
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::TimestampNumber, stryke::Int> writer({"date", "value"}, root_folder, "test", options);
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
      stryke::OrcWriterThread<stryke::OrcWriterSequentialDuplicate, stryke::TimestampNumber, stryke::FolderEncode<stryke::Int>, stryke::Int, stryke::Int> writer({"folder1", "date", "value", "folder1fordebug"}, root_folder, "test", options);
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


  fs::remove_all(root_folder);
}