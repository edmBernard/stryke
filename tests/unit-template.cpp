#include "catch.hpp"
#include "stryke_read_for_test.hpp"
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterImpl", "[Simple]") {

  std::string filename = "example.orc";

  SECTION("Test Int") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i+1, i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Double") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Double> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i+1, i/1000.);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }

  fs::remove(filename);
}
