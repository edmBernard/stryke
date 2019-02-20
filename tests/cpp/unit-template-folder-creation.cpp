#include "catch.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
#include "stryke/options.hpp"
#include <filesystem>

namespace fs = std::filesystem;


void test_write_folder(std::string folder, std::string filename) {
    stryke::WriterOptions options;

    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, folder, filename, options);
      writer.write(1, 1);
    }

    stryke::BasicStats tmp_b = stryke::get_basic_stats(fs::path(folder) / filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 1);
    fs::remove(fs::path(folder) / filename);
    fs::path tmp = fs::path(folder) / filename;
    while (!tmp.empty()) {
      fs::remove(tmp);
      tmp = tmp.parent_path();
    }
}


TEST_CASE("OrcWriterImpl folder", "[Core]") {

    SECTION("Empty folder string") {
      test_write_folder("",  "test.orc");
    }
    SECTION("1 level folder string") {
      test_write_folder("data_test_folder",  "test.orc");
    }
    SECTION("2 level folder string") {
      test_write_folder("data_test_folder/subdata_test_folder", "test.orc");
    }

    SECTION("1 level folder in filename string") {
      test_write_folder("",  "data_test_folder/test.orc");
    }
    SECTION("2 level folder in filename string") {
      test_write_folder("",  "data_test_folder/subdata_test_folder/test.orc");
    }

}

