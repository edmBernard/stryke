#include "catch.hpp"
#include "stryke/reader.hpp"
#include "stryke/core.hpp"
#include "stryke/type.hpp"
#include <filesystem>

namespace fs = std::filesystem;


TEST_CASE("OrcWriterImpl Column name", "[Core]") {

  std::string filename = "test.orc";

  stryke::WriterOptions options;
  options.set_batch_size(10000);
  options.set_batch_max(0);
  options.set_stripe_size(10000);

  std::vector<std::string> column_name_list;
  for (int i = 0; i < 10; ++i) {
    column_name_list.push_back("col" + std::to_string(i));
  }

  SECTION("1") {
    {
      stryke::OrcWriterImpl<stryke::Int> writer({column_name_list[0]}, filename, options);
      writer.write(1);
    }

    stryke::OrcReader<stryke::Int> reader(filename);
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == column_name_list[0]);

  }
  SECTION("2") {
    {
      stryke::OrcWriterImpl<stryke::Int, stryke::Int> writer({column_name_list[0], column_name_list[1]}, filename, options);
      writer.write(1, 1);
    }

    stryke::OrcReader<stryke::Int> reader(filename);
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == column_name_list[0]);
    REQUIRE(columns_read[1] == column_name_list[1]);

  }

  fs::remove(filename);
}

