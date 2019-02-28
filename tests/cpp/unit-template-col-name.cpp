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

  std::vector<std::string> cnl;
  for (int i = 0; i < 10; ++i) {
    cnl.push_back("col" + std::to_string(i));
  }

  SECTION("OrcWriterImpl<Int") {
    {
      stryke::OrcWriterImpl<stryke::Int> writer({cnl[0]}, filename, options);
      writer.write(1);
    }

    stryke::OrcReader<stryke::Int> reader(filename);
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[0]);

  }
  SECTION("OrcWriterImpl<Int, Int>") {
    {
      stryke::OrcWriterImpl<stryke::Int, stryke::Int> writer({cnl[0], cnl[1]}, filename, options);
      writer.write(1, 1);
    }

    stryke::OrcReader<stryke::Int, stryke::Int> reader(filename);
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[0]);
    REQUIRE(columns_read[1] == cnl[1]);

  }

  fs::remove(filename);
}

