#include "catch.hpp"
#include "stryke/reader.hpp"
#include "stryke/dispatch.hpp"
#include "stryke/type.hpp"
#include <filesystem>

namespace fs = std::filesystem;


TEST_CASE("OrcWriterDispatch Column name", "[Dispatch]") {

  std::string root_folder = "data_test";
  fs::remove_all(root_folder);

  stryke::WriterOptions options;
  options.set_batch_size(10000);
  options.set_batch_max(0);
  options.set_stripe_size(10000);

  std::vector<std::string> cnl;
  for (int i = 0; i < 10; ++i) {
    cnl.push_back("col" + std::to_string(i));
  }

  SECTION("OrcWriterDispatch<FolderEncode<Int>, Int, Int>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int>, stryke::Int, stryke::Int> writer({cnl[0], cnl[1], cnl[2]}, root_folder, "test", options);
      writer.write(1, 1, 1);
    }

    stryke::OrcReader<stryke::Int, stryke::Int> reader(root_folder + "/" + cnl[0] + "=1/test-1-0.orc");
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[1]);
    REQUIRE(columns_read[1] == cnl[2]);

  }

  SECTION("OrcWriterDispatch<FolderEncode<>, Int, Int>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<>, stryke::Int, stryke::Int> writer({cnl[1], cnl[2]}, root_folder, "test", options);
      writer.write(1, 1);
    }

    stryke::OrcReader<stryke::Int, stryke::Int> reader(root_folder + "/test-0.orc");
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[1]);
    REQUIRE(columns_read[1] == cnl[2]);

  }

  SECTION("OrcWriterDispatch<Int, Int>") {
    {
      stryke::OrcWriterDispatch<stryke::Int, stryke::Int> writer({cnl[1], cnl[2]}, root_folder, "test", options);
      writer.write(1, 1);
    }

    stryke::OrcReader<stryke::Int, stryke::Int> reader(root_folder + "/test-0.orc");
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[1]);
    REQUIRE(columns_read[1] == cnl[2]);

  }

  SECTION("OrcWriterDispatch<FolderEncode<Int>, Int, Int, Int, Int, Int, Int>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int>, stryke::Int, stryke::Int, stryke::Int, stryke::Int, stryke::Int, stryke::Int> writer({cnl[0], cnl[1], cnl[2], cnl[3], cnl[4], cnl[5], cnl[6]}, root_folder, "test", options);
      writer.write(1, 1, 1, 1, 1, 1, 1);
    }

    stryke::OrcReader<stryke::Int, stryke::Int, stryke::Int, stryke::Int, stryke::Int, stryke::Int> reader(root_folder + "/" + cnl[0] + "=1/test-1-0.orc");
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[1]);
    REQUIRE(columns_read[1] == cnl[2]);
    REQUIRE(columns_read[2] == cnl[3]);
    REQUIRE(columns_read[3] == cnl[4]);
    REQUIRE(columns_read[4] == cnl[5]);
    REQUIRE(columns_read[5] == cnl[6]);

  }

  SECTION("OrcWriterDispatch<FolderEncode<Int, Int, Int, Int, Int, Int>, Int>") {
    {
      stryke::OrcWriterDispatch<stryke::FolderEncode<stryke::Int, stryke::Int, stryke::Int, stryke::Int, stryke::Int, stryke::Int>, stryke::Int> writer({cnl[0], cnl[1], cnl[2], cnl[3], cnl[4], cnl[5], cnl[6]}, root_folder, "test", options);
      writer.write(1, 1, 1, 1, 1, 1, 1);
    }

    std::string path = "";
    for (int i=0; i<6; ++i) {
      path += cnl[i] + "=1/";
    }
    stryke::OrcReader<stryke::Int> reader(root_folder + "/" +  path + "test-1-1-1-1-1-1-0.orc");
    auto columns_read = reader.get_cols_name();

    REQUIRE(columns_read[0] == cnl[6]);

  }

  fs::remove_all(root_folder);
}

