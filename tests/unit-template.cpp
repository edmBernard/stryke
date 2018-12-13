#include "catch.hpp"
#include "stryke_read_for_test.hpp"
#include "stryke_template.hpp"
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterImpl Types", "[Simple]") {

  std::string filename = "test.orc";

  SECTION("Test Int") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Short") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Short> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Long") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Long> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Float") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Float> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i + 1, i / 1000.);
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
        writer.write(i, i / 1000.);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Bool") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Boolean> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, (i%2==0));
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Date") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Date> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, "1990-12-18 12:26:20");
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test DateNumber") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::DateNumber> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, i + 1);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test Timestamp") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Timestamp> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, "1990-12-18 12:26:20");
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }
  SECTION("Test TimestampNumber") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::TimestampNumber> writer({"date", "col1"}, filename, 10000);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, i + 1);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 10);
  }

  fs::remove(filename);
}

TEST_CASE("OrcWriterImpl Batch", "[Simple]") {

  for (auto &&batchsize : {10, 100, 1000, 10000}) {
    for (auto &&nbr_rows : {10, 100, 1000, 100000}) {

      std::string filename = "test.orc";

      SECTION("Batch size: " + std::to_string(batchsize) + " nbr_rows: " + std::to_string(nbr_rows)) {
        {
          stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, filename, batchsize);
          for (int i = 0; i < nbr_rows; ++i) {
            writer.write(i, i);
          }
        }
        stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
        REQUIRE(tmp_b.nbr_columns == 3);
        REQUIRE(tmp_b.nbr_rows == nbr_rows);
      }

      fs::remove(filename);
    }
  }

}

TEST_CASE("OrcWriterImpl Lock File", "[Simple]") {

  std::string filename = "test.orc";

  SECTION("Without lock file") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Boolean> writer({"date", "col1"}, filename, false);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, (i%2==0));
        REQUIRE_FALSE(fs::exists(filename + ".lock"));
      }
    }
    REQUIRE_FALSE(fs::exists(filename + ".lock"));
  }
  SECTION("With lock file") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Boolean> writer({"date", "col1"}, filename, true);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, (i%2==0));
        REQUIRE(fs::exists(filename + ".lock"));
      }
    }
    REQUIRE_FALSE(fs::exists(filename + ".lock"));
  }

  fs::remove(filename);

}
