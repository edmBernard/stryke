#include "catch.hpp"
#include "stryke_read_for_test.hpp"
#include "stryke_template.hpp"
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterImpl Types", "[Simple]") {

  std::string filename = "test.orc";

  stryke::WriterOptions options;
  options.set_batch_size(10000);
  options.set_batch_max(0);
  options.set_stripe_size(10000);

  SECTION("Test Int") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Short> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Long> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Float> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Double> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Boolean> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Date> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::DateNumber> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Timestamp> writer({"date", "col1"}, filename, options);
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
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::TimestampNumber> writer({"date", "col1"}, filename, options);
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
