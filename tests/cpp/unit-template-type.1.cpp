#include "catch.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
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
      for (int i = -10; i < 10; ++i) {
        writer.write(i, i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 3);
    REQUIRE(tmp_b.nbr_rows == 20);
  }
  SECTION("Test String") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::String> writer({"date", "col1"}, filename, options);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, std::to_string(i));
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

TEST_CASE("OrcWriterImpl One Column", "[Simple]") {

  std::string filename = "test.orc";
  stryke::WriterOptions options;
  options.set_batch_size(10000);
  options.set_batch_max(0);
  options.set_stripe_size(10000);

  SECTION("DateNumber column") {
    {
      options.disable_lock_file();

      stryke::OrcWriterImpl<stryke::DateNumber> writer({"date"}, filename, options);
      for (int i = 0; i < 10; ++i) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 10);
  }

  fs::remove(filename);

}
