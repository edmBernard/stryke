#include "catch.hpp"
#include "stryke_read_for_test.hpp"
#include "stryke_reader.hpp"
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
      stryke::OrcWriterImpl<stryke::Int> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::Int>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Short") {
    {
      stryke::OrcWriterImpl<stryke::Short> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::Short>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Long") {
    {
      stryke::OrcWriterImpl<stryke::Long> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::Long>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test String") {
    {
      stryke::OrcWriterImpl<stryke::String> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(std::to_string(i));
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    // auto result = stryke::orcReader<stryke::String>(filename);
    // int count = 0;
    // for (int i = -10; i < 10; ++i) {
    //   REQUIRE(std::get<0>(result[count]).data == i);
    //   ++count;
    // }
  }
  SECTION("Test Float") {
    {
      stryke::OrcWriterImpl<stryke::Float> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i / 1000.);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::Float>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data ==  Approx(i / 1000.));
      ++count;
    }
  }
  SECTION("Test Double") {
    {
      stryke::OrcWriterImpl<stryke::Double> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i / 1000.);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::Double>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == Approx(i / 1000.));
      ++count;
    }
  }
  SECTION("Test Bool") {
    {
      stryke::OrcWriterImpl<stryke::Boolean> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write((i % 2 == 0));
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    // auto result = stryke::orcReader<stryke::Boolean>(filename);
    // int count = 0;
    // for (int i = -10; i < 10; ++i) {
    //   REQUIRE(std::get<0>(result[count]).data == (i % 2 == 0));
    //   ++count;
    // }
  }
  SECTION("Test Date") {
    {
      stryke::OrcWriterImpl<stryke::Date> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write("1990-12-18");
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::Date>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == "1990-12-18");
      ++count;
    }
  }
  SECTION("Test DateNumber") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::DateNumber>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Timestamp") {

    SECTION("Without nanosecond") {
      {
        stryke::OrcWriterImpl<stryke::Timestamp> writer({"col1"}, filename, options);
        for (int i = -10; i < 10; ++i) {
          writer.write("1990-12-18 12:26:20");
        }
      }
      stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == 20);

      auto result = stryke::orcReader<stryke::Timestamp>(filename);
      int count = 0;
      for (int i = -10; i < 10; ++i) {
        REQUIRE(std::get<0>(result[count]).data == "1990-12-18 12:26:20");
        ++count;
      }
    }
    SECTION("With nanosecond") {
      {
        stryke::OrcWriterImpl<stryke::Timestamp> writer({"col1"}, filename, options);
        for (int i = -10; i < 10; ++i) {
          writer.write("1990-12-18 12:26:20.123456789");
        }
      }
      stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == 20);

      auto result = stryke::orcReader<stryke::Timestamp>(filename);
      int count = 0;
      for (int i = -10; i < 10; ++i) {
        REQUIRE(std::get<0>(result[count]).data == "1990-12-18 12:26:20.123456789");
        ++count;
      }
    }
  }

  SECTION("Test TimestampNumber") {
    {
      stryke::OrcWriterImpl<stryke::TimestampNumber> writer({"col1"}, filename, options);

      for (int i = -10; i < 10; ++i) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == 20);

    auto result = stryke::orcReader<stryke::TimestampNumber>(filename);
    int count = 0;
    for (int i = -10; i < 10; ++i) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }

  fs::remove(filename);
}
