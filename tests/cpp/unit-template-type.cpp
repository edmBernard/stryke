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
    std::vector<int> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i);
    }

    {
      stryke::OrcWriterImpl<stryke::Int> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Int>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Short") {
    std::vector<short> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i);
    }

    {
      stryke::OrcWriterImpl<stryke::Short> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Short>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Long") {
    std::vector<long> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i);
    }

    {
      stryke::OrcWriterImpl<stryke::Long> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Long>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test String") {
    std::vector<std::string> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(std::to_string(i));
    }

    {
      stryke::OrcWriterImpl<stryke::String> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::String>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Float") {
    std::vector<float> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i / 1000.);
    }

    {
      stryke::OrcWriterImpl<stryke::Float> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Float>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Double") {
    std::vector<double> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i / 1000.);
    }

    {
      stryke::OrcWriterImpl<stryke::Double> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Double>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Bool") {
    std::vector<bool> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i % 2 == 0);
    }

    {
      stryke::OrcWriterImpl<stryke::Boolean> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(bool(i));
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Boolean>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Date") {
    std::vector<std::string> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back("1990-12-18");
    }

    {
      stryke::OrcWriterImpl<stryke::Date> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::Date>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test DateNumber") {
    std::vector<long> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i);
    }

    {
      stryke::OrcWriterImpl<stryke::DateNumber> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::DateNumber>(filename);
    int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
  }
  SECTION("Test Timestamp") {

    SECTION("Without nanosecond") {
      std::vector<std::string> input;
      for (int i = -10; i < 10; ++i) {
        input.push_back("1990-12-18 12:26:20");
      }

      {
        stryke::OrcWriterImpl<stryke::Timestamp> writer({"col1"}, filename, options);
      for (auto && i: input) {
        writer.write(i);
      }
      }
      stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == input.size());

      auto result = stryke::orcReader<stryke::Timestamp>(filename);
      int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
    }
    SECTION("With nanosecond") {
      std::vector<std::string> input;
      for (int i = -10; i < 10; ++i) {
        input.push_back("1990-12-18 12:26:20.123456789");
      }

      {
        stryke::OrcWriterImpl<stryke::Timestamp> writer({"col1"}, filename, options);
      for (auto &&i: input) {
        writer.write(i);
      }
      }
      stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
      REQUIRE(tmp_b.nbr_columns == 2);
      REQUIRE(tmp_b.nbr_rows == input.size());

      auto result = stryke::orcReader<stryke::Timestamp>(filename);
      int count = 0;
    for (auto && i: input) {
      REQUIRE(std::get<0>(result[count]).data == i);
      ++count;
    }
    }
  }

  SECTION("Test TimestampNumber") {
    std::vector<double> input;
    for (int i = -30; i < 10; ++i) {
      input.push_back(i / 10.);
    }

    {
      stryke::OrcWriterImpl<stryke::TimestampNumber> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(i);
      }
    }
    stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
    REQUIRE(tmp_b.nbr_columns == 2);
    REQUIRE(tmp_b.nbr_rows == input.size());

    auto result = stryke::orcReader<stryke::TimestampNumber>(filename);
    int count = 0;
    for (auto && i: input) {
      if (i > -1 && i < 0) {
        // It seem that orc was not able to store timestamp with -1 second on there itegrale part and nanosecond "1969-12-31 23:59:59.001" will be saved as "1970-01-01 00:00:00.001"
        REQUIRE_FALSE(std::get<0>(result[count]).data == i);
      } else {
        REQUIRE(std::get<0>(result[count]).data == Approx(i));
      }
      ++count;
    }
  }

  fs::remove(filename);
}
