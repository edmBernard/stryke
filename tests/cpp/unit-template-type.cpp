#include "catch.hpp"
#include "utils_for_test.hpp"
#include "stryke/reader.hpp"
#include "stryke/core.hpp"
#include <filesystem>

namespace fs = std::filesystem;


template<typename T, typename U>
void test_simple_writer_impl(std::vector<U> input, std::string filename, stryke::WriterOptions options) {

  {
    stryke::OrcWriterImpl<T> writer({"col1"}, filename, options);

    for (auto && i: input) {
      writer.write(i);
    }
  }
  stryke::BasicStats tmp_b = stryke::get_basic_stats(filename);
  REQUIRE(tmp_b.nbr_columns == 2);
  REQUIRE(tmp_b.nbr_rows == input.size());

  auto result = stryke::orcReader<T>(filename);
  int count = 0;
  for (auto && i: input) {
    REQUIRE(std::get<0>(result[count]).data == i);
    ++count;
  }
}

template<typename T, typename U>
void test_simple_writer(std::string filename, stryke::WriterOptions options) {
  std::vector<U> input;
  for (U i = -10; i < 10; ++i) {
    input.push_back(i);
  }
  test_simple_writer_impl<T, U>(input, filename, options);
}


TEST_CASE("OrcWriterImpl Types", "[Simple]") {

  std::string filename = "test.orc";

  stryke::WriterOptions options;
  options.set_batch_size(10000);
  options.set_batch_max(0);
  options.set_stripe_size(10000);

  SECTION("Test Int") {
    test_simple_writer<stryke::Int, int>(filename, options);
  }

  SECTION("Test Short") {
    test_simple_writer<stryke::Int, short>(filename, options);
  }

  SECTION("Test Long") {
    test_simple_writer<stryke::Long, long>(filename, options);
  }

  SECTION("Test String") {
    std::vector<std::string> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(std::to_string(i));
    }
    test_simple_writer_impl<stryke::String, std::string>(input, filename, options);
  }

  SECTION("Test Float") {
    test_simple_writer<stryke::Float, float>(filename, options);
  }

  SECTION("Test Double") {
    test_simple_writer<stryke::Double, double>(filename, options);
  }

  SECTION("Test Bool") {
    std::vector<bool> input;
    for (int i = -10; i < 10; ++i) {
      input.push_back(i % 2 == 0);
    }

    {
      stryke::OrcWriterImpl<stryke::Boolean> writer({"col1"}, filename, options);

      for (auto && i: input) {
        writer.write(bool(i));  // Add bool because vector<bool> are bitfield
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
    test_simple_writer_impl<stryke::Date, std::string>(input, filename, options);
  }

  SECTION("Test DateNumber") {
    test_simple_writer<stryke::DateNumber, long>(filename, options);
  }

  SECTION("Test Timestamp") {

    SECTION("Without nanosecond") {
      std::vector<std::string> input;
      for (int i = -10; i < 10; ++i) {
        input.push_back("1990-12-18 12:26:20");
      }
      test_simple_writer_impl<stryke::Timestamp, std::string>(input, filename, options);
    }

    SECTION("With nanosecond") {
      std::vector<std::string> input;
      for (int i = -10; i < 10; ++i) {
        input.push_back("1990-12-18 12:26:20.123456789");
      }
      test_simple_writer_impl<stryke::Timestamp, std::string>(input, filename, options);
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
