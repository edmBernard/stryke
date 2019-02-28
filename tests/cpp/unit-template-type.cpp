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

  auto reader = stryke::OrcReader<T>(filename);
  auto result = reader.get_data();
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

template<typename stdType, typename strykeType>
void test_type_cast(stdType input) {
  strykeType b(input);
  REQUIRE(input == b);
  REQUIRE(input == stdType(b));
}

template<typename strykeType>
void test_type_cast_with_string(std::string input) {
  strykeType b(input);
  REQUIRE(input == std::string(b));
}

TEST_CASE("Types implicit cast", "[Core]") {

  SECTION("Test Int") {
    test_type_cast<int, stryke::Int>(12345);
  }

  SECTION("Test Short") {
    test_type_cast<short, stryke::Short>(12345);
  }

  SECTION("Test Long") {
    test_type_cast<long, stryke::Long>(12345);
  }

  SECTION("Test String") {
    test_type_cast_with_string<stryke::String>("my super string");
  }

  SECTION("Test Float") {
    test_type_cast<float, stryke::Float>(1234567.12345678);
  }

  SECTION("Test Double") {
    test_type_cast<double, stryke::Double>(1234567.12345678);
  }

  SECTION("Test Bool") {
    test_type_cast<bool, stryke::Boolean>(1234567.12345678);
  }

  SECTION("Test Date") {
    test_type_cast_with_string<stryke::Date>("2018-01-11");
  }

  SECTION("Test DateNumber") {
    test_type_cast<long, stryke::DateNumber>(1234567);
  }

  SECTION("Test Timestamp") {
    test_type_cast_with_string<stryke::Date>("2018-01-11 22:10:10");
  }

  SECTION("Test TimestampNumber") {
    test_type_cast<double, stryke::TimestampNumber>(1234567.1234567);
  }

}



TEST_CASE("OrcWriterImpl Types", "[Core]") {

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

    auto reader = stryke::OrcReader<stryke::Boolean>(filename);
    auto result = reader.get_data();
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

    SECTION("Without hour") {
      std::vector<std::string> input;
      std::vector<std::string> expected_output;
      for (int i = -10; i < 10; ++i) {
        input.push_back("2018-12-18");
        expected_output.push_back("2018-12-18 00:00:00");
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

      auto reader = stryke::OrcReader<stryke::Timestamp>(filename);
      auto result = reader.get_data();
      int count = 0;
      for (auto && i: expected_output) {
        REQUIRE(std::get<0>(result[count]).data == i);
        ++count;
      }
    }

    SECTION("Without nanosecond") {
      std::vector<std::string> input;
      for (int i = -10; i < 10; ++i) {
        input.push_back("2018-12-18 12:26:20");
      }
      test_simple_writer_impl<stryke::Timestamp, std::string>(input, filename, options);
    }

    SECTION("With nanosecond") {
      std::vector<std::string> input;
      for (int i = -10; i < 10; ++i) {
        input.push_back("2018-12-18 12:26:20.123456789");
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

    auto reader = stryke::OrcReader<stryke::TimestampNumber>(filename);
    auto result = reader.get_data();
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

