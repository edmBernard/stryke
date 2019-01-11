#include "catch.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
#include "stryke/options.hpp"
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterImpl Lock File", "[Simple]") {

  std::string filename = "test.orc";
  stryke::WriterOptions options;
  options.set_batch_size(10000);
  options.set_batch_max(0);
  options.set_stripe_size(10000);

  SECTION("Without lock file") {
    {
      options.disable_lock_file();

      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Boolean> writer({"date", "col1"}, filename, options);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, (i%2==0));
        REQUIRE_FALSE(fs::exists(filename + ".lock"));
      }
    }
    REQUIRE_FALSE(fs::exists(filename + ".lock"));
  }
  SECTION("With lock file") {
    {
      stryke::OrcWriterImpl<stryke::DateNumber, stryke::Boolean> writer({"date", "col1"}, filename, options);
      for (int i = 0; i < 10; ++i) {
        writer.write(i, (i%2==0));
        REQUIRE(fs::exists(filename + ".lock"));
      }
    }
    REQUIRE_FALSE(fs::exists(filename + ".lock"));
  }

  fs::remove(filename);

}
