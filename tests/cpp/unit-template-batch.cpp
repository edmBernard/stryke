#include "catch.hpp"
#include "utils_for_test.hpp"
#include "stryke/core.hpp"
#include <filesystem>

namespace fs = std::filesystem;

TEST_CASE("OrcWriterImpl Batch", "[Simple]") {

  for (auto &&batchsize : {10, 100, 1000, 10000}) {
    for (auto &&nbr_rows : {10, 100, 1000, 100000}) {

      std::string filename = "test.orc";
      stryke::WriterOptions options;
      options.set_batch_size(batchsize);
      options.set_batch_max(0);
      options.set_stripe_size(10000);

      SECTION("Batch size: " + std::to_string(batchsize) + " nbr_rows: " + std::to_string(nbr_rows)) {
        {
          stryke::OrcWriterImpl<stryke::DateNumber, stryke::Int> writer({"date", "col1"}, filename, options);
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

