//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 08/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//

#include "stryke_thread.hpp"
#include "stryke_dispatch.hpp"
#include "stryke_multifile.hpp"
#include "stryke_template.hpp"
#include <filesystem>
#include <chrono>

using namespace stryke;
namespace fs = std::filesystem;

void bench_thread(int number_line) {
  fs::create_directories("data");
  auto start1 = std::chrono::high_resolution_clock::now();
  {
    OrcWriterThread<OrcWriterMulti, DateNumber, Double> writer_multi({"A2", "B2"}, "data", "date", 1000000, 100);
    for (int i = 0; i < number_line; ++i) {
      auto start3 = std::chrono::system_clock::now() + std::chrono::duration<double, std::milli>(2000);
      std::cout << "i :" << i << std::endl;
      writer_multi.write(300 * 30 + i / 100000., 11111.111111 + i / 10000.);
      std::this_thread::sleep_until(start3);
    }
    std::chrono::duration<double, std::milli> elapsed1 = std::chrono::high_resolution_clock::now() - start1;
    std::cout << std::setw(40) << std::left << "thread:  processing time : description : " << elapsed1.count() << " ms\n";
  }
  std::chrono::duration<double, std::milli> elapsed2 = std::chrono::high_resolution_clock::now() - start1;
  std::cout << std::setw(40) << std::left << "thread after full release:  processing time : description : " << elapsed2.count() << " ms\n";
  fs::remove_all("data");
}

void bench_multi(int number_line) {
  fs::create_directories("data");
  auto start1 = std::chrono::high_resolution_clock::now();
  {
    OrcWriterMulti<DateNumber, Double> writer_multi({"A2", "B2"}, "data", "date", 1000000, 100);
    for (int i = 0; i < number_line; ++i) {
      writer_multi.write(300 * 30 + i / 100000., 11111.111111 + i / 10000.);
    }
    std::chrono::duration<double, std::milli> elapsed1 = std::chrono::high_resolution_clock::now() - start1;
    std::cout << std::setw(40) << std::left << "multi:  processing time : description : " << elapsed1.count() << " ms\n";
  }
  std::chrono::duration<double, std::milli> elapsed2 = std::chrono::high_resolution_clock::now() - start1;
  std::cout << std::setw(40) << std::left << "multi after full release:  processing time : description : " << elapsed2.count() << " ms\n";
  fs::remove_all("data");
}

void bench_dispatch(int number_line) {
  fs::create_directories("data");
  auto start1 = std::chrono::high_resolution_clock::now();
  {
    OrcWriterDispatch<DateNumber, Double> writer_dispatch({"A2", "B2"}, "data", "date", 1000000, 100);
    for (int i = 0; i < number_line; ++i) {
      writer_dispatch.write(300 * 30 + i / 100000., 11111.111111 + i / 10000.);
    }
    std::chrono::duration<double, std::milli> elapsed1 = std::chrono::high_resolution_clock::now() - start1;
    std::cout << std::setw(40) << std::left << "dispatch:  processing time : description : " << elapsed1.count() << " ms\n";
  }
  std::chrono::duration<double, std::milli> elapsed2 = std::chrono::high_resolution_clock::now() - start1;
  std::cout << std::setw(40) << std::left << "dispatch after full release:  processing time : description : " << elapsed2.count() << " ms\n";
  fs::remove_all("data");
}

void bench_simple(int number_line) {
  fs::create_directories("data");
  auto start1 = std::chrono::high_resolution_clock::now();
  {
    OrcWriterImpl<DateNumber, Double> writer_simple({"A2", "B2"}, "data/date", 100);
    for (int i = 0; i < number_line; ++i) {
      writer_simple.write(300 * 30 + i / 100000., 11111.111111 + i / 10000.);
    }
    std::chrono::duration<double, std::milli> elapsed1 = std::chrono::high_resolution_clock::now() - start1;
    std::cout << std::setw(40) << std::left << "simple: processing time : description : " << elapsed1.count() << " ms\n";
  }
  std::chrono::duration<double, std::milli> elapsed2 = std::chrono::high_resolution_clock::now() - start1;
  std::cout << std::setw(40) << std::left << "simple after full release:  processing time : description : " << elapsed2.count() << " ms\n";
  fs::remove_all("data");
}

int main(int argc, char const *argv[]) {

  int number_line = 10000000;
  bench_multi(number_line);
  bench_thread(number_line);
  // bench_dispatch(number_line);
  bench_simple(number_line);

  return 0;
}
