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

#include "stryke_multifile.hpp"
#include "stryke_template.hpp"

using namespace stryke;

void bench_multi(int number_line) {
    auto start1 = std::chrono::high_resolution_clock::now();
  {
    OrcWriterMulti<DateNumber, Int, Long, Double> writer_multi({"A2", "B2", "C2", "D2"}, ".", "date", 100000, 100);
    for (int i = 0; i < number_line; ++i) {
      writer_multi.write(300 * 30 + i / 10., 2000 + i + 1, 3000 + i + 2, 11111.111111 + i / 10000.);
    }
  }
  std::chrono::duration<double, std::milli> elapsed1 = std::chrono::high_resolution_clock::now() - start1;
  std::cout << std::setw(40) << std::left << "multi:  processing time : description : " << elapsed1.count() << " ms\n";

}

void bench_simple(int number_line) {
    auto start2 = std::chrono::high_resolution_clock::now();
  {
    OrcWriterImpl<DateNumber, Int, Long, Double> writer_simple({"A2", "B2", "C2", "D2"}, "date", 100);
    for (int i = 0; i < number_line; ++i) {
      writer_simple.write(300 * 30 + i / 10., 2000 + i + 1, 3000 + i + 2, 11111.111111 + i / 10000.);
    }
  }
  std::chrono::duration<double, std::milli> elapsed2 = std::chrono::high_resolution_clock::now() - start2;

  std::cout << std::setw(40) << std::left << "simple: processing time : description : " << elapsed2.count() << " ms\n";

}

int main(int argc, char const *argv[]) {

  int number_line = 10000;
  bench_multi(number_line);
  bench_simple(number_line);

  return 0;
}
