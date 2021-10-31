//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018, 2019, 2020, 2021 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//

#include "stryke/type.hpp"
#include "stryke/csv/core.hpp"
#include "date/date.h"
#include <iostream>
#include <sstream>
#include <string>
#include <fstream>


using namespace stryke;
using namespace stryke::csv;

int main(int argc, char const *argv[]) {
  std::string filename = "my_file.csv";
  CsvWriterImpl<DateNumber, Int> writer({"date", "value"}, filename, WriterOptions());
  for (int i = 0; i < 10100; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer.write(17875 + i/100000., i);
  }

  std::string filename2 = "my_file2.csv";
  CsvWriterImpl<long, int> writer2({"date", "value"}, filename2, WriterOptions());
  for (int i = 0; i < 10100; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer2.write(17875 + i/100000., i);
  }

  std::string filename3 = "my_file3.csv";
  CsvWriterImpl<TimestampNumber, Int> writer3({"date", "value"}, filename3, WriterOptions());
  for (int i = 0; i < 10; ++i) {
      std::cout << "43 + i :" << i << std::endl;
      writer3.write(1556780436 + i, i);
  }

  return 0;
}
