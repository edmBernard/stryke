//
//  Stryke
//
//  https://github.com/edmBernard/Stryke
//
//  Created by Erwan BERNARD on 02/12/2018.
//
//  Copyright (c) 2018 Erwan BERNARD. All rights reserved.
//  Distributed under the Apache License, Version 2.0. (See accompanying
//  file LICENSE or copy at http://www.apache.org/licenses/LICENSE-2.0)
//

#include "stryke/core.hpp"
#include "stryke/reader.hpp"
#include "date/date.h"
#include <iostream>
#include <sstream>
#include <string>
#include <fstream>


using namespace stryke;


int main(int argc, char const *argv[]) {
  std::string filename = "test.orc";
  std::string filename2 = "test2.orc";

  WriterOptions writer_options = WriterOptions();
  std::vector<double> input;
  for (int i = -30; i <= 10; ++i) {
    input.push_back(i/10.);
  }

  {
    stryke::OrcWriterImpl<stryke::DateNumber> writer({"col1"}, filename, writer_options);
    // writer.write(-2.999);
    // writer.write(-1.999);
    // writer.write(-0.999);
    // writer.write(0.001);
    writer.write(-2);
    writer.write(-1);
    writer.write(0);
    writer.write(1);

    stryke::OrcWriterImpl<stryke::Date> writer2({"col1"}, filename2, writer_options);
    // writer2.write("1969-12-31 23:59:57.001");
    // writer2.write("1969-12-31 23:59:58.001");
    // writer2.write("1969-12-31 23:59:59.001");
    // writer2.write("1970-01-01 00:00:00.001");
    writer2.write("1969-12-30");
    writer2.write("1969-12-31");
    writer2.write("1970-01-01");
    writer2.write("1970-01-02");
  }

  std::cout << "filename Number" << std::endl;
  auto result = stryke::orcReader<stryke::DateNumber>(filename);
  for (auto && i: result) {
    std::cout << "std::get<0>(result[count]).data :" << std::get<0>(i).data << std::endl;
  }

  std::cout << "filename String" << std::endl;
  result = stryke::orcReader<stryke::DateNumber>(filename2);
  for (auto && i: result) {
    std::cout << "std::get<0>(result[count]).data :" << std::get<0>(i).data << std::endl;
  }

  return 0;
}
