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

#include "stryke_template.hpp"
#include <iostream>
#include <string>
#include <fstream>

using namespace stryke;

int main(int argc, char const *argv[]) {
  std::string filename = "test.orc";

  {
    OrcWriterImpl<Date, Long> writer({"date", "col1"}, filename, WriterOptions());
    for (int i = 0; i < 10; ++i) {
      // std::this_thread::sleep_for(std::chrono::duration<double, std::milli>(1000));
      writer.write("", (i%2==0));
      writer.write("2018-12-12", (i%4==0));
    }
    // writer.write(21, true);
  }

  return 0;
}
