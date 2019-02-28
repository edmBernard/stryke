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

#include "stryke/core.hpp"
#include "stryke/dispatch.hpp"


using namespace stryke;

int main(int argc, char const *argv[]) {
  WriterOptions options;
  options.enable_suffix_timestamp();

  OrcWriterDispatch<FolderEncode<DateNumber, Int>, Int, Int> writer({"date", "direction", "value", "direction"}, "data", "date", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer.write(17875 + i/100000., i%2, i, i%2);
      if (i == 10) {
        writer.close();
      }
  }

  OrcWriterDispatch<FolderEncode<DateNumber>, Int, Int> writer2({"date", "value", "direction"}, "data2", "date", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer2.write(17875 + i/100000., i, i%2);
      if (i == 10) {
        writer2.close();
      }
  }

  OrcWriterDispatch<DateNumber, Int, Int> writer3({"date", "value", "direction"}, "data3", "date", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer3.write(17875 + i/100000., i, i%2);
      if (i == 10) {
        writer3.close();
      }
  }

  OrcWriterDispatch<FolderEncode<Int, DateNumber>, DateNumber, Int> writer4({"direction", "date", "date", "value"}, "data4", "date", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer4.write(int(i/10), 17875 + i/100000., 17875 + i/100000., i);

  }

  OrcWriterDispatch<FolderEncode<>, DateNumber, Int, Int> writer5({"date", "value", "direction"}, "data5", "date", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer5.write(17875 + i/100000., i, i%2);
      if (i == 10) {
        writer5.close();
      }
  }

  return 0;
}
