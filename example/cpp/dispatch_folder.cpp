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

#include "stryke/type.hpp"
#include "stryke/options.hpp"
#include "stryke/dispatch_folder.hpp"


using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterDispatchFolder<FolderEncode<Int>, DateNumber, Int> writer({"direction", "date", "value"}, "data", "date_", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer.write(i%2, 17875 + i/100000., i);
      if (i == 10) {
        writer.close();
      }
  }

  OrcWriterDispatchFolder<FolderEncode<>, DateNumber, Int, Int> writer2({"direction", "date", "value"}, "data2", "date_", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer2.write(17875 + i/100000., i%2, i);
      if (i == 10) {
        writer2.close();
      }
  }

  return 0;
}
