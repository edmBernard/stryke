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
#include "stryke/sequential.hpp"


using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterSequential<Int, FolderEncode<DateNumber>, DateNumber, Int> writer({"direction", "date", "date", "value"}, "data", "date", WriterOptions());
  for (int i = 0; i < 20; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer.write(int(i/10), 17875 + i/100000., 17875 + i/100000., i);

  }

  return 0;
}
