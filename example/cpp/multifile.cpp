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
#include "stryke/multifile.hpp"


using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterMulti<DateNumber, Int> writer({"date", "value"}, "data", "date_", WriterOptions());
  for (int i = 0; i < 10100; ++i) {
      std::cout << "42 + i :" << i << std::endl;
      writer.write(17875 + i/100000., i);
  }

  return 0;
}
