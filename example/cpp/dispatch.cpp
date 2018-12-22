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

  OrcWriterDispatch<DateNumber, Int, Long, Double> writer({"A2", "B2", "C2","D2"}, "data", "date", WriterOptions());

  for (int i = 0; i < 100; ++i) {
    writer.write(300 * 30 + i/10., 2000 + i+1, 3000 + i+2, 11111.111111 + i/10000.);
  }

  return 0;
}
