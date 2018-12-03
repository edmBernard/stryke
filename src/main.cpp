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

#include "stryke.hpp"
#include <iostream>
#include <string>

using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterImpl<int, int, long> my_writer(10, 100, "../data", "toto.orc");
  for (int i = 0; i < 150; ++i) {
    my_writer.write(1000 + i, 2000 + i+1, 3000 + i+2);
  }

  return 0;
}
