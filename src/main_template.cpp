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

using namespace stryke;

int main(int argc, char const *argv[]) {

  OrcWriterImpl<Int, Long, Date, Timestamp> my_writer(10, 100, ".", "toto.orc");
  for (int i = 0; i < 3; ++i) {

    my_writer.write(1000 + i, 2000 + i+1, std::string("1990-12-18 12:26:20"), std::string("1980-12-18 12:26:20"));
    // my_writer.write(1000 + i, 2000 + i+1, 3000 + i+2, 4000 + i+3);
  }

  return 0;
}
